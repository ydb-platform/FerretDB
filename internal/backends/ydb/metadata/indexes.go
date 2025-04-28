package metadata

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"math/rand/v2"
	"path"
	"regexp"
	"slices"
	"strings"
	"text/template"

	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
)

// Indexes represents information about all indexes in a collection.
type Indexes []IndexInfo

// IndexInfo represents information about a single index.
type IndexInfo struct {
	Name   string
	Ready  bool
	Key    []IndexKeyPair
	Unique bool
}

// IndexKeyPair consists of a field name and a sort order that are part of the index.
type IndexKeyPair struct {
	Field      string
	Descending bool
}

type IndexColumn struct {
	ColumnName  string
	BsonType    string
	ColumnType  string
	ColumnValue any
}

// deepCopy returns a deep copy.
func (indexes Indexes) deepCopy() Indexes {
	res := make(Indexes, len(indexes))

	for i, index := range indexes {
		res[i] = IndexInfo{
			Name:   index.Name,
			Ready:  index.Ready,
			Key:    slices.Clone(index.Key),
			Unique: index.Unique,
		}
	}

	return res
}

func addFieldColumns(ctx context.Context, c table.Client, prefix string, tableName string, columns []options.AlterTableOption) error {
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.AlterTable(ctx, path.Join(prefix, tableName), columns...)
		},
		table.WithIdempotent(),
	)

	if err != nil {
		return lazyerrors.Error(err)
	}

	return nil
}

func dropFieldColumns(ctx context.Context, c table.Client, prefix string, tableName string, columns []options.AlterTableOption) error {
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.AlterTable(ctx, path.Join(prefix, tableName), columns...)
		},
	)
	if err != nil {
		return lazyerrors.Error(err)
	}

	return nil
}

func GetColumnsInDefinedOrder() []string {
	return []string{"string", "objectId", "bool", "date", "long", "int", "double"}
}

func buildTypePath(path string) string {
	parts := strings.Split(path, ".")

	var sb strings.Builder
	sb.WriteString(`$`) // стартуем с корня

	for _, part := range parts {
		sb.WriteString(`.\"$s\".p.`)
		sb.WriteString(part)
	}

	sb.WriteString(`.t`)

	return sb.String()
}

func migrateIndexData(ctx context.Context, c table.Client, prefix, tableName string, fieldNames []IndexKeyPair) error {
	supportedTypes := SupportedIndexTypes()
	var columns []string
	var casts []string
	for _, pair := range fieldNames {
		fieldName := pair.Field
		typePath := buildTypePath(fieldName)
		jsonPath := DotNotationToJSONPath(fieldName)
		for bson, yType := range supportedTypes {
			if strings.Contains(fieldName, ".") {
				fieldName = strings.ReplaceAll(fieldName, ".", "")
			}
			colName := fmt.Sprintf("%s_%s", fieldName, bson)
			columns = append(columns, colName)
			if bson == "double" {
				casts = append(casts, fmt.Sprintf(`
					CASE 
						WHEN val_type = "%s" 
						THEN
							CASE 
								WHEN is_min THEN -9223372036854775808
								WHEN is_max THEN 9223372036854775807
								ELSE CAST(val as %s)
							END
						ELSE NULL
					END AS %s
			`, bson, yType.String(), colName))
			} else {
				casts = append(casts, fmt.Sprintf(`
					CASE 
						WHEN val_type = "%s" 
						THEN CAST(val AS %s) 
						ELSE NULL 
					END AS %s
			`, bson, yType.String(), colName))
			}
		}

		offset := 0
		batchSize := 10

		for {
			q, _ := Render(
				template.Must(template.New("").
					Funcs(template.FuncMap{
						"escapeName": escapeName,
					}).
					Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					SELECT COUNT(*) AS cnt
					FROM (
					  SELECT *
					  FROM {{ escapeName .TableName }}
					  LIMIT {{ .Limit }} OFFSET {{ .Offset }}
					);

					UPSERT INTO {{ escapeName .TableName }} (id{{ range .Columns }}, {{ . }}{{ end }})
					SELECT
					  id,
					  {{ .Casts }}
					FROM (
					  SELECT
						id,
						JSON_VALUE(_jsonb, "$.{{ .Path }}") AS val,
						JSON_VALUE(_jsonb, "{{ .TypePath }}") AS val_type,
						JSON_EXISTS(_jsonb, '$.{{ .Path }} ? (@ == $min)' PASSING -9223372036854775808 AS "min") AS is_min,
  						JSON_EXISTS(_jsonb, '$.{{ .Path }} ? (@ == $max)' PASSING 9223372036854775807 AS "max") AS is_max
					  FROM {{ escapeName .TableName }}
					  WHERE JSON_EXISTS(_jsonb, "$.{{ .Path }}")
					  ORDER BY id
					  LIMIT {{ .Limit }} OFFSET {{ .Offset }}
					);
				`)),
				map[string]any{
					"TablePathPrefix": prefix,
					"TableName":       tableName,
					"Columns":         columns,
					"Path":            jsonPath,
					"TypePath":        typePath,
					"Casts":           strings.Join(casts, ",\n  "),
					"Limit":           batchSize,
					"Offset":          offset,
				},
			)

			var rowCount uint64
			err := c.DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
				res, err := tx.Execute(ctx, q, table.NewQueryParameters())
				if err != nil {
					return err
				}
				defer res.Close()

				if res.NextResultSet(ctx) && res.NextRow() {
					if err = res.ScanNamed(named.OptionalWithDefault("cnt", &rowCount)); err != nil {
						return err
					}
				}

				return res.Err()
			})
			if err != nil {
				return err
			}

			if rowCount == 0 {
				break
			}

			offset += batchSize
		}
	}

	return nil
}

func addIndex(ctx context.Context, c table.Client, prefix string, tableName string, columnsGroup []string, indexName string) error {
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.AlterTable(ctx, path.Join(prefix, tableName),
				options.WithAddIndex(indexName,
					options.WithIndexColumns(columnsGroup...),
					options.WithIndexType(options.GlobalIndex()),
				))
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return lazyerrors.Error(err)
	}

	return nil
}

func dropIndex(ctx context.Context, c table.Client, prefix string, tableName string, name string) error {
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.AlterTable(ctx, path.Join(prefix, tableName),
				options.WithDropIndex(name))
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return lazyerrors.Error(err)
	}

	return nil
}

func DotNotationToJSONPath(dotPath string) string {
	parts := strings.Split(dotPath, ".")
	re := regexp.MustCompile(`^\d+$`)

	var sb strings.Builder

	for i, part := range parts {
		if re.MatchString(part) {
			sb.WriteString("[" + part + "]")
		} else {
			if i != 0 {
				sb.WriteString(".")
			}
			sb.WriteString(part)
		}
	}

	return sb.String()
}

func CleanRootKey(rootKey string) string {
	re := regexp.MustCompile(`[.\[\]-]`)
	cleanedKey := re.ReplaceAllString(rootKey, "")
	randomNumber := rand.IntN(1000000)

	return fmt.Sprintf("f_%s_%d", cleanedKey, randomNumber)
}

func CleanColumnName(rootKey string) string {
	re := regexp.MustCompile(`[.\[\]]`)
	cleanedKey := re.ReplaceAllString(rootKey, "")

	return cleanedKey
}
