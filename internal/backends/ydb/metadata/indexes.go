package metadata

import (
	"context"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/handler/sjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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
	sb.WriteString(`$`)

	for _, part := range parts {
		sb.WriteString(`.\"$s\".p.`)
		sb.WriteString(part)
	}

	sb.WriteString(`.t`)

	return sb.String()
}

func migrateIndexData(ctx context.Context, c table.Client, prefix, tableName string, fieldNames []IndexKeyPair) error {
	supportedTypes := GetSupportedIndexTypes()
	var columns []string
	var casts []string
	for _, pair := range fieldNames {
		fieldName := pair.Field
		typePath := buildTypePath(fieldName)
		jsonPath := DotNotationToJSONPath(fieldName)
		fieldName = strings.ReplaceAll(fieldName, ".", "")

		for bson, yType := range supportedTypes {
			colName := fmt.Sprintf("%s_%s", fieldName, bson)
			columns = append(columns, colName)

			if bson == "double" {
				casts = append(casts, fmt.Sprintf(`
					CASE 
						WHEN JSON_VALUE(_jsonb, "%s") = "%s" 
						THEN
							CASE 
								WHEN JSON_EXISTS(_jsonb, '$.%s ? (@ == $min)' PASSING -9223372036854775808 AS "min") THEN -9223372036854775808
								WHEN JSON_EXISTS(_jsonb, '$.%s ? (@ == $max)' PASSING 9223372036854775807 AS "max") THEN 9223372036854775807
								ELSE CAST(JSON_VALUE(_jsonb, "$.%s") AS %s)
							END
						ELSE NULL
					END AS %s
			`, typePath, bson, jsonPath, jsonPath, jsonPath, yType.String(), colName))
			} else {
				casts = append(casts, fmt.Sprintf(`
					CASE 
						WHEN JSON_VALUE(_jsonb, "%s") = "%s" 
						THEN CAST(JSON_VALUE(_jsonb, "$.%s") AS %s) 
						ELSE NULL 
					END AS %s
			`, typePath, bson, jsonPath, yType.String(), colName))
			}
		}
	}

	limit := 100
	lastId := ""
	empty := false

	for !empty {
		q, _ := Render(
			template.Must(template.New("").
				Funcs(template.FuncMap{
					"escapeName": escapeName,
				}).
				Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					DECLARE $limit AS Uint64;
					DECLARE $lastId AS String;

					UPSERT INTO {{ escapeName .TableName }} (id_hash, id, _jsonb{{ range .Columns }}, {{ . }}{{ end }})
					SELECT
					    id_hash,
						id,
						_jsonb,
					  {{ .Casts }}
					FROM (
					  SELECT
						id_hash,
						id,
						_jsonb
					  FROM {{ escapeName .TableName }}
					  WHERE id_hash > $lastId

					  ORDER BY id_hash
					  LIMIT $limit
					);

					SELECT
						id_hash
					  FROM {{ escapeName .TableName }}
					  WHERE id_hash > $lastId

					  ORDER BY id_hash
					  LIMIT $limit;
				`)),
			map[string]any{
				"TablePathPrefix": prefix,
				"TableName":       tableName,
				"Columns":         columns,
				"Casts":           strings.Join(casts, ",\n"),
			},
		)

		err := c.DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, q, table.NewQueryParameters(
				table.ValueParam("$limit", ydbTypes.Uint64Value(uint64(limit))),
				table.ValueParam("$lastId", ydbTypes.BytesValueFromString(lastId)),
			))
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()

			if !res.NextResultSet(ctx) || !res.HasNextRow() {
				empty = true

				return res.Err()
			}

			for res.NextRow() {
				err = res.ScanNamed(
					named.Required("id_hash", &lastId),
				)
				if err != nil {
					return err
				}
			}

			return res.Err()
		})

		if err != nil {
			return err
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

func ExtractIndexFields(doc *types.Document, indexes Indexes) map[string]IndexColumn {
	extraColumns := map[string]IndexColumn{}
	for _, index := range indexes {
		for _, pair := range index.Key {
			path, err := types.NewPathFromString(pair.Field)
			if err != nil {
				continue
			}

			has := doc.HasByPath(path)
			if !has {
				continue
			}

			val, err := doc.GetByPath(path)
			if err != nil {
				continue
			}

			bsonType := sjson.GetTypeOfValue(val)
			ydbType := MapBSONTypeToYDBType(bsonType)

			if ydbType == nil {
				continue
			}

			columnName := fmt.Sprintf("%s_%s", CleanColumnName(pair.Field), bsonType)
			extraColumns[columnName] = IndexColumn{
				ColumnName:  columnName,
				BsonType:    bsonType,
				ColumnType:  ydbType.String(),
				ColumnValue: val,
			}
		}
	}

	return extraColumns
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
