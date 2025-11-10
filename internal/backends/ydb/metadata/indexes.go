package metadata

import (
	"context"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/handler/sjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"path"
	"regexp"
	"slices"
	"strings"
	"text/template"
)

// Indexes represents information about all indexes in a collection.
type Indexes []IndexInfo

// IndexInfo represents information about a single index.
type IndexInfo struct {
	Name          string
	SanitizedName string
	Ready         bool
	Key           []IndexKeyPair
	Unique        bool
}

// IndexKeyPair consists of a field name and a sort order that are part of the index.
type IndexKeyPair struct {
	Field      string
	Descending bool
}

type IndexColumn struct {
	ColumnName  string
	BsonType    BsonType
	ColumnType  string
	ColumnValue any
}

type SecondaryIndexDef struct {
	Name    string
	Unique  bool
	Columns []string
}

// deepCopy returns a deep copy.
func (indexes Indexes) deepCopy() Indexes {
	res := make(Indexes, len(indexes))

	for i, index := range indexes {
		res[i] = IndexInfo{
			Name:          index.Name,
			SanitizedName: index.SanitizedName,
			Ready:         index.Ready,
			Key:           slices.Clone(index.Key),
			Unique:        index.Unique,
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

func migrateIndexData(ctx context.Context, d *ydb.Driver, prefix, tableName string, fieldNames []IndexKeyPair) error {
	var columns []string
	var casts []string
	for _, pair := range fieldNames {
		fieldName := pair.Field
		typePath := buildTypePath(fieldName)
		jsonPath := DotNotationToJsonPath(fieldName)
		fieldName = columnNameCharacters.ReplaceAllString(fieldName, "_")

		for _, colType := range ColumnOrder {
			colName := fmt.Sprintf("%s_%s", fieldName, colType)
			columns = append(columns, colName)

			if colType == "double" {
				casts = append(casts, fmt.Sprintf(`
					CASE 
						WHEN JSON_VALUE(_jsonb, "%s") = "%s" 
						THEN 
							(	
							FromBytes(ToBytes(CAST(JSON_VALUE(_jsonb, "$.%s") AS Double)), uint64) 
       				 		^
							(
								FromBytes(ToBytes(
									CASE 
										WHEN FromBytes(ToBytes(CAST(JSON_VALUE(_jsonb, "$.%s") AS Double)), int64) < 0 
										THEN CAST(-1 AS int64) 
										ELSE CAST(0 AS int64) 
									END
								), uint64) 
								| 0x8000000000000000ul
							)
							)
							
						ELSE NULL
					END AS %s
			`, typePath, colType, jsonPath, jsonPath, colName))
			} else {
				casts = append(casts, fmt.Sprintf(`
					CASE 
						WHEN JSON_VALUE(_jsonb, "%s") = "%s" 
						THEN CAST(JSON_VALUE(_jsonb, "$.%s") AS %s) 
						ELSE NULL 
					END AS %s
			`, typePath, colType, jsonPath, ColumnStoreToYdbType(colType).String(), colName))
			}
		}
	}

	limit := 100
	var lastId uint64
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
					DECLARE $lastId AS Uint64;

					UPSERT INTO {{ escapeName .TableName }} (id_hash, _jsonb{{ range .Columns }}, {{ . }}{{ end }})
					SELECT
					    id_hash,
						_jsonb,
					  {{ .Casts }}
					FROM (
					  SELECT
						id_hash,
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

		err := d.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
			r, err := s.Query(ctx, q,
				query.WithTxControl(query.SerializableReadWriteTxControl(query.CommitTx())),
				query.WithParameters(
					table.NewQueryParameters(
						table.ValueParam("$limit", ydbTypes.Uint64Value(uint64(limit))),
						table.ValueParam("$lastId", ydbTypes.Uint64Value(lastId)),
					),
				))
			if err != nil {
				return err
			}
			defer func() {
				_ = r.Close(ctx)
			}()

			res := sugar.Result(r)

			found := false
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					found = true
					err = res.ScanNamed(named.Required(IdHashColumn, &lastId))
					if err != nil {
						return err
					}

				}
			}

			if !found {
				empty = true
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
		if index.Name == backends.DefaultIndexName {
			continue
		}
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

			bsonType := BsonType(sjson.GetTypeOfValue(val))
			ydbType := BsonTypeToYdbType(bsonType)

			if ydbType == nil {
				continue
			}

			fieldName := columnNameCharacters.ReplaceAllString(pair.Field, "_")
			columnName := fmt.Sprintf("%s_%s", fieldName, bsonType)
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

func DotNotationToJsonPath(dotPath string) string {
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
