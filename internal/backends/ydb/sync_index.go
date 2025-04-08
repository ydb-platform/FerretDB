package ydb

import (
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/types"
	"strings"
)

func extractIndexFields(doc *types.Document, indexes metadata.Indexes) []metadata.IndexColumn {
	docFields := doc.Keys()
	fieldSet := make(map[string]struct{}, len(docFields))
	for _, f := range docFields {
		fieldSet[f] = struct{}{}
	}

	var extraFields []metadata.IndexColumn
	for _, index := range indexes {
		for _, pair := range index.Key {
			val, _ := doc.Get(pair.Field)
			_, exists := fieldSet[pair.Field]
			if !exists {
				val = nil
			}
			extraFields = append(extraFields, metadata.IndexColumn{
				ColumnName:  pair.Field,
				ColumnType:  pair.YdbType,
				ColumnValue: val,
			})
		}
	}
	return extraFields
}

func buildUpsertQuery(pathPrefix, tableName string, indexes metadata.Indexes) string {
	var fieldDecls = []string{"id: String", "_jsonb: JsonDocument"}
	var selectFields = []string{"id", "_jsonb"}

	for _, index := range indexes {
		for _, pair := range index.Key {
			fieldDecls = append(fieldDecls, fmt.Sprintf("%s: %s", pair.Field, pair.YdbType))
			selectFields = append(selectFields, pair.Field)
		}
	}

	return fmt.Sprintf(`
		PRAGMA TablePathPrefix("%s");

		DECLARE $insertData AS List<Struct<
		%s>>;

		UPSERT INTO %s
		SELECT
		%s
		FROM AS_TABLE($insertData);
	`, pathPrefix,
		strings.Join(fieldDecls, ",\n"),
		tableName,
		strings.Join(selectFields, ",\n"),
	)
}
