package ydb

import (
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/handler/sjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/must"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"hash/fnv"
	"strings"
	"text/template"
)

func singleDocumentData(doc *types.Document, extra map[string]metadata.IndexColumn, capped bool) ydbTypes.Value {
	b, err := sjson.Marshal(doc)
	if err != nil {
		return nil
	}

	idValue := getId(doc)
	bsonType := metadata.BsonType(sjson.GetTypeOfValue(idValue))
	bid, err := sjson.MarshalSingleValue(idValue)
	if err != nil {
		return nil
	}

	idHash := generateIdHash(bid, bsonType)

	fields := []ydbTypes.StructValueOption{
		ydbTypes.StructFieldValue(metadata.IdHashColumn, ydbTypes.Uint64Value(idHash)),
		ydbTypes.StructFieldValue(metadata.DefaultColumn, ydbTypes.JSONValueFromBytes(b)),
	}

	for _, colType := range metadata.ColumnOrder {
		columnName := fmt.Sprintf("%s_%s", metadata.IdMongoField, colType)

		if metadata.BsonTypeToColumnStore(bsonType) == colType {
			fields = append(fields, ydbTypes.StructFieldValue(columnName, ydbTypes.OptionalValue(metadata.BsonValueToYdbValue(bsonType, idValue))))
		} else {
			fields = append(fields, ydbTypes.StructFieldValue(columnName, ydbTypes.NullValue(metadata.ColumnStoreToYdbType(colType))))
		}
	}

	for name, info := range extra {
		fields = append(fields, ydbTypes.StructFieldValue(
			name,
			metadata.BsonValueToYdbValue(info.BsonType, info.ColumnValue)),
		)
	}

	if capped {
		fields = append(fields, ydbTypes.StructFieldValue(metadata.RecordIDColumn, ydbTypes.Int64Value(doc.RecordID())))
	}

	return ydbTypes.StructValue(fields...)
}

func buildWriteQuery(pathPrefix, tableName string, extra map[string]metadata.IndexColumn, capped bool, tmplName *template.Template) string {
	pkColumns := metadata.BuildPrimaryKeyColumns()

	fieldDecls := make([]string, 0, len(pkColumns))
	selectFields := make([]string, 0, len(pkColumns))

	for _, col := range pkColumns {
		fieldDecls = append(fieldDecls, fmt.Sprintf("%s: %s", col.Name, col.Type.String()))
		selectFields = append(selectFields, col.Name)
	}

	fieldDecls = append(fieldDecls, fmt.Sprintf("%s: %s", metadata.DefaultColumn, ydbTypes.TypeJSON.String()))
	selectFields = append(selectFields, metadata.DefaultColumn)

	for name, info := range extra {
		fieldDecls = append(fieldDecls, fmt.Sprintf("%s: %s", name, info.ColumnType))
		selectFields = append(selectFields, name)
	}

	if capped {
		fieldDecls = append(fieldDecls, fmt.Sprintf("%s: %s", metadata.RecordIDColumn, ydbTypes.TypeInt64.String()))
		selectFields = append(selectFields, metadata.RecordIDColumn)
	}

	config := metadata.UpsertTemplateConfig{
		TablePathPrefix: pathPrefix,
		TableName:       tableName,
		FieldDecls:      strings.Join(fieldDecls, ", "),
		SelectFields:    strings.Join(selectFields, ", "),
	}

	q, _ := metadata.Render(tmplName, config)

	return q
}

func buildInsertQuery(pathPrefix, tableName string, capped bool, extra map[string]metadata.IndexColumn) string {
	return buildWriteQuery(pathPrefix, tableName, extra, capped, metadata.InsertTmpl)
}

func buildUpsertQuery(pathPrefix, tableName string, extra map[string]metadata.IndexColumn) string {
	return buildWriteQuery(pathPrefix, tableName, extra, false, metadata.UpsertTmpl)
}

func getId(doc *types.Document) any {
	value, _ := doc.Get(metadata.IdMongoField)
	must.NotBeZero(value)

	return value
}

func generateIdHash(jsonData []byte, idType metadata.BsonType) uint64 {
	h := fnv.New64a()
	h.Write(jsonData)
	h.Write([]byte{0})
	h.Write([]byte(idType))

	return h.Sum64()
}

func prepareIds(params *backends.DeleteAllParams) []ydbTypes.Value {
	var ids []ydbTypes.Value
	if params.RecordIDs == nil {
		ids = make([]ydbTypes.Value, 0, len(params.IDs))
		for _, id := range params.IDs {
			bid, err := sjson.MarshalSingleValue(id)
			if err != nil {
				return nil
			}

			idType := sjson.GetTypeOfValue(id)
			idHash := generateIdHash(bid, metadata.BsonType(idType))

			ids = append(ids, ydbTypes.Uint64Value(idHash))
		}
	} else {
		ids = make([]ydbTypes.Value, 0, len(params.RecordIDs))
		for _, id := range params.RecordIDs {
			ids = append(ids, ydbTypes.Int64Value(id))
		}
	}

	return ids
}
