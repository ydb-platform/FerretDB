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
)

func SingleDocumentData(doc *types.Document, extra map[string]metadata.IndexColumn, capped bool) ydbTypes.Value {
	b, err := sjson.Marshal(doc)
	if err != nil {
		return nil
	}

	idValue := GetId(doc)
	idType := sjson.GetTypeOfValue(idValue)
	jsonBytes, err := sjson.MarshalSingleValue(idValue)
	if err != nil {
		return nil
	}

	combo := fmt.Sprintf("%s_%s", string(jsonBytes), idType)
	h := fnv.New64a()
	h.Write([]byte(combo))
	idHash := fmt.Sprintf("%x", h.Sum64())

	fields := []ydbTypes.StructValueOption{
		ydbTypes.StructFieldValue("id_hash", ydbTypes.BytesValueFromString(idHash)),
		ydbTypes.StructFieldValue(metadata.DefaultIDColumn, ydbTypes.BytesValue(jsonBytes)),
		ydbTypes.StructFieldValue(metadata.DefaultColumn, ydbTypes.JSONValueFromBytes(b)),
	}

	for name, info := range extra {
		fields = append(fields, ydbTypes.StructFieldValue(
			name,
			metadata.MapBSONValueToYDBValue(info.BsonType, info.ColumnValue)),
		)
	}

	if capped {
		fields = append(fields, ydbTypes.StructFieldValue(metadata.RecordIDColumn, ydbTypes.Int64Value(doc.RecordID())))
	}

	return ydbTypes.StructValue(fields...)
}

func buildInsertQuery(pathPrefix, tableName string, capped bool, extra map[string]metadata.IndexColumn) string {
	var fieldDecls = []string{
		fmt.Sprintf("id_hash: %s", ydbTypes.TypeString.String()),
		fmt.Sprintf("id: %s", ydbTypes.TypeString.String()),
		fmt.Sprintf("%s: %s", metadata.DefaultColumn, ydbTypes.TypeJSON.String()),
	}
	var selectFields = []string{"id_hash", "id", metadata.DefaultColumn}

	for name, info := range extra {
		fieldDecls = append(fieldDecls, fmt.Sprintf("%s: %s", name, info.ColumnType))
		selectFields = append(selectFields, name)
	}

	if capped {
		fieldDecls = append(fieldDecls, fmt.Sprintf("%s: %s", metadata.RecordIDColumn, ydbTypes.TypeInt64.String()))
		selectFields = append(selectFields, metadata.RecordIDColumn)
	}

	config := metadata.InsertTemplateConfig{
		TablePathPrefix: pathPrefix,
		TableName:       tableName,
		FieldDecls:      strings.Join(fieldDecls, ", "),
		SelectFields:    strings.Join(selectFields, ", "),
	}

	q, _ := metadata.Render(metadata.InsertTmpl, config)

	return q
}

func buildUpsertQuery(pathPrefix, tableName string, extra map[string]metadata.IndexColumn) string {
	var fieldDecls = []string{
		fmt.Sprintf("id_hash: %s", ydbTypes.TypeString.String()),
		fmt.Sprintf("id: %s", ydbTypes.TypeString.String()),
		fmt.Sprintf("%s: %s", metadata.DefaultColumn, ydbTypes.TypeJSON.String()),
	}
	var selectFields = []string{"id_hash", "id", metadata.DefaultColumn}

	for name, info := range extra {
		fieldDecls = append(fieldDecls, fmt.Sprintf("%s: %s", name, info.ColumnType))
		selectFields = append(selectFields, name)
	}

	config := metadata.UpsertTemplateConfig{
		TablePathPrefix: pathPrefix,
		TableName:       tableName,
		FieldDecls:      strings.Join(fieldDecls, ", "),
		SelectFields:    strings.Join(selectFields, ", "),
	}

	q, _ := metadata.Render(metadata.UpsertTmpl, config)

	return q
}

func GetId(doc *types.Document) any {
	value, _ := doc.Get("_id")
	must.NotBeZero(value)

	return value
}

func prepareIDs(params *backends.DeleteAllParams) []ydbTypes.Value {
	var ids []ydbTypes.Value
	if params.RecordIDs == nil {
		ids = make([]ydbTypes.Value, 0, len(params.IDs))
		for _, id := range params.IDs {
			jsonBytes, err := sjson.MarshalSingleValue(id)
			if err != nil {
				return nil
			}

			ids = append(ids, ydbTypes.BytesValue(jsonBytes))
		}
	} else {
		ids = make([]ydbTypes.Value, 0, len(params.RecordIDs))
		for _, id := range params.RecordIDs {
			ids = append(ids, ydbTypes.Int64Value(id))
		}
	}

	return ids
}
