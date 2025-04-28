package ydb

import (
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/handler/sjson"
	"github.com/FerretDB/FerretDB/internal/types"
)

func extractIndexFields(doc *types.Document, indexes metadata.Indexes) map[string]metadata.IndexColumn {
	extraColumns := map[string]metadata.IndexColumn{}
	for _, index := range indexes {
		if index.Name == fmt.Sprintf("_%s_", metadata.DefaultIDColumn) {
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

			bsonType := sjson.GetTypeOfValue(val)
			ydbType := metadata.MapBSONTypeToYDBType(bsonType)

			if ydbType == nil {
				continue
			}

			columnName := fmt.Sprintf("%s_%s", metadata.CleanColumnName(pair.Field), bsonType)
			extraColumns[columnName] = metadata.IndexColumn{
				ColumnName:  columnName,
				BsonType:    bsonType,
				ColumnType:  ydbType.String(),
				ColumnValue: val,
			}
		}
	}

	return extraColumns
}
