package ydb

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/must"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"strconv"
	"text/template"
)

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		panic(err)
	}

	return buf.String()
}

type selectParams struct {
	Schema  string
	Table   string
	Comment string

	Capped        bool
	OnlyRecordIDs bool
}

func data(id string, _jsonb []byte) ydbTypes.Value {
	return ydbTypes.StructValue(
		ydbTypes.StructFieldValue("id", ydbTypes.BytesValue([]byte(id))),
		ydbTypes.StructFieldValue("_jsonb", ydbTypes.JSONDocumentValueFromBytes(_jsonb)),
	)
}

type ObjectId struct {
	ID string `json:"_id"`
}

type templateConfig struct {
	TablePathPrefix string
	Table           string
}

const (
	upsertTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
		
					DECLARE $insertData AS List<Struct<
					id: String,
					_jsonb: JsonDocument>>;

					UPSERT INTO {{ .Table }} 
					SELECT
						id,
						_jsonb,
					FROM AS_TABLE($insertData);

			`
	replaceTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
		
					DECLARE $updateData AS List<Struct<
					id: String,
					_jsonb: JsonDocument>>;

					REPLACE INTO {{ .Table }} 
					SELECT
						id,
						_jsonb,
					FROM AS_TABLE($updateData);

			`
	deleteTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					DELETE FROM {{ .Table }}
					WHERE id IN $IDs;
			`
)

func getId(doc *types.Document) string {
	var ydbId string
	value, _ := doc.Get("_id")
	must.NotBeZero(value)

	switch v := value.(type) {
	case types.ObjectID:
		ydbId = hex.EncodeToString(v[:])
	case string:
		ydbId = v
	case int:
		ydbId = strconv.Itoa(v)
	case int32:
		ydbId = strconv.Itoa(int(v))
	case int64:
		ydbId = strconv.FormatInt(v, 10)
	default:
		panic(fmt.Sprintf("unsupported _id type: %T", value))
	}
	return ydbId
}

func prepareSelectClause(params *selectParams) string {
	return fmt.Sprintf(
		`
				DECLARE $id AS Optional<Uint64>;
				DECLARE $myStr AS Optional<Text>;
				SELECT * FROM %s;`, params.Table)

}
