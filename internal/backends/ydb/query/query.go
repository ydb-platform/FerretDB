package query

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"golang.org/x/exp/maps"
	"strconv"
	"text/template"
)

func Render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		panic(err)
	}

	return buf.String()
}

type SelectParams struct {
	Schema  string
	Table   string
	Comment string

	Capped        bool
	OnlyRecordIDs bool
}

func Data(id string, _jsonb []byte) ydbTypes.Value {
	return ydbTypes.StructValue(
		ydbTypes.StructFieldValue("id", ydbTypes.BytesValue([]byte(id))),
		ydbTypes.StructFieldValue("_jsonb", ydbTypes.JSONDocumentValueFromBytes(_jsonb)),
	)
}

type ObjectId struct {
	ID string `json:"_id"`
}

type TemplateConfig struct {
	TablePathPrefix string
	TableName       string
	ColumnName      string
	ColumnType      string
}

const (
	UpsertTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
		
					DECLARE $insertData AS List<Struct<
					id: String,
					_jsonb: JsonDocument>>;

					UPSERT INTO {{ .TableName }} 
					SELECT
						id,
						_jsonb,
					FROM AS_TABLE($insertData);

			`
	ReplaceTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
		
					DECLARE $updateData AS List<Struct<
					id: String,
					_jsonb: JsonDocument>>;

					REPLACE INTO {{ .TableName }} 
					SELECT
						id,
						_jsonb,
					FROM AS_TABLE($updateData);

			`
	DeleteTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					DELETE FROM {{ .TableName }}
					WHERE id IN $IDs;
			`
)

func GetId(doc *types.Document) string {
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

func PrepareSelectClause(params *SelectParams) string {
	return fmt.Sprintf(
		`
				DECLARE $id AS Optional<Uint64>;
				DECLARE $myStr AS Optional<Text>;
				SELECT * FROM %s;`, params.Table)

}

func UnmarshalExplainFromString(explainStr string) (*types.Document, error) {
	explainBytes := []byte(explainStr)

	return unmarshalExplain(explainBytes)
}

func unmarshalExplain(b []byte) (*types.Document, error) {
	var plans []map[string]any
	if err := json.Unmarshal(b, &plans); err != nil {
		return nil, lazyerrors.Error(err)
	}

	if len(plans) == 0 {
		return nil, lazyerrors.Error(errors.New("no execution plan returned"))
	}

	return convertJSON(plans[0]).(*types.Document), nil
}

// convertJSON transforms decoded JSON map[string]any value into *types.Document.
func convertJSON(value any) any {
	switch value := value.(type) {
	case map[string]any:
		d := types.MakeDocument(len(value))
		keys := maps.Keys(value)

		for _, k := range keys {
			v := value[k]
			d.Set(k, convertJSON(v))
		}

		return d

	case []any:
		a := types.MakeArray(len(value))
		for _, v := range value {
			a.Append(convertJSON(v))
		}

		return a

	case nil:
		return types.Null

	case float64, string, bool:
		return value

	default:
		panic(fmt.Sprintf("unsupported type: %[1]T (%[1]v)", value))
	}
}
