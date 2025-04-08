package ydb

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/iterator"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"golang.org/x/exp/maps"
	"strconv"
	"strings"
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

func Data(id string, _jsonb []byte, extra []metadata.IndexColumn) ydbTypes.Value {
	fields := []ydbTypes.StructValueOption{
		ydbTypes.StructFieldValue("id", ydbTypes.BytesValue([]byte(id))),
		ydbTypes.StructFieldValue("_jsonb", ydbTypes.JSONDocumentValueFromBytes(_jsonb)),
	}

	for _, e := range extra {
		fields = append(fields, ydbTypes.StructFieldValue(e.ColumnName, metadata.ConvertToYDBValueByStringRepresentation(e.ColumnType, e.ColumnValue)))
	}

	return ydbTypes.StructValue(fields...)
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
					DECLARE $IDs AS List<String>;

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

func getIdFromAny(value any) string {
	var ydbId string

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
	if params == nil {
		params = new(SelectParams)
	}

	if params.Comment != "" {
		params.Comment = strings.ReplaceAll(params.Comment, "/*", "/ *")
		params.Comment = strings.ReplaceAll(params.Comment, "*/", "* /")
		params.Comment = `/* ` + params.Comment + ` */`
	}

	if params.Capped && params.OnlyRecordIDs {
		return fmt.Sprintf(
			`SELECT %s %s FROM %s`,
			params.Comment,
			metadata.RecordIDColumn,
			params.Table,
		)
	}

	if params.Capped {
		return fmt.Sprintf(
			`SELECT %s %s, %s FROM %s`,

			params.Comment,
			metadata.RecordIDColumn,
			metadata.DefaultColumn,
			params.Table,
		)
	}

	return fmt.Sprintf(
		`SELECT %s %s FROM %s`,

		params.Comment,
		metadata.DefaultColumn,
		params.Table,
	)

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

func prepareWhereClause(sqlFilters *types.Document) (string, *table.QueryParameters, error) {
	var conditions []string
	var params []table.ParameterOption

	iter := sqlFilters.Iterator()
	defer iter.Close()

	// iterate through root document
	for {
		rootKey, rootVal, err := iter.Next()
		if err != nil {
			if errors.Is(err, iterator.ErrIteratorDone) {
				break
			}

			return "", nil, lazyerrors.Error(err)
		}

		if strings.HasPrefix(rootKey, "$") {
			continue
		}

		path := fmt.Sprintf(`"$.%s"`, rootKey)
		paramName := fmt.Sprintf("%s_%s", "f", rootKey)

		var yqlExpr string
		var param table.ParameterOption

		switch val := rootVal.(type) {
		case int32:
			yqlExpr = fmt.Sprintf(`CAST(JSON_VALUE(_jsonb, %s) AS Optional<Int32>) = $%s`, path, paramName)
			param = table.ValueParam(paramName, ydbTypes.OptionalValue(ydbTypes.Int32Value(val)))

		case int64:
			yqlExpr = fmt.Sprintf(`CAST(JSON_VALUE(_jsonb, %s) AS Optional<Int64>) = $%s`, path, paramName)
			param = table.ValueParam(paramName, ydbTypes.OptionalValue(ydbTypes.Int64Value(val)))

		case float64:
			yqlExpr = fmt.Sprintf(`CAST(JSON_VALUE(_jsonb, %s) AS Optional<Double>) = $%s`, path, paramName)
			param = table.ValueParam(paramName, ydbTypes.OptionalValue(ydbTypes.DoubleValue(val)))

		case string:
			yqlExpr = fmt.Sprintf(`JSON_VALUE(_jsonb, %s) = $%s`, path, paramName)
			param = table.ValueParam(paramName, ydbTypes.BytesValueFromString(val))

		case bool:
			yqlExpr = fmt.Sprintf(`CAST(JSON_VALUE(_jsonb, %s) AS Optional<Bool>) = $%s`, path, paramName)
			param = table.ValueParam(paramName, ydbTypes.OptionalValue(ydbTypes.BoolValue(val)))

		case *types.Document:
			iter := val.Iterator()
			defer iter.Close()

			// iterate through subdocument, as it may contain operators
			for {
				k, v, err := iter.Next()
				if err != nil {
					if errors.Is(err, iterator.ErrIteratorDone) {
						break
					}

					return "", nil, lazyerrors.Error(err)
				}

				switch k {
				case "$eq":
					yqlExpr = fmt.Sprintf(`CAST(JSON_VALUE(_jsonb, %s) AS Optional<Int32>) %s $%s`, path, "=", paramName)
					param = table.ValueParam(paramName, ydbTypes.OptionalValue(ydbTypes.Int32Value(v.(int32))))
				}
			}

		default:
			return "", nil, lazyerrors.Errorf("unsupported filter type for field '%s': %T", paramName, val)
		}

		conditions = append(conditions, yqlExpr)
		params = append(params, param)

	}

	query := " WHERE " + strings.Join(conditions, " AND ")
	return query, table.NewQueryParameters(params...), nil

}

func prepareOrderByClause(sort *types.Document) string {
	if sort.Len() != 1 {
		return ""
	}

	v := must.NotFail(sort.Get("$natural"))
	var order string

	switch v.(int64) {
	case 1:
	case -1:
		order = " DESC"
	default:
		panic("not reachable")
	}

	return fmt.Sprintf(" ORDER BY %s%s", metadata.RecordIDColumn, order)
}
