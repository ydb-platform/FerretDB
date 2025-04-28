package ydb

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/handler/sjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/iterator"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"golang.org/x/exp/maps"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	idField      = "_id"
	findOperator = "=="
	jsonPathRoot = "$"
)

func SingleDocumentData(doc *types.Document, extra map[string]metadata.IndexColumn, capped bool) ydbTypes.Value {
	b, err := sjson.Marshal(doc)
	if err != nil {
		return nil
	}

	id := GetId(doc)
	fields := []ydbTypes.StructValueOption{
		ydbTypes.StructFieldValue(metadata.DefaultIDColumn, ydbTypes.BytesValueFromString(id)),
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

func GetId(doc *types.Document) string {
	value, _ := doc.Get("_id")
	must.NotBeZero(value)

	return getIdFromAny(value)
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
	case float64:
		ydbId = strconv.FormatFloat(v, 'f', -1, 64)
	case *types.Document:
		jsonBytes, _ := sjson.MarshalSingleValue(v)
		ydbId = string(jsonBytes)
	default:
		panic(fmt.Sprintf("unsupported _id type: %T", value))
	}
	return ydbId
}

func PrepareSelectClause(params *metadata.SelectParams) string {
	if params == nil {
		params = new(metadata.SelectParams)
	}

	if params.Comment != "" {
		params.Comment = strings.ReplaceAll(params.Comment, "/*", "/ *")
		params.Comment = strings.ReplaceAll(params.Comment, "*/", "* /")
		params.Comment = `/* ` + params.Comment + ` */`
	}

	if params.Capped && params.OnlyRecordIDs {
		return fmt.Sprintf(
			"SELECT %s %s FROM `%s`",
			params.Comment,
			metadata.RecordIDColumn,
			params.Table,
		)
	}

	if params.Capped {
		return fmt.Sprintf(
			"SELECT %s %s, %s FROM `%s`",

			params.Comment,
			metadata.RecordIDColumn,
			metadata.DefaultColumn,
			params.Table,
		)
	}

	return fmt.Sprintf(
		"SELECT %s %s FROM `%s`",

		params.Comment,
		metadata.DefaultColumn,
		params.Table,
	)

}

func UnmarshalExplain(explainStr string) (*types.Document, error) {
	b := []byte(explainStr)

	var plan map[string]any
	if err := json.Unmarshal(b, &plan); err != nil {
		return nil, lazyerrors.Error(err)
	}

	return convertJSON(plan).(*types.Document), nil
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

type secondadyIndex struct {
	use     bool
	idxName string
}

type typePatchInfo struct {
	fieldPath  string
	scalarType string
	columnName string
}

func prepareWhereClause(sqlFilters *types.Document, meta *metadata.Collection) (string, *table.QueryParameters, *secondadyIndex, *typePatchInfo, error) {
	var conditions []string
	var args []table.ParameterOption
	secondaryIndex := &secondadyIndex{
		use:     false,
		idxName: "",
	}
	indexes := meta.Indexes

	typePatch := &typePatchInfo{
		fieldPath:  "",
		scalarType: "",
	}

	iter := sqlFilters.Iterator()
	defer iter.Close()

	// iterate through root document
	for {
		rootKey, rootVal, err := iter.Next()
		if err != nil {
			if errors.Is(err, iterator.ErrIteratorDone) {
				break
			}

			return "", nil, nil, nil, lazyerrors.Error(err)
		}

		if strings.HasPrefix(rootKey, "$") {
			continue
		}

		nestedPathCheck, err := types.NewPathFromString(rootKey)
		var pe *types.PathError
		switch {
		case err == nil:
			if nestedPathCheck.Len() > 1 {
				rootKey = metadata.DotNotationToJSONPath(rootKey)
			}

		case errors.As(err, &pe):
			// ignore empty key error, empty key is acceptable
			if pe.Code() != types.ErrPathElementEmpty {
				return "", nil, nil, nil, lazyerrors.Error(err)
			}
		default:
			panic("Invalid error type: PathError expected")
		}

		switch val := rootVal.(type) {
		case types.Binary, *types.Array, types.NullType, types.Regex, types.Timestamp:
			// not supported for pushdown
		case int32, int64, bool, string, types.ObjectID, float64, time.Time:
			if yqlExpr, param, useIndex := buildYQLComparisonExpr(rootKey, indexes, val, findOperator, "$eq"); yqlExpr != "" && param != nil {
				conditions = append(conditions, yqlExpr)
				args = append(args, param)
				secondaryIndex = useIndex
			}
		case *types.Document:
			iter := val.Iterator()
			defer iter.Close()

			for {
				k, v, err := iter.Next()
				if err != nil {
					if errors.Is(err, iterator.ErrIteratorDone) {
						break
					}

					return "", nil, nil, nil, lazyerrors.Error(err)
				}

				if k == "$type" && isScalar(v.(string)) {
					typePatch = &typePatchInfo{
						fieldPath:  rootKey,
						scalarType: v.(string),
						columnName: fmt.Sprintf("%s_%s", rootKey, v.(string)),
					}
				}

				if op, exists := pushdownOperators[k]; exists {
					if yqlExpr, param, useIndex := buildYQLComparisonExpr(rootKey, indexes, v, op, k); yqlExpr != "" && param != nil {
						conditions = append(conditions, yqlExpr)
						args = append(args, param)
						secondaryIndex = useIndex
					}
				}
			}

		default:
			return "", nil, nil, nil, lazyerrors.Errorf("unsupported filter type for field: %v", val)
		}
	}

	query := strings.Join(conditions, " AND ")
	return query, table.NewQueryParameters(args...), secondaryIndex, typePatch, nil
}

func transformCondition(input, field, targetType string) (string, error) {
	allTypes := metadata.GetColumnsInDefinedOrder()
	reValue := regexp.MustCompile(fmt.Sprintf(`\b%s_\w+\s*==\s*(\$\w+)`, field))
	m := reValue.FindStringSubmatch(input)
	if m == nil {
		return "", errors.New("не удалось найти плейсхолдер в исходной строке")
	}
	value := m[1]

	var parts []string
	for _, b := range allTypes {
		col := fmt.Sprintf("%s_%s", field, b)
		if b == targetType {
			parts = append(parts, fmt.Sprintf("%s == %s", col, value))
		} else {
			parts = append(parts, fmt.Sprintf("%s IS NULL", col))
		}
	}

	return strings.Join(parts, " AND "), nil
}

func buildJSONPath(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Sprintf(`%s.""`, jsonPathRoot)
	}
	if strings.Contains(key, "-") {
		return fmt.Sprintf(`%s."%s"`, jsonPathRoot, key)
	}
	return fmt.Sprintf("%s.%s", jsonPathRoot, key)
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

var pushdownOperators = map[string]string{
	"$eq":  "==",
	"$ne":  "!=",
	"$gte": ">=",
	"$lte": "<=",
	"$lt":  "<",
	"$gt":  ">",
}

var operatorsSupportedForIndexing = map[string]struct{}{
	"==": {},
}

type YQLExpressionParams struct {
	rootKey       string
	bsonType      string
	operator      string
	paramName     string
	path          string
	mongoOperator string
	useIndex      bool
	indexName     string
	value         any
}

func buildYQLExpression(info YQLExpressionParams) (string, table.ParameterOption) {
	if info.rootKey == idField {
		return buildIDIndexExpr(info.operator, info.bsonType, info.paramName, info.value)
	}
	if info.useIndex {
		return buildIndexedFieldExpr(info.rootKey, info.bsonType, info.operator, info.paramName, info.value)
	}
	return buildJSONFilterExpr(info.path, info.operator, info.bsonType, info.paramName, info.value, info.rootKey, info.mongoOperator)
}

func buildYQLComparisonExpr(rootKey string, indexes []metadata.IndexInfo, val any, operator string, mongoOp string) (string, table.ParameterOption, *secondadyIndex) {
	bsonType := sjson.GetTypeOfValue(val)
	ydbType := metadata.MapBSONTypeToYDBType(bsonType)
	ydbValue := metadata.MapBSONValueToYDBValue(bsonType, val)

	if ydbType == nil || ydbValue == nil {
		return "", nil, nil
	}

	path := buildJSONPath(rootKey)
	paramName := metadata.CleanRootKey(rootKey)

	useIndex := false
	var indexName string

	if _, ok := operatorsSupportedForIndexing[operator]; ok {
		for _, in := range indexes {
			for _, field := range in.Key {
				if field.Field == rootKey && in.Ready && rootKey != idField {
					useIndex = true // предварительно разрешаем всем типам
					indexName = in.Name
				}
			}

		}
	}

	if bsonType == "double" {
		useIndex = false
	}

	if mongoOp == "$type" && !useIndex {
		return "", nil, nil
	}

	expressionParams := YQLExpressionParams{
		useIndex:      useIndex,
		indexName:     indexName,
		rootKey:       rootKey,
		bsonType:      bsonType,
		operator:      operator,
		paramName:     paramName,
		path:          path,
		mongoOperator: mongoOp,
		value:         val,
	}

	yqlExpr, param := buildYQLExpression(expressionParams)

	useSecIndex := secondadyIndex{
		use:     useIndex,
		idxName: indexName,
	}

	return yqlExpr, param, &useSecIndex
}

func buildIDIndexExpr(op, bsonType, paramName string, val any) (string, table.ParameterOption) {
	ydbValue := metadata.MapBSONValueToYDBValue(bsonType, val)
	param := table.ValueParam(paramName, ydbValue)

	return fmt.Sprintf(`%s %s CAST($%s AS Utf8)`, metadata.DefaultIDColumn, op, paramName), param
}

func buildJSONFilterExpr(path, op, bsonType, paramName string, val any, rootKey string, mongoOp string) (string, table.ParameterOption) {
	adjustedVal := val

	if mongoOp == "$eq" {
		switch v := adjustedVal.(type) {
		case int64:
			maxSafeDouble := int64(types.MaxSafeDouble)

			// If value cannot be safe double, fetch all numbers out of the safe range.
			switch {
			case v > maxSafeDouble:
				op = ">"
				adjustedVal = maxSafeDouble

			case v < -maxSafeDouble:
				op = "<"
				adjustedVal = -maxSafeDouble
			default:
				// don't change the default eq query
			}

		case float64:
			switch {
			case v > types.MaxSafeDouble:
				op = ">"
				adjustedVal = types.MaxSafeDouble

			case v < -types.MaxSafeDouble:
				op = "<"
				adjustedVal = -types.MaxSafeDouble
			default:
				// don't change the default eq query
			}
		}
	}

	ydbValue := metadata.MapBSONValueToYDBValueForJsonQuery(bsonType, adjustedVal)
	param := table.ValueParam(paramName, ydbValue)

	if mongoOp == "$ne" {
		return fmt.Sprintf(
			`NOT JSON_EXISTS(_jsonb, '$ ? ( 
											exists(@.%s) 
											&& @.%s == $param 
											&& @.\"$s\".p.%s.t == \"%s\")' PASSING $%s AS "param")`,
			rootKey, rootKey, rootKey, bsonType, paramName,
		), param
	}

	return fmt.Sprintf(
		`JSON_EXISTS(_jsonb, '%s ? (@ %s $param)' PASSING $%s AS "param")`,
		path, op, paramName,
	), param
}

// Константы для типов скалярных данных
const (
	IntType    = "int"
	LongType   = "long"
	DoubleType = "double"
)

func isScalar(colType string) bool {
	switch colType {
	case IntType, LongType, DoubleType:
		return true
	default:
		return false
	}
}

func buildIndexedFieldExpr(rootKey, bsonType, op, paramName string, val any) (string, table.ParameterOption) {
	ydbValue := metadata.MapBSONValueToYDBValue(bsonType, val)
	param := table.ValueParam(paramName, ydbValue)
	isTargetIsScalar := isScalar(bsonType)

	scalarColumns := []string{
		fmt.Sprintf("%s_%s", rootKey, LongType),
		fmt.Sprintf("%s_%s", rootKey, IntType),
		fmt.Sprintf("%s_%s", rootKey, DoubleType),
	}

	scalarSeparator := " OR "
	var scalarGroupBuilder strings.Builder
	for _, v := range scalarColumns {
		if scalarGroupBuilder.Len() > 0 {
			scalarGroupBuilder.WriteString(scalarSeparator)
		}
		if strings.Contains(v, DoubleType) {
			scalarGroupBuilder.WriteString(fmt.Sprintf("%s %s CAST($%s AS Double)", v, op, paramName))
		} else {
			scalarGroupBuilder.WriteString(fmt.Sprintf("%s %s $%s", v, op, paramName))
		}
	}
	scalarGroup := fmt.Sprintf("(%s)", scalarGroupBuilder.String())

	columnsOrder := metadata.GetColumnsInDefinedOrder()

	var sb strings.Builder
	separator := " AND "

	for _, colType := range columnsOrder {
		column := fmt.Sprintf("%s_%s", rootKey, colType)

		if colType == LongType && isTargetIsScalar {
			sb.WriteString(scalarGroup)
		}
		if !isScalar(colType) || !isTargetIsScalar {
			sb.WriteString(fmt.Sprintf("%s IS NULL", column))
			sb.WriteString(separator)
		}
	}

	q := sb.String()

	if !isTargetIsScalar {
		indexedColumn := fmt.Sprintf("%s_%s", rootKey, bsonType)
		replacer := fmt.Sprintf("%s IS NULL", indexedColumn)
		replacement := fmt.Sprintf("%s %s $%s", indexedColumn, op, paramName)
		q = strings.Replace(q, replacer, replacement, 1)
	}

	q = strings.TrimSuffix(q, separator)

	return q, param
}

func buildUpsertQuery(pathPrefix, tableName string, extra map[string]metadata.IndexColumn) string {
	var fieldDecls = []string{
		fmt.Sprintf("id: %s", ydbTypes.TypeString.String()),
		fmt.Sprintf("%s: %s", metadata.DefaultColumn, ydbTypes.TypeJSON.String()),
	}
	var selectFields = []string{"id", metadata.DefaultColumn}

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

func buildInsertQuery(pathPrefix, tableName string, capped bool, extra map[string]metadata.IndexColumn) string {
	var fieldDecls = []string{
		fmt.Sprintf("id: %s", ydbTypes.TypeString.String()),
		fmt.Sprintf("%s: %s", metadata.DefaultColumn, ydbTypes.TypeJSON.String()),
	}
	var selectFields = []string{"id", metadata.DefaultColumn}

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
