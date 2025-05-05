package ydb

import (
	"errors"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/handler/sjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/iterator"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"strings"
	"time"
)

const (
	jsonPathRoot = "$"
)

type FieldOp string

const (
	FieldOpEq FieldOp = "$eq"
	FieldOpNe FieldOp = "$ne"
)

type CompareOp string

const (
	CompareOpEq CompareOp = "=="
	CompareOpNe CompareOp = "!="
	CompareOpGt CompareOp = ">"
	CompareOpLt CompareOp = "<"
)

var pushdownOperators = map[FieldOp]CompareOp{
	FieldOpEq: CompareOpEq,
	FieldOpNe: CompareOpNe,
}

var operatorsSupportedForIndexing = map[FieldOp]CompareOp{
	FieldOpEq: CompareOpEq,
}

func IsSupportedForPushdown(opStr string) bool {
	op := FieldOp(opStr)
	_, ok := pushdownOperators[op]
	return ok
}

func GetCompareOp(op FieldOp) CompareOp {
	return pushdownOperators[op]
}

func IsIndexable(op FieldOp) bool {
	_, ok := operatorsSupportedForIndexing[op]
	return ok
}

type secondaryIndex struct {
	idxName *string
}

type whereExpressionParams struct {
	rootKey       string
	bsonType      string
	paramName     string
	path          string
	mongoOperator FieldOp
	indexName     *string
	value         any
}

type conditionExpressionResult struct {
	YQLExpression string
	ParamOption   table.ParameterOption
	SecondaryIdx  *secondaryIndex
}

func prepareSelectClause(params *metadata.SelectParams) string {
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

func prepareWhereClause(sqlFilters *types.Document, meta *metadata.Collection) (string, *table.QueryParameters, *secondaryIndex, error) {
	var conditions []string
	var args []table.ParameterOption
	secIndex := &secondaryIndex{
		idxName: nil,
	}
	indexes := meta.Indexes

	iter := sqlFilters.Iterator()
	defer iter.Close()

	// iterate through root document
	for {
		rootKey, rootVal, err := iter.Next()
		if err != nil {
			if errors.Is(err, iterator.ErrIteratorDone) {
				break
			}

			return "", nil, nil, lazyerrors.Error(err)
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
				return "", nil, nil, lazyerrors.Error(err)
			}
		default:
			panic("Invalid error type: PathError expected")
		}

		switch val := rootVal.(type) {
		case types.Binary, *types.Array, types.NullType, types.Regex, types.Timestamp:
			// not supported for pushdown
		case int32, int64, bool, string, types.ObjectID, float64, time.Time:
			if res := getConditionExpr(rootKey, indexes, val, FieldOpEq); res != nil {
				conditions = append(conditions, res.YQLExpression)
				args = append(args, res.ParamOption)
				secIndex = res.SecondaryIdx
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

					return "", nil, nil, lazyerrors.Error(err)
				}

				if IsSupportedForPushdown(k) {
					if res := getConditionExpr(rootKey, indexes, v, FieldOp(k)); res != nil {
						conditions = append(conditions, res.YQLExpression)
						args = append(args, res.ParamOption)
						secIndex = res.SecondaryIdx
					}
				}
			}

		default:
			return "", nil, nil, lazyerrors.Errorf("unsupported filter type for field: %v", val)
		}
	}
	query := strings.Join(conditions, " AND ")

	return query, table.NewQueryParameters(args...), secIndex, nil
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

func buildYQLExpression(info whereExpressionParams) (string, table.ParameterOption) {
	operator := GetCompareOp(info.mongoOperator)

	if info.indexName != nil {
		return buildIndexedFieldExpr(info.rootKey, info.bsonType, info.paramName, operator, info.value)
	}

	return buildJSONFilterExpr(info.path, info.bsonType, info.paramName, info.value, info.rootKey, operator)
}

func getConditionExpr(rootKey string, indexes []metadata.IndexInfo, val any, mongoOp FieldOp) *conditionExpressionResult {
	bsonType := sjson.GetTypeOfValue(val)
	ydbType := metadata.MapBSONTypeToYDBType(bsonType)
	ydbValue := metadata.MapBSONValueToYDBValue(bsonType, val)

	if ydbType == nil || ydbValue == nil {
		return nil
	}

	path := buildJSONPath(rootKey)
	paramName := metadata.CleanRootKey(rootKey)

	var indexName *string
	if IsIndexable(mongoOp) && isIndexableType(bsonType) {
		for _, in := range indexes {
			for _, field := range in.Key {
				if field.Field == rootKey && in.Ready {
					indexName = &in.Name
				}
			}
		}
	}

	expressionParams := whereExpressionParams{
		indexName:     indexName,
		rootKey:       rootKey,
		bsonType:      bsonType,
		paramName:     paramName,
		path:          path,
		mongoOperator: mongoOp,
		value:         val,
	}

	yqlExpr, param := buildYQLExpression(expressionParams)

	return &conditionExpressionResult{
		YQLExpression: yqlExpr,
		ParamOption:   param,
		SecondaryIdx: &secondaryIndex{
			idxName: indexName,
		},
	}
}

func buildJSONFilterExpr(path, bsonType, paramName string, val any, rootKey string, op CompareOp) (string, table.ParameterOption) {
	adjustedVal := val

	if op == CompareOpEq {
		switch v := adjustedVal.(type) {
		case int64:
			maxSafeDouble := int64(types.MaxSafeDouble)

			// If value cannot be safe double, fetch all numbers out of the safe range.
			switch {
			case v > maxSafeDouble:
				op = CompareOpGt
				adjustedVal = maxSafeDouble

			case v < -maxSafeDouble:
				op = CompareOpLt
				adjustedVal = -maxSafeDouble
			default:
				// don't change the default eq query
			}

		case float64:
			switch {
			case v > types.MaxSafeDouble:
				op = CompareOpGt
				adjustedVal = types.MaxSafeDouble

			case v < -types.MaxSafeDouble:
				op = CompareOpLt
				adjustedVal = -types.MaxSafeDouble
			default:
				// don't change the default eq query
			}
		}
	}

	ydbValue := metadata.MapBSONValueToYDBValueForJsonQuery(bsonType, adjustedVal)
	param := table.ValueParam(paramName, ydbValue)

	if op == CompareOpNe {
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

func buildIndexedFieldExpr(rootKey, bsonType, paramName string, op CompareOp, val any) (string, table.ParameterOption) {
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

func getTypesForIndexSearch() map[string]struct{} {
	return map[string]struct{}{
		"string":   {},
		"objectId": {},
		"bool":     {},
		"date":     {},
		"long":     {},
		"int":      {},
	}
}

func isIndexableType(bsonType string) bool {
	supportedTypes := getTypesForIndexSearch()
	_, exists := supportedTypes[bsonType]

	return exists
}
