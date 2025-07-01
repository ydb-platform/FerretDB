package ydb

import (
	"errors"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/handler/sjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/iterator"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"strings"
	"time"
)

const (
	jsonPathRoot = "$"
)

type MongoOp string

const (
	FieldOpEq MongoOp = "$eq"
	FieldOpNe MongoOp = "$ne"
)

type CompareOp string

const (
	CompareOpEq CompareOp = "=="
	CompareOpNe CompareOp = "!="
	CompareOpGt CompareOp = ">"
	CompareOpLt CompareOp = "<"
)

var pushdownOperators = map[MongoOp]CompareOp{
	FieldOpEq: CompareOpEq,
	FieldOpNe: CompareOpNe,
}

// TODO: this is the only operators which is supported for indexed search queries using columns
var operatorsSupportedForIndexing = map[MongoOp]CompareOp{
	FieldOpEq: CompareOpEq,
}

func IsSupportedForPushdown(opStr string) bool {
	op := MongoOp(opStr)
	_, ok := pushdownOperators[op]
	return ok
}

func GetCompareOp(op MongoOp) CompareOp {
	return pushdownOperators[op]
}

func IsIndexableOp(op MongoOp) bool {
	_, ok := operatorsSupportedForIndexing[op]
	return ok
}

type secondaryIndex struct {
	idxName *string
}

type whereExpressionParams struct {
	rootKey       string
	bsonType      metadata.BsonType
	path          string
	mongoOperator MongoOp
	useIndex      bool
	value         any
}

type conditionExpressionResult struct {
	Expression   string
	ParamOptions []table.ParameterOption
	SecondaryIdx *secondaryIndex
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
			"%s %s %s %s `%s`",
			SelectWord,
			params.Comment,
			metadata.RecordIDColumn,
			FromWord,
			params.Table,
		)
	}

	if params.Capped {
		return fmt.Sprintf(
			"%s %s %s, %s %s `%s`",
			SelectWord,
			params.Comment,
			metadata.RecordIDColumn,
			metadata.DefaultColumn,
			FromWord,
			params.Table,
		)
	}

	return fmt.Sprintf(
		"%s %s %s %s `%s`",
		SelectWord,
		params.Comment,
		metadata.DefaultColumn,
		FromWord,
		params.Table,
	)

}

func prepareWhereClause(sqlFilters *types.Document, meta *metadata.Collection, placeholder *metadata.Placeholder) (string, []table.ParameterOption, *secondaryIndex, error) {
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
				rootKey = metadata.DotNotationToJsonPath(rootKey)
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
			if res := getConditionExpr(rootKey, indexes, val, FieldOpEq, placeholder); res != nil {
				conditions = append(conditions, res.Expression)
				args = append(args, res.ParamOptions...)
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
					if res := getConditionExpr(rootKey, indexes, v, MongoOp(k), placeholder); res != nil {
						conditions = append(conditions, res.Expression)
						args = append(args, res.ParamOptions...)
						secIndex = res.SecondaryIdx
					}
				}
			}

		default:
			return "", nil, nil, lazyerrors.Errorf("unsupported filter type for field: %v", val)
		}
	}
	query := strings.Join(conditions, " AND ")

	return query, args, secIndex, nil
}

func prepareLimitClause(params *backends.QueryParams, placeholder *metadata.Placeholder) (string, table.ParameterOption) {
	limitParamName := placeholder.Next()

	if params.Limit != 0 {
		return limitParamName, table.ValueParam(limitParamName, ydbTypes.Uint64Value(uint64(params.Limit)))
	} else {
		return limitParamName, table.ValueParam(limitParamName, ydbTypes.Uint64Value(defaultRowsLimit))
	}
}

func buildPathToField(key string) string {
	key = strings.TrimSpace(key)

	if key == "" {
		return fmt.Sprintf(`%s.""`, jsonPathRoot)
	}

	if strings.Contains(key, "-") {
		return fmt.Sprintf(`%s."%s"`, jsonPathRoot, key)
	}

	return fmt.Sprintf(`%s.%s`, jsonPathRoot, key)

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

	return fmt.Sprintf(" %s %s%s", OrderByWord, metadata.RecordIDColumn, order)
}

func buildWhereExpression(info whereExpressionParams, placeholder *metadata.Placeholder) (string, []table.ParameterOption) {
	operator := GetCompareOp(info.mongoOperator)

	if info.useIndex {
		return buildIndexedFieldExpr(info.rootKey, info.bsonType, operator, info.value, placeholder)
	}

	return buildJsonPathExpr(info.path, info.bsonType, info.value, info.rootKey, operator, placeholder)
}

func getConditionExpr(rootKey string, indexes []metadata.IndexInfo, val any, mongoOp MongoOp, placeholder *metadata.Placeholder) *conditionExpressionResult {
	stringType := sjson.GetTypeOfValue(val)
	bsonType := metadata.BsonType(stringType)
	ydbType := metadata.BsonTypeToYdbType(bsonType)
	ydbValue := metadata.BsonValueToYdbValue(bsonType, val)

	if ydbType == nil || ydbValue == nil {
		return nil
	}

	path := buildPathToField(rootKey)
	secIdx := findSecondaryIndex(rootKey, bsonType, mongoOp, indexes)
	useIndex := secIdx != nil || rootKey == metadata.IdMongoField

	expressionParams := whereExpressionParams{
		useIndex:      useIndex,
		rootKey:       rootKey,
		bsonType:      bsonType,
		path:          path,
		mongoOperator: mongoOp,
		value:         val,
	}

	yqlExpr, param := buildWhereExpression(expressionParams, placeholder)

	return &conditionExpressionResult{
		Expression:   yqlExpr,
		ParamOptions: param,
		SecondaryIdx: secIdx,
	}
}

func findSecondaryIndex(rootKey string, bsonType metadata.BsonType, mongoOp MongoOp, indexes []metadata.IndexInfo) *secondaryIndex {
	if rootKey == metadata.IdMongoField {
		return nil
	}

	if !IsIndexableOp(mongoOp) || !isIndexableType(bsonType) {
		return nil
	}

	for _, idx := range indexes {
		if !idx.Ready {
			continue
		}
		for _, fld := range idx.Key {
			if fld.Field == rootKey {
				return &secondaryIndex{
					idxName: &idx.SanitizedName,
				}
			}
		}
	}

	return nil
}

func buildJsonPathExpr(path string, bsonType metadata.BsonType, val any, rootKey string, op CompareOp, placeholder *metadata.Placeholder) (string, []table.ParameterOption) {
	params := make([]table.ParameterOption, 0)
	adjustedVal := val

	if op == CompareOpEq {
		switch v := adjustedVal.(type) {
		case int64:
			adjustedVal, op = adjustInt64Value(v)
		case float64:
			adjustedVal, op = adjustFloat64Value(v)
		}
	}

	ydbValue := metadata.BsonValueToYdbValueForJsonQuery(bsonType, adjustedVal)
	paramName := placeholder.Next()
	param := table.ValueParam(paramName, ydbValue)
	params = append(params, param)

	if op == CompareOpNe {
		return getNotEqualJsonFilterExpr(rootKey, bsonType, paramName), params
	}

	return getDefaultJsonFilterExpr(path, paramName, op), params
}

func adjustInt64Value(v int64) (any, CompareOp) {
	maxSafeDouble := int64(types.MaxSafeDouble)

	switch {
	case v > maxSafeDouble:
		return maxSafeDouble, CompareOpGt
	case v < -maxSafeDouble:
		return -maxSafeDouble, CompareOpLt
	default:
		return v, CompareOpEq
	}
}

func adjustFloat64Value(v float64) (any, CompareOp) {
	switch {
	case v > types.MaxSafeDouble:
		return types.MaxSafeDouble, CompareOpGt
	case v < -types.MaxSafeDouble:
		return -types.MaxSafeDouble, CompareOpLt
	default:
		return v, CompareOpEq
	}
}

func getNotEqualJsonFilterExpr(rootKey string, bsonType metadata.BsonType, paramName string) string {
	return fmt.Sprintf(
		`NOT JSON_EXISTS(_jsonb, '$ ? ( 
											exists(@.%s) 
											&& @.%s == $param 
											&& @.\"$s\".p.%s.t == \"%s\")' PASSING %s AS "param")`,
		rootKey, rootKey, rootKey, bsonType, paramName,
	)
}

func getDefaultJsonFilterExpr(path, paramName string, op CompareOp) string {
	return fmt.Sprintf(
		`JSON_EXISTS(_jsonb, '%s ? (@ %s $param)' PASSING %s AS "param")`,
		path, op, paramName,
	)
}

func buildIndexedFieldExpr(rootKey string, bsonType metadata.BsonType, op CompareOp, val any, placeholder *metadata.Placeholder) (string, []table.ParameterOption) {
	params := make([]table.ParameterOption, 0)

	ydbValue := metadata.BsonValueToYdbValue(bsonType, val)
	paramName := placeholder.Next()
	params = append(params, table.ValueParam(paramName, ydbValue))

	targetColumnSuffix := metadata.BsonTypeToColumnStore(bsonType)
	targetColumn := fmt.Sprintf("%s_%s", rootKey, targetColumnSuffix)

	var conditions []string

	if rootKey == metadata.IdMongoField && op == CompareOpEq {
		bid, _ := sjson.MarshalSingleValue(val)
		idHash := generateIdHash(bid, bsonType)

		idParamName := placeholder.Next()
		params = append(params, table.ValueParam(idParamName, ydbTypes.Uint64Value(idHash)))

		conditions = append(conditions, fmt.Sprintf("%s%s%s", metadata.IdHashColumn, op, idParamName))
	}

	for _, colType := range metadata.ColumnOrder {
		colName := fmt.Sprintf("%s_%s", rootKey, colType)
		if colName == targetColumn {
			conditions = append(conditions, fmt.Sprintf("%s %s %s", colName, op, paramName))
		} else {
			conditions = append(conditions, fmt.Sprintf("%s IS NULL", colName))
		}
	}

	q := strings.Join(conditions, " "+AndWord+" ")

	return q, params
}

func isIndexableType(bsonType metadata.BsonType) bool {
	_, exists := metadata.IndexedBsonTypes[bsonType]

	return exists
}
