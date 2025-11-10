package metadata

import (
	"encoding/hex"
	"github.com/FerretDB/FerretDB/internal/types"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"math"
	"strconv"
	"time"
)

type ColumnAlias string

const (
	ColumnString   ColumnAlias = "string"
	ColumnObjectId ColumnAlias = "objectId"
	ColumnBool     ColumnAlias = "bool"
	ColumnDate     ColumnAlias = "date"
	ColumnScalar   ColumnAlias = "scalar"
)

type BsonType string

const (
	BsonString   BsonType = "string"
	BsonObjectId BsonType = "objectId"
	BsonBool     BsonType = "bool"
	BsonDate     BsonType = "date"
	BsonInt      BsonType = "int"
	BsonLong     BsonType = "long"
	BsonDouble   BsonType = "double"
)

var (
	ColumnOrder = []ColumnAlias{
		ColumnString,
		ColumnObjectId,
		ColumnScalar,
		ColumnDate,
		ColumnBool,
	}

	bsonTypeToColumnStore = map[BsonType]ColumnAlias{
		BsonString:   ColumnString,
		BsonObjectId: ColumnObjectId,
		BsonBool:     ColumnBool,
		BsonDate:     ColumnDate,
		BsonInt:      ColumnScalar,
		BsonLong:     ColumnScalar,
		BsonDouble:   ColumnScalar,
	}

	bsonToYdbType = map[BsonType]ydbTypes.Type{
		BsonString:   ydbTypes.TypeString,
		BsonObjectId: ydbTypes.TypeString,
		BsonInt:      ydbTypes.TypeDyNumber,
		BsonLong:     ydbTypes.TypeDyNumber,
		BsonDouble:   ydbTypes.TypeDyNumber,
		BsonBool:     ydbTypes.TypeBool,
		BsonDate:     ydbTypes.TypeInt64,
	}

	columnStoreToYdbType = map[ColumnAlias]ydbTypes.Type{
		ColumnString:   ydbTypes.TypeString,
		ColumnObjectId: ydbTypes.TypeString,
		ColumnBool:     ydbTypes.TypeBool,
		ColumnDate:     ydbTypes.TypeInt64,
		ColumnScalar:   ydbTypes.TypeDyNumber,
	}
)

var IndexedBsonTypes = map[BsonType]struct{}{
	BsonString:   {},
	BsonObjectId: {},
	BsonBool:     {},
	BsonDate:     {},
	BsonLong:     {},
	BsonDouble:   {},
	BsonInt:      {},
}

func BsonTypeToYdbType(bsonType BsonType) ydbTypes.Type {
	if t, ok := bsonToYdbType[bsonType]; ok {
		return t
	}

	return nil
}

func BsonTypeToColumnStore(bsonType BsonType) ColumnAlias {
	if col, ok := bsonTypeToColumnStore[bsonType]; ok {
		return col
	}

	return ""
}

func ColumnStoreToYdbType(col ColumnAlias) ydbTypes.Type {
	if t, ok := columnStoreToYdbType[col]; ok {
		return t
	}

	return nil
}

var scalarTypes = map[BsonType]struct{}{
	BsonInt:    {},
	BsonLong:   {},
	BsonDouble: {},
}

func isScalar(colType BsonType) bool {
	_, ok := scalarTypes[colType]
	return ok
}

type converterFunc func(val any) ydbTypes.Value

var convertersForJsonQuery = map[BsonType]converterFunc{
	BsonString: func(val any) ydbTypes.Value {
		return ydbTypes.UTF8Value(val.(string))
	},
	BsonObjectId: func(val any) ydbTypes.Value {
		oid := val.(types.ObjectID)
		return ydbTypes.UTF8Value(hex.EncodeToString(oid[:]))
	},
	BsonInt: func(val any) ydbTypes.Value {
		return ydbTypes.Int32Value(val.(int32))
	},
	BsonLong: func(val any) ydbTypes.Value {
		return ydbTypes.Int64Value(val.(int64))
	},
	BsonDouble: func(val any) ydbTypes.Value {
		return ydbTypes.DoubleValue(val.(float64))
	},
	BsonBool: func(val any) ydbTypes.Value {
		return ydbTypes.BoolValue(val.(bool))
	},
	BsonDate: func(val any) ydbTypes.Value {
		v := val.(time.Time)
		date := primitive.NewDateTimeFromTime(v)
		return ydbTypes.Int64Value(int64(date))
	},
}

var convertersForColumnQuery = map[BsonType]converterFunc{
	BsonString: func(val any) ydbTypes.Value {
		return ydbTypes.BytesValueFromString(val.(string))
	},
	BsonObjectId: func(val any) ydbTypes.Value {
		oid := val.(types.ObjectID)
		return ydbTypes.BytesValueFromString(hex.EncodeToString(oid[:]))
	},
	BsonInt: func(val any) ydbTypes.Value {
		i := val.(int32)
		s := strconv.FormatInt(int64(i), 10)
		return ydbTypes.DyNumberValue(s)
	},
	BsonLong: func(val any) ydbTypes.Value {
		i := val.(int64)
		s := strconv.FormatInt(i, 10)
		return ydbTypes.DyNumberValue(s)
	},
	BsonDouble: func(val any) ydbTypes.Value {
		f := val.(float64)
		s := strconv.FormatFloat(f, 'f', -1, 64)
		return ydbTypes.DyNumberValue(s)
	},
	BsonBool: func(val any) ydbTypes.Value {
		return ydbTypes.BoolValue(val.(bool))
	},
	BsonDate: func(val any) ydbTypes.Value {
		v := val.(time.Time)
		date := primitive.NewDateTimeFromTime(v)
		return ydbTypes.Int64Value(int64(date))
	},
}

func BsonValueToYdbValueForJsonQuery(bsonType BsonType, val any) ydbTypes.Value {
	if conv, ok := convertersForJsonQuery[bsonType]; ok {
		return conv(val)
	}
	return nil
}

func BsonValueToYdbValue(bsonType BsonType, val any) ydbTypes.Value {
	if conv, ok := convertersForColumnQuery[bsonType]; ok {
		return conv(val)
	}

	return nil
}

func float64ToOrderedUint64(f float64) uint64 {
	temp := math.Float64bits(f)
	tempAsInt64 := int64(temp)
	shifted := tempAsInt64 >> 63
	signShifted := uint64(shifted)
	mask := signShifted | 0x8000000000000000

	return temp ^ mask
}
