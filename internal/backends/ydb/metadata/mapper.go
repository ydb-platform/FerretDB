package metadata

import (
	"encoding/hex"
	"github.com/FerretDB/FerretDB/internal/types"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

func SupportedIndexTypes() map[string]ydbTypes.Type {
	return map[string]ydbTypes.Type{
		"string":   ydbTypes.TypeString,
		"objectId": ydbTypes.TypeString,
		"bool":     ydbTypes.TypeBool,
		"date":     ydbTypes.TypeInt64,
		"long":     ydbTypes.TypeInt64,
		"int":      ydbTypes.TypeInt32,
		"double":   ydbTypes.TypeInt64, // when double can be represented as int64
	}
}

func SupportedIndexValues(val any) map[string]ydbTypes.Value {
	return map[string]ydbTypes.Value{
		"string": ydbTypes.BytesValueFromString(val.(string)),
		"bool":   ydbTypes.BoolValue(val.(bool)),
		"long":   ydbTypes.Int64Value(val.(int64)),
		"int":    ydbTypes.Int32Value(val.(int32)),
	}
}

func MapBSONTypeToYDBType(bsonType string) ydbTypes.Type {
	switch bsonType {
	case "string", "objectId":
		return ydbTypes.TypeString
	case "int":
		return ydbTypes.TypeInt32
	case "long":
		return ydbTypes.TypeInt64
	case "double":
		return ydbTypes.TypeDouble
	case "bool":
		return ydbTypes.TypeBool
	case "date":
		return ydbTypes.TypeInt64
	default:
		return nil
	}
}

func MapBSONValueToYDBValueForJsonQuery(bsonType string, val any) ydbTypes.Value {
	switch bsonType {
	case "string":
		return ydbTypes.UTF8Value(val.(string))
	case "objectId":
		oid := val.(types.ObjectID)
		return ydbTypes.UTF8Value(hex.EncodeToString(oid[:]))
	case "int":
		return ydbTypes.Int32Value(val.(int32))
	case "long":
		return ydbTypes.Int64Value(val.(int64))
	case "double":
		return ydbTypes.DoubleValue(val.(float64))
	case "bool":
		return ydbTypes.BoolValue(val.(bool))
	case "date":
		v := val.(time.Time)
		date := primitive.NewDateTimeFromTime(v)
		return ydbTypes.Int64Value(int64(date))
	default:
		return nil
	}
}

func MapBSONValueToYDBValue(bsonType string, val any) ydbTypes.Value {
	switch bsonType {
	case "string":
		return ydbTypes.BytesValueFromString(val.(string))
	case "objectId":
		oid := val.(types.ObjectID)
		return ydbTypes.BytesValueFromString(hex.EncodeToString(oid[:]))
	case "int":
		return ydbTypes.Int32Value(val.(int32))
	case "long":
		return ydbTypes.Int64Value(val.(int64))
	case "double":
		return ydbTypes.DoubleValue(val.(float64))
	case "bool":
		return ydbTypes.BoolValue(val.(bool))
	case "date":
		v := val.(time.Time)
		date := primitive.NewDateTimeFromTime(v)
		return ydbTypes.Int64Value(int64(date))
	default:
		return nil
	}
}
