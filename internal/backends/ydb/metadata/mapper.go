package metadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"io"
)

func MapToYdbType(data []byte, field string) (ydbTypes.Type, error) {
	var v map[string]json.RawMessage
	r := bytes.NewReader(data)
	dec := json.NewDecoder(r)

	err := dec.Decode(&v)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if err = checkConsumed(dec, r); err != nil {
		return nil, lazyerrors.Error(err)
	}

	jsch, ok := v["$s"]
	if !ok {
		return nil, lazyerrors.Errorf("schema is not set")
	}

	var sch schema
	r = bytes.NewReader(jsch)
	dec = json.NewDecoder(r)
	dec.DisallowUnknownFields()

	if err := dec.Decode(&sch); err != nil {
		return nil, lazyerrors.Error(err)
	}

	if err := checkConsumed(dec, r); err != nil {
		return nil, lazyerrors.Error(err)
	}

	fieldSchema, ok := sch.Properties[field]
	if !ok {
		return nil, lazyerrors.Errorf("field %q not found in schema", field)
	}

	ydbType, err := UnmarshalYDBValue(fieldSchema)
	if ydbType == nil {
		return nil, lazyerrors.Errorf("unhandled type %q for field %q", fieldSchema.Type, field)
	}
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return ydbType, nil
}

func UnmarshalYDBValue(sch *elem) (ydbTypes.Type, error) {
	if sch == nil {
		return nil, lazyerrors.Errorf("schema is not set")
	}

	var res ydbTypes.Type

	switch sch.Type {
	case elemTypeDouble:
		res = ydbTypes.TypeDouble
	case elemTypeString:
		res = ydbTypes.TypeString
	case elemTypeBinData:
		res = ydbTypes.TypeBytes
	case elemTypeBool:
		res = ydbTypes.TypeBool
	case elemTypeDate:
		res = ydbTypes.TypeDate
	case elemTypeInt:
		res = ydbTypes.TypeInt32
	case elemTypeTimestamp:
		res = ydbTypes.TypeTimestamp
	case elemTypeLong:
		res = ydbTypes.TypeInt64
	case elemTypeObjectID:
		return res, lazyerrors.Errorf("object IDs don't need to be processed")
	default:
		return ydbTypes.TypeUnknown, lazyerrors.Errorf("UnmarshalYDBValue: unhandled type %q", sch.Type)
	}

	return res, nil
}

func ConvertToYDBValueByStringRepresentation(t any, v any) ydbTypes.Value {
	switch t {
	case "Utf8":
		return ydbTypes.UTF8Value(v.(string))
	case "Int32":
		return ydbTypes.Int32Value(v.(int32))
	case "Int64":
		return ydbTypes.Int64Value(v.(int64))
	case "Double":
		return ydbTypes.DoubleValue(v.(float64))
	case "Bool":
		return ydbTypes.BoolValue(v.(bool))
	case "String":
		return ydbTypes.BytesValueFromString(v.(string))
	default:
		panic(fmt.Sprintf("unsupported type: %v", t))
	}
}

func checkConsumed(dec *json.Decoder, r *bytes.Reader) error {
	if dr := dec.Buffered().(*bytes.Reader); dr.Len() != 0 {
		b, _ := io.ReadAll(dr)

		if l := len(b); l != 0 {
			return lazyerrors.Errorf("%d bytes remains in the decoder: %s", l, b)
		}
	}

	if l := r.Len(); l != 0 {
		b, _ := io.ReadAll(r)
		return lazyerrors.Errorf("%d bytes remains in the reader: %s", l, b)
	}

	return nil
}

type schema struct {
	Properties map[string]*elem `json:"p"`  // document's properties
	Keys       []string         `json:"$k"` // to preserve properties' order
}

type elem struct {
	Type    elemType             `json:"t"`            // for each field
	Schema  *schema              `json:"$s,omitempty"` // only for objects
	Options *string              `json:"o,omitempty"`  // only for regex
	Items   []*elem              `json:"i,omitempty"`  // only for arrays
	Subtype *types.BinarySubtype `json:"s,omitempty"`  // only for binData
}

type elemType string

const (
	elemTypeDouble    elemType = "double"
	elemTypeString    elemType = "string"
	elemTypeBinData   elemType = "binData"
	elemTypeObjectID  elemType = "objectId"
	elemTypeBool      elemType = "bool"
	elemTypeDate      elemType = "date"
	elemTypeInt       elemType = "int"
	elemTypeTimestamp elemType = "timestamp"
	elemTypeLong      elemType = "long"
)
