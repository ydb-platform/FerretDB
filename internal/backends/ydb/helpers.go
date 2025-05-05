package ydb

import (
	"encoding/json"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"golang.org/x/exp/maps"
)

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
