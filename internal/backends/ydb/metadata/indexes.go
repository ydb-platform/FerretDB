package metadata

import (
	"errors"
	"log/slog"
	"slices"

	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/iterator"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
)

// Indexes represents information about all indexes in a collection.
type Indexes []IndexInfo

// IndexInfo represents information about a single index.
type IndexInfo struct {
	Name    string
	YDBType YDBIndexType
	Key     []IndexKeyPair
	Unique  bool
}

// IndexKeyPair consists of a field name and a sort order that are part of the index.
type IndexKeyPair struct {
	Field      string
	Descending bool
}

type YDBIndexType string

const (
	GlobalIndex YDBIndexType = "GLOBAL"
)

// deepCopy returns a deep copy.
func (indexes Indexes) deepCopy() Indexes {
	res := make(Indexes, len(indexes))

	for i, index := range indexes {
		res[i] = IndexInfo{
			Name:    index.Name,
			YDBType: index.YDBType,
			Key:     slices.Clone(index.Key),
			Unique:  index.Unique,
		}
	}

	return res
}

// marshal returns [*types.Array] for indexes.
func (indexes Indexes) marshal() *types.Array {
	res := types.MakeArray(len(indexes))

	for _, index := range indexes {
		key := types.MakeDocument(len(index.Key))

		for _, pair := range index.Key {
			order := int32(1)
			if pair.Descending {
				slog.Warn("YDB не поддерживает Descending-индексы, поле `%s` будет ASC.", "field", pair.Field)
			}
			key.Set(pair.Field, order)
		}

		res.Append(must.NotFail(types.NewDocument(
			"ydbindex", string(index.YDBType),
			"name", index.Name,
			"key", key,
			"unique", index.Unique,
		)))
	}

	return res
}

// unmarshal sets indexes from [*types.Array].
func (s *Indexes) unmarshal(a *types.Array) error {
	res := make(Indexes, a.Len())

	iter := a.Iterator()
	defer iter.Close()

	for {
		i, v, err := iter.Next()
		if errors.Is(err, iterator.ErrIteratorDone) {
			break
		}

		if err != nil {
			return lazyerrors.Error(err)
		}

		index := v.(*types.Document)

		keyDoc := must.NotFail(index.Get("key")).(*types.Document)
		fields := keyDoc.Keys()
		key := make([]IndexKeyPair, keyDoc.Len())

		for j, f := range fields {
			key[j] = IndexKeyPair{
				Field:      f,
				Descending: false,
			}
		}

		ydbType, _ := index.Get("ydbindex")

		v, _ = index.Get("unique")
		unique, _ := v.(bool)

		res[i] = IndexInfo{
			Name:    must.NotFail(index.Get("name")).(string),
			YDBType: YDBIndexType(ydbType.(string)),
			Key:     key,
			Unique:  unique,
		}
	}

	*s = res

	return nil
}
