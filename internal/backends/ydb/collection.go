package ydb

import (
	"context"
	"sort"
	"sync"

	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/handler/sjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/iterator"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/FerretDB/FerretDB/internal/util/resource"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

// collection implements backends.Collection interface.
type collection struct {
	r      *metadata.Registry
	dbName string
	name   string
}

// newCollection creates a new Collection.
func newCollection(r *metadata.Registry, dbName, name string) backends.Collection {
	return backends.CollectionContract(&collection{
		r:      r,
		dbName: dbName,
		name:   name,
	})
}

func (c *collection) Explain(ctx context.Context, params *backends.ExplainParams) (*backends.ExplainResult, error) {
	return nil, nil
}

// InsertAll implements backends.Collection interface.
func (c *collection) InsertAll(ctx context.Context, params *backends.InsertAllParams) (*backends.InsertAllResult, error) {
	return nil, nil
}

// Query implements backends.Collection interface.
func (c *collection) Query(ctx context.Context, params *backends.QueryParams) (*backends.QueryResult, error) {
	p, err := c.r.DatabaseGetExisting(ctx, c.dbName)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if p == nil {
		return &backends.QueryResult{
			Iter: newQueryIterator(ctx, nil, params.OnlyRecordIDs),
		}, nil
	}

	meta, err := c.r.CollectionGet(ctx, c.dbName, c.name)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if meta == nil {
		return &backends.QueryResult{
			Iter: newQueryIterator(ctx, nil, params.OnlyRecordIDs),
		}, nil
	}

	return nil, nil
}

// queryIterator implements iterator.Interface to fetch documents from the database.
type queryIterator struct {
	// the order of fields is weird to make the struct smaller due to alignment
	ctx           context.Context
	rs            result.BaseResult
	token         *resource.Token
	m             sync.Mutex
	onlyRecordIDs bool
}

// newQueryIterator returns a new queryIterator for the given Rows.
//
// Iterator's Close method closes rows.
// They are also closed by the Next method on any error, including context cancellation,
// to make sure that the database connection is released as early as possible.
// In that case, the iterator's Close method should still be called.
//
// Nil rows are possible and return already done iterator.
// It still should be Closed.
func newQueryIterator(ctx context.Context, rs result.BaseResult, onlyRecordIDs bool) types.DocumentsIterator {
	iter := &queryIterator{
		ctx:           ctx,
		rs:            rs,
		onlyRecordIDs: onlyRecordIDs,
		token:         resource.NewToken(),
	}
	resource.Track(iter, iter.token)

	return iter
}

// Next implements iterator.Interface.
func (iter *queryIterator) Next() (struct{}, *types.Document, error) {
	iter.m.Lock()
	defer iter.m.Unlock()

	var unused struct{}

	// ignore context error, if any, if iterator is already closed
	if iter.rs == nil {
		return unused, nil, iterator.ErrIteratorDone
	}

	if err := context.Cause(iter.ctx); err != nil {
		iter.close()
		return unused, nil, lazyerrors.Error(err)
	}

	if !iter.rs.NextRow() {
		err := iter.rs.Err()

		iter.close()

		if err == nil {
			err = iterator.ErrIteratorDone
		}

		return unused, nil, lazyerrors.Error(err)
	}

	var recordID int64
	var b []byte

	if iter.rs.HasNextRow() {
		if err := iter.rs.ScanWithDefaults(&recordID, &b); err != nil {
			iter.close()
			return unused, nil, lazyerrors.Error(err)
		}
	}

	var err error
	doc := must.NotFail(types.NewDocument())

	if !iter.onlyRecordIDs {
		if doc, err = sjson.Unmarshal(b); err != nil {
			iter.close()
			return unused, nil, lazyerrors.Error(err)
		}
	}

	doc.SetRecordID(recordID)

	return unused, doc, nil
}

// Close implements iterator.Interface.
func (iter *queryIterator) Close() {
	iter.m.Lock()
	defer iter.m.Unlock()

	iter.close()
}

// close closes iterator without holding mutex.
//
// This should be called only when the caller already holds the mutex.
func (iter *queryIterator) close() {

	resource.Untrack(iter, iter.token)
}

// DeleteAll implements backends.Collection interface.
func (c *collection) DeleteAll(ctx context.Context, params *backends.DeleteAllParams) (*backends.DeleteAllResult, error) {
	return &backends.DeleteAllResult{}, nil
}

func (c *collection) UpdateAll(ctx context.Context, params *backends.UpdateAllParams) (*backends.UpdateAllResult, error) {
	return nil, nil
}

// Stats implements backends.Collection interface.
func (c *collection) Stats(ctx context.Context, params *backends.CollectionStatsParams) (*backends.CollectionStatsResult, error) {
	return nil, nil
}

// ListIndexes implements backends.Collection interface.
func (c *collection) ListIndexes(ctx context.Context, params *backends.ListIndexesParams) (*backends.ListIndexesResult, error) {
	db, err := c.r.DatabaseGetExisting(ctx, c.dbName)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if db == nil {
		return nil, backends.NewError(
			backends.ErrorCodeCollectionDoesNotExist,
			lazyerrors.Errorf("no ns %s.%s", c.dbName, c.name),
		)
	}

	coll, err := c.r.CollectionGet(ctx, c.dbName, c.name)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	if coll == nil {
		return nil, backends.NewError(
			backends.ErrorCodeCollectionDoesNotExist,
			lazyerrors.Errorf("no ns %s.%s", c.dbName, c.name),
		)
	}

	res := backends.ListIndexesResult{
		Indexes: make([]backends.IndexInfo, len(coll.Indexes)),
	}

	for i, index := range coll.Indexes {
		res.Indexes[i] = backends.IndexInfo{
			Name:   index.Name,
			Unique: index.Unique,
			Key:    make([]backends.IndexKeyPair, len(index.Key)),
		}

		for j, key := range index.Key {
			res.Indexes[i].Key[j] = backends.IndexKeyPair{
				Field:      key.Field,
				Descending: key.Descending,
			}
		}
	}

	sort.Slice(res.Indexes, func(i, j int) bool {
		return res.Indexes[i].Name < res.Indexes[j].Name
	})

	return &res, nil
}

// CreateIndexes implements backends.Collection interface.
func (c *collection) CreateIndexes(ctx context.Context, params *backends.CreateIndexesParams) (*backends.CreateIndexesResult, error) { //nolint:lll // for readability
	indexes := make([]metadata.IndexInfo, len(params.Indexes))
	for i, index := range params.Indexes {
		indexes[i] = metadata.IndexInfo{
			Name:   index.Name,
			Key:    make([]metadata.IndexKeyPair, len(index.Key)),
			Unique: index.Unique,
		}

		for j, key := range index.Key {
			indexes[i].Key[j] = metadata.IndexKeyPair{
				Field:      key.Field,
				Descending: key.Descending,
			}
		}
	}

	err := c.r.IndexesCreate(ctx, c.dbName, c.name, indexes)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return new(backends.CreateIndexesResult), nil
}

// DropIndexes implements backends.Collection interface.
func (c *collection) DropIndexes(ctx context.Context, params *backends.DropIndexesParams) (*backends.DropIndexesResult, error) {
	err := c.r.IndexesDrop(ctx, c.dbName, c.name, params.Indexes)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return new(backends.DropIndexesResult), nil
}

// Compact implements backends.Collection interface.
func (c *collection) Compact(ctx context.Context, params *backends.CompactParams) (*backends.CompactResult, error) {
	return new(backends.CompactResult), nil
}

// check interfaces
var (
	_ backends.Collection = (*collection)(nil)
)
