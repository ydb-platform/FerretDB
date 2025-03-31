package query

import (
	"context"
	"github.com/FerretDB/FerretDB/internal/handler/sjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/iterator"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/FerretDB/FerretDB/internal/util/resource"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"sync"
)

// queryIterator implements iterator.Interface to fetch documents from the database.
type queryIterator struct {
	// the order of fields is weird to make the struct smaller due to alignment
	ctx           context.Context
	rs            result.BaseResult
	token         *resource.Token
	m             sync.Mutex
	onlyRecordIDs bool
}

// NewQueryIterator returns a new queryIterator for the given Rows.
//
// Iterator's Close method closes rows.
// They are also closed by the Next method on any error, including context cancellation,
// to make sure that the database connection is released as early as possible.
// In that case, the iterator's Close method should still be called.
//
// Nil rows are possible and return already done iterator.
// It still should be Closed.
func NewQueryIterator(ctx context.Context, rs result.BaseResult, onlyRecordIDs bool) types.DocumentsIterator {
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

// check interfaces
var (
	_ types.DocumentsIterator = (*queryIterator)(nil)
)
