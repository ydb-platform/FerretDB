package ydb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/exp/maps"
	"sort"
	"strings"
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
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type selectParams struct {
	Schema  string
	Table   string
	Comment string

	Capped        bool
	OnlyRecordIDs bool
}

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
	p, err := c.r.DatabaseGetExisting(ctx, c.dbName)
	if err != nil {
		return &backends.ExplainResult{
			QueryPlanner: must.NotFail(types.NewDocument()),
		}, nil
	}

	if p == nil {
		return &backends.ExplainResult{
			QueryPlanner: must.NotFail(types.NewDocument()),
		}, nil
	}

	meta, err := c.r.CollectionGet(ctx, c.dbName, c.name)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if meta == nil {
		return &backends.ExplainResult{
			QueryPlanner: must.NotFail(types.NewDocument()),
		}, nil
	}

	res := new(backends.ExplainResult)

	opts := &selectParams{
		Schema: c.dbName,
		Table:  meta.TableName,
		Capped: meta.Capped(),
	}

	var (
		plan string
		ast  string
	)

	q := prepareSelectClause(opts)

	err = c.r.D.Driver.Table().Do( // Do retry operation on errors with best effort
		ctx, // context manage exiting from Do
		func(ctx context.Context, s table.Session) (err error) { // retry operation
			explanation, err := s.Explain(ctx, q)
			if err != nil {
				return err // for auto-retry with driver
			}

			plan, ast = explanation.Plan, explanation.AST

			return nil
		},
		table.WithIdempotent(),
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Plan: %s\n", plan)
	fmt.Printf("AST: %s\n", ast)

	queryPlan, err := unmarshalExplainFromString(plan)
	res.QueryPlanner = queryPlan

	return res, nil
}

func unmarshalExplainFromString(explainStr string) (*types.Document, error) {
	explainBytes := []byte(explainStr)

	return unmarshalExplain(explainBytes)
}

func unmarshalExplain(b []byte) (*types.Document, error) {
	var plans []map[string]any
	if err := json.Unmarshal(b, &plans); err != nil {
		return nil, lazyerrors.Error(err)
	}

	if len(plans) == 0 {
		return nil, lazyerrors.Error(errors.New("no execution plan returned"))
	}

	return convertJSON(plans[0]).(*types.Document), nil
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

func prepareSelectClause(params *selectParams) string {
	return fmt.Sprintf(
		`
				DECLARE $id AS Optional<Uint64>;
				DECLARE $myStr AS Optional<Text>;
				SELECT * FROM %s;`, params.Table)

}

// InsertAll implements backends.Collection interface.
func (c *collection) InsertAll(ctx context.Context, params *backends.InsertAllParams) (*backends.InsertAllResult, error) {
	if _, err := c.r.CollectionCreate(ctx, &metadata.CollectionCreateParams{
		DBName: c.dbName,
		Name:   c.name,
	}); err != nil {
		return nil, lazyerrors.Error(err)
	}

	meta, err := c.r.CollectionGet(ctx, c.dbName, c.name)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	err = c.r.D.Driver.Table().DoTx(
		ctx,
		func(ctx context.Context, tx table.TransactionActor) (err error) {

			batchSize := c.r.BatchSize
			if batchSize < 1 {
				panic("batch-size should be greater or equal to 1")
			}

			var batch []*types.Document
			docs := params.Docs

			var values []string
			var paramss []table.ParameterOption
			for len(docs) > 0 {
				i := min(batchSize, len(docs))
				batch, docs = docs[:i], docs[i:]

				for i, doc := range batch {

					oid, _ := GetObjectID(doc)
					oidStr := oid.Hex()

					b, err := sjson.Marshal(doc)
					if err != nil {
						panic(err)
					}

					idParam := fmt.Sprintf("$id%d", i)
					jsonbParam := fmt.Sprintf("$jsonb%d", i)

					values = append(values, fmt.Sprintf("(%s, %s)", idParam, jsonbParam))
					paramss = append(paramss,
						table.ValueParam(fmt.Sprintf("$id%d", i), ydbTypes.BytesValue([]byte(oidStr))),
						table.ValueParam(fmt.Sprintf("$jsonb%d", i), ydbTypes.JSONDocumentValueFromBytes(b)),
					)
				}
			}

			query := fmt.Sprintf(`
            PRAGMA TablePathPrefix("/local");
            UPSERT INTO %s (id, _jsonb) VALUES %s;
        `, meta.TableName, strings.Join(values, ", "))

			res, err := tx.Execute(ctx, query,
				table.NewQueryParameters(paramss...),
			)
			if err != nil {
				return err
			}
			if err = res.Err(); err != nil {
				return err
			}
			return res.Close()
		}, table.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}

	return new(backends.InsertAllResult), nil
}

func GetObjectID(d *types.Document) (primitive.ObjectID, error) {
	if d == nil {
		return primitive.NilObjectID, fmt.Errorf("document is nil")
	}

	value, err := d.Get("_id")
	if err != nil {
		return primitive.NilObjectID, fmt.Errorf("_id not found")
	}

	oid, ok := value.(primitive.ObjectID)
	if !ok {
		return primitive.NilObjectID, fmt.Errorf("invalid _id type: %T", value)
	}

	return oid, nil
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
	p, err := c.r.DatabaseGetExisting(ctx, c.dbName)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if p == nil {
		return nil, backends.NewError(backends.ErrorCodeDatabaseDoesNotExist, lazyerrors.Errorf("no database %s", c.dbName))
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

	stats, err := collectionsStats(ctx, c.r.D.Driver, c.dbName, coll, params.Refresh)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	indexMap := map[string]string{}
	indexSizes := make([]backends.IndexSize, len(indexMap))
	indexSizes = []backends.IndexSize{
		{
			Name: "indexName",
			Size: 0,
		},
	}

	return &backends.CollectionStatsResult{
		CountDocuments:  stats.countDocuments,
		SizeTotal:       stats.sizeTables,
		SizeIndexes:     stats.sizeIndexes,
		SizeCollection:  stats.sizeTables,
		SizeFreeStorage: stats.sizeFreeStorage,
		IndexSizes:      indexSizes,
	}, nil
}

func collectionsStats(ctx context.Context, driver *ydb.Driver, dbName string, coll *metadata.Collection, refresh bool) (*stats, error) {
	var sizeTables, countDocuments int64
	if refresh {
		statsQuery := fmt.Sprintf(`
				PRAGMA TablePathPrefix("%v");
				ANALYZE %s;
	`, "/local", coll.TableName)

		readTx := table.TxControl(
			table.BeginTx(
				table.WithOnlineReadOnly(),
			),
			table.CommitTx(),
		)

		err := driver.Table().Do(ctx,
			func(ctx context.Context, s table.Session) (err error) {
				_, res, err := s.Execute(ctx, readTx, statsQuery, table.NewQueryParameters())
				if err != nil {
					return err
				}

				defer func() {
					_ = res.Close()
				}()

				if !res.NextResultSet(ctx) || !res.NextRow() {
					return fmt.Errorf("failed to get collection stats")
				}

				if err = res.Scan(&sizeTables, &countDocuments); err != nil {
					return err
				}
				return err
			})

		if err != nil {
			fmt.Printf("Failed to fill table with data: %v\n", err)
		}
	}

	var sizeIndexes int64 = 0
	var sizeFreeStorage int64 = 0

	return &stats{
		countDocuments:  countDocuments,
		sizeIndexes:     sizeIndexes,
		sizeTables:      sizeTables,
		sizeFreeStorage: sizeFreeStorage,
	}, nil
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

// stats represents information about statistics of tables and indexes.
type stats struct {
	countDocuments  int64
	sizeIndexes     int64
	sizeTables      int64
	sizeFreeStorage int64
}
