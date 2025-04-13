package ydb

import (
	"context"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata/transaction"
	"github.com/FerretDB/FerretDB/internal/handler/sjson"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"sort"
	"strings"
	"text/template"
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

// Query implements backends.Collection interface.
func (c *collection) Query(ctx context.Context, params *backends.QueryParams) (*backends.QueryResult, error) {
	ydbPath := c.r.DbMapping[c.dbName]

	p, err := c.r.DatabaseGetExisting(ctx, c.dbName)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if p == nil {
		return &backends.QueryResult{
			Iter: NewQueryIterator(ctx, nil, params.OnlyRecordIDs),
		}, nil
	}

	meta, err := c.r.CollectionGet(ctx, c.dbName, c.name)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if meta == nil {
		return &backends.QueryResult{
			Iter: NewQueryIterator(ctx, nil, params.OnlyRecordIDs),
		}, nil
	}

	q := PrepareSelectClause(&SelectParams{
		Schema:        c.dbName,
		Table:         meta.TableName,
		Comment:       params.Comment,
		Capped:        meta.Capped(),
		OnlyRecordIDs: params.OnlyRecordIDs,
	})

	where, args, err := prepareWhereClause(params.Filter)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	var declareParts []string
	for _, param := range *args {
		paramName := param.Name()
		paramValue := param.Value()
		declareParts = append(declareParts, fmt.Sprintf("DECLARE %s AS %s;", paramName, paramValue.Type().String()))
	}

	if len(*args) > 0 {
		q += where
	}

	if len(declareParts) > 0 {
		q = fmt.Sprintf("PRAGMA TablePathPrefix(\"%s\");\n%s\n%s", ydbPath, strings.Join(declareParts, "\n"), q)
	}

	q = fmt.Sprintf("PRAGMA TablePathPrefix(\"%s\");\n%s", ydbPath, q)

	sort := prepareOrderByClause(params.Sort)
	q += sort

	if params.Limit != 0 {
		q += fmt.Sprintf(` LIMIT %s`, params.Limit)
	}

	var res result.Result
	err = c.r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			if err = ctx.Err(); err != nil {
				return err
			}

			_, res, err = session.Execute(ctx, transaction.ReadTx, q, args)
			if err != nil {
				return err
			}

			err = res.NextResultSetErr(ctx)
			if err != nil {
				return err
			}

			return res.Err()
		},
		table.WithIdempotent(),
	)

	return &backends.QueryResult{
		Iter: NewQueryIterator(ctx, res, params.OnlyRecordIDs),
	}, nil
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

	opts := &SelectParams{
		Schema: c.dbName,
		Table:  meta.TableName,
		Capped: meta.Capped(),
	}

	var plan string

	ydbPath := c.r.DbMapping[c.dbName]

	q := PrepareSelectClause(opts)
	q = fmt.Sprintf("PRAGMA TablePathPrefix(\"%s\");\n%s", ydbPath, q)

	err = c.r.D.Driver.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			explanation, err := s.Explain(ctx, q)
			if err != nil {
				return err
			}

			plan, _ = explanation.Plan, explanation.AST

			return nil
		},
		table.WithIdempotent(),
	)
	if err != nil {
		panic(err)
	}

	queryPlan, err := UnmarshalExplain(plan)
	res.QueryPlanner = queryPlan

	return res, nil
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

	ydbPath := c.r.DbMapping[c.dbName]
	batchSize := c.r.BatchSize
	if batchSize < 1 {
		panic("batch-size should be greater or equal to 1")
	}

	docs := params.Docs
	var allCollectionData []ydbTypes.Value
	indexes := meta.Indexes

	for len(docs) > 0 {
		i := min(batchSize, len(docs))
		batch, rest := docs[:i], docs[i:]

		for _, doc := range batch {
			var b []byte
			if b, err = sjson.Marshal(doc); err != nil {
				return nil, lazyerrors.Error(err)
			}

			id := GetId(doc)
			extraFields := extractIndexFields(doc, indexes)
			allCollectionData = append(allCollectionData, Data(id, b, extraFields))
		}

		docs = rest
	}

	q := buildUpsertQuery(ydbPath, meta.TableName, indexes)

	err = c.r.D.Driver.Table().DoTx(
		ctx,
		func(ctx context.Context, tx table.TransactionActor) (err error) {

			res, err := tx.Execute(ctx, q, table.NewQueryParameters(
				table.ValueParam("$insertData", ydbTypes.ListValue(allCollectionData...))),
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
		return nil, lazyerrors.Error(err)
	}

	return new(backends.InsertAllResult), nil
}

// DeleteAll implements backends.Collection interface.
func (c *collection) DeleteAll(ctx context.Context, params *backends.DeleteAllParams) (*backends.DeleteAllResult, error) {
	p, err := c.r.DatabaseGetExisting(ctx, c.dbName)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if p == nil {
		return &backends.DeleteAllResult{Deleted: 0}, nil
	}

	meta, err := c.r.CollectionGet(ctx, c.dbName, c.name)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if meta == nil {
		return &backends.DeleteAllResult{Deleted: 0}, nil
	}

	var IDs []ydbTypes.Value
	for _, id := range params.IDs {
		IDs = append(IDs, ydbTypes.BytesValueFromString(getIdFromAny(id)))
	}

	ydbPath := c.r.DbMapping[c.dbName]

	var (
		res          result.Result
		deletedCount uint64
	)

	err = c.r.D.Driver.Table().DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) (err error) {

			q := template.Must(template.New("delete").Parse(metadata.DeleteTemplate))

			res, err = tx.Execute(ctx, Render(q, TemplateConfig{
				TablePathPrefix: ydbPath,
				TableName:       meta.TableName,
			}), table.NewQueryParameters(
				table.ValueParam("$IDs", ydbTypes.ListValue(IDs...)),
			))
			if err != nil {
				return err
			}
			if err = res.Err(); err != nil {
				return err
			}

			err = res.NextResultSetErr(ctx)
			if err != nil {
				return lazyerrors.Error(err)
			}

			for res.NextRow() {
				err = res.ScanNamed(
					named.OptionalWithDefault("deleted_count", &deletedCount),
				)
				if err != nil {
					return lazyerrors.Error(err)
				}
			}

			_ = res.Close()

			return nil
		}, table.WithIdempotent())

	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	return &backends.DeleteAllResult{Deleted: int32(deletedCount)}, nil
}

func (c *collection) UpdateAll(ctx context.Context, params *backends.UpdateAllParams) (*backends.UpdateAllResult, error) {
	p, err := c.r.DatabaseGetExisting(ctx, c.dbName)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	var updateAllResult backends.UpdateAllResult
	if p == nil {
		return &updateAllResult, nil
	}

	meta, err := c.r.CollectionGet(ctx, c.dbName, c.name)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if meta == nil {
		return &updateAllResult, nil
	}

	ydbPath := c.r.DbMapping[c.dbName]
	indexes := meta.Indexes

	var collectionData []ydbTypes.Value

	for _, doc := range params.Docs {
		var b []byte
		if b, err = sjson.Marshal(doc); err != nil {
			return nil, lazyerrors.Error(err)
		}

		id := GetId(doc)
		extraFields := extractIndexFields(doc, indexes)
		collectionData = append(collectionData, Data(id, b, extraFields))
	}

	q := buildUpsertQuery(ydbPath, meta.TableName, indexes)

	err = c.r.D.Driver.Table().DoTx(
		ctx,
		func(ctx context.Context, tx table.TransactionActor) (err error) {

			res, err := tx.Execute(ctx, q, table.NewQueryParameters(
				table.ValueParam("$insertData", ydbTypes.ListValue(collectionData...))),
			)
			if err != nil {
				return err
			}
			if err = res.Err(); err != nil {
				return err
			}

			updateAllResult.Updated = int32(res.ResultSetCount())

			return res.Close()
		}, table.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}

	return &updateAllResult, nil
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
			`, dbName, coll.TableName)

		err := driver.Table().Do(ctx,
			func(ctx context.Context, s table.Session) (err error) {
				_, res, err := s.Execute(ctx, transaction.ReadTx, statsQuery, table.NewQueryParameters())
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
			fmt.Printf("Failed to fill table with insertData: %v\n", err)
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
