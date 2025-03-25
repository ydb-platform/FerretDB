package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/util/resource"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"hash/fnv"
	"log/slog"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/handler/sjson"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/FerretDB/FerretDB/internal/util/state"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	// YDB table name where FerretDB metadata is stored.
	metadataTableName = backends.ReservedPrefix + "database_metadata"
)

// Parts of Prometheus metric names.
const (
	namespace = "ferretdb"
	subsystem = "ydb_metadata"
)

type Registry struct {
	D         *DB
	l         *slog.Logger
	BatchSize int
	rw        sync.RWMutex
	colls     map[string]map[string]*Collection // database name -> collection name -> collection
}

// NewRegistry creates a registry for YDB databases with a given base URI.
func NewRegistry(dsn string, batchSize int, l *slog.Logger, sp *state.Provider) (*Registry, error) {
	db, err := New(dsn, l, sp)
	if err != nil {
		return nil, err
	}

	r := &Registry{
		D:         db,
		l:         l,
		BatchSize: batchSize,
	}

	return r, nil
}

// Close closes the registry.
func (r *Registry) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.D.Driver.Close(ctx)

	resource.Untrack(r.D, r.D.token)
}

// LoadMetadata loads collections metadata from the database during initialization
// if it hasn't been loaded from the database yet.
//
// It acquires read lock to check metadata, if metadata is empty it acquires write lock
// to load metadata, so it is safe for concurrent use.
//
// All methods should use this method to check authentication and load metadata.
func (r *Registry) LoadMetadata(ctx context.Context, dbName string) error {
	r.rw.RLock()
	if r.colls != nil {
		r.rw.RUnlock()
		return nil
	}
	r.rw.RUnlock()

	r.colls = make(map[string]map[string]*Collection)

	dbName = "/local"
	absTablePath := path.Join(dbName, metadataTableName)
	exists, err := sugar.IsTableExists(ctx, r.D.Driver.Scheme(), absTablePath)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			var (
				res      result.Result
				jsonData string
			)

			query := fmt.Sprintf(
				`PRAGMA TablePathPrefix("%v");

						SELECT %s FROM %s`,
				dbName,
				DefaultColumn,
				metadataTableName,
			)

			_, res, err = s.Execute(
				ctx,
				readTx,
				query,
				table.NewQueryParameters(),
			)

			if err != nil {
				return lazyerrors.Error(err)
			}
			defer res.Close()

			colls := map[string]*Collection{}

			for res.NextResultSet(ctx) {
				for res.NextRow() {
					err = res.ScanNamed(
						named.OptionalWithDefault(DefaultColumn, &jsonData),
					)
					if err != nil {
						return lazyerrors.Error(err)
					}

					var c Collection
					if err := json.Unmarshal([]byte(jsonData), &c); err != nil {
						return lazyerrors.Error(err)
					}

					colls[c.Name] = &c
				}
			}

			if err = res.Err(); err != nil {
				return lazyerrors.Error(err)
			}

			r.colls[dbName] = colls

			return nil
		})

	return err
}

// DatabaseList returns a sorted list of existing databases.
//
// If the user is not authenticated, it returns error.
func (r *Registry) DatabaseList(ctx context.Context) ([]string, error) {
	r.rw.RLock()
	defer r.rw.RUnlock()

	res := maps.Keys(r.colls)
	sort.Strings(res)

	return res, nil
}

// DatabaseGetExisting returns a connection to existing database or nil if it doesn't exist.
//
// If the user is not authenticated, it returns error.
func (r *Registry) DatabaseGetExisting(ctx context.Context, dbName string) (map[string]*Collection, error) {
	r.rw.RLock()
	defer r.rw.RUnlock()

	db := r.colls[dbName]
	if db == nil {
		return nil, nil
	}

	return db, nil
}

// DatabaseGetOrCreate returns a connection to existing database or newly created database.
//
// The dbName must be a validated database name.
//
// If the user is not authenticated, it returns error.
func (r *Registry) DatabaseGetOrCreate(ctx context.Context, dbName string) error {
	if err := r.LoadMetadata(ctx, dbName); err != nil {
		return lazyerrors.Error(err)
	}

	r.rw.Lock()
	defer r.rw.Unlock()

	return r.databaseGetOrCreate(ctx, dbName)
}

// databaseGet returns a connection to existing database
//
// The dbName must be a validated database name.
//
// It does not hold the lock.
func (r *Registry) databaseGetOrCreate(ctx context.Context, dbName string) error {
	db := r.colls[dbName]
	if db != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(dbName, metadataTableName),
				options.WithColumn("meta_id", types.TypeUUID),
				options.WithColumn(DefaultColumn, types.Optional(types.TypeJSONDocument)),
				options.WithPrimaryKeyColumn("meta_id"),
			)
		})

	if err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
	}

	r.colls[dbName] = map[string]*Collection{}

	return nil
}

// CollectionCreateParams contains parameters for CollectionCreate.
type CollectionCreateParams struct {
	DBName          string
	Name            string
	CappedSize      int64
	CappedDocuments int64
	_               struct{} // prevent unkeyed literals
}

// Capped returns true if capped collection creation is requested.
func (ccp *CollectionCreateParams) Capped() bool {
	return ccp.CappedSize > 0
}

// CollectionCreate creates a collection in the database.
//
// Returned boolean value indicates whether the collection was created.
// If collection already exists, (false, nil) is returned.
//
// If the user is not authenticated, it returns error.
func (r *Registry) CollectionCreate(ctx context.Context, params *CollectionCreateParams) (bool, error) {
	if err := r.LoadMetadata(ctx, params.DBName); err != nil {
		return false, lazyerrors.Error(err)
	}

	r.rw.Lock()
	defer r.rw.Unlock()

	return r.collectionCreate(ctx, params)
}

// collectionCreate creates a collection in the database.

// Returned boolean value indicates whether the collection was created.
// If collection already exists, (false, nil) is returned.
//
// It does not hold the lock.
func (r *Registry) collectionCreate(ctx context.Context, params *CollectionCreateParams) (bool, error) {
	dbName, collectionName := params.DBName, params.Name

	dbName = "/local"
	err := r.databaseGetOrCreate(ctx, dbName)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	colls := r.colls[dbName]
	if colls != nil && colls[collectionName] != nil {
		return false, nil
	}

	id := uuid.New()
	tableName := collectionName
	c := &Collection{
		Name:      collectionName,
		UUID:      id.String(),
		TableName: tableName,
		Settings: Settings{
			CappedSize:      params.CappedSize,
			CappedDocuments: params.CappedDocuments,
		},
	}

	jsonData, err := json.Marshal(c)
	if err != nil {
		return false, fmt.Errorf("failed to marshal collection data: %v", err)
	}

	columns := []options.CreateTableOption{
		options.WithColumn(DefaultColumn, types.Optional(types.TypeJSONDocument)),
	}

	var primaryKey string

	if params.Capped() {
		columns = append(columns, options.WithColumn(RecordIDColumn, types.TypeUUID))
		primaryKey = RecordIDColumn
	}
	columns = append(columns, options.WithColumn("id", types.TypeString))
	primaryKey = "id"

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(dbName, tableName),
				append(columns, options.WithPrimaryKeyColumn(primaryKey))...,
			)
		})

	if err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
	}

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $meta_id as Uuid;
		DECLARE $json as JsonDocument;

		REPLACE INTO %s (meta_id, _jsonb)
		VALUES ($meta_id, $json);
		`, dbName, metadataTableName)

	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query, table.NewQueryParameters(
				table.ValueParam("$meta_id", types.UuidValue(id)),
				table.ValueParam("$json", types.JSONDocumentValueFromBytes(jsonData)),
			))

			return err
		})

	if err != nil {
		fmt.Printf("Failed to fill table with data: %v\n", err)
	}

	if r.colls[dbName] == nil {
		r.colls[dbName] = map[string]*Collection{}
	}
	r.colls[dbName][collectionName] = c

	return true, nil
}

// CollectionGet returns a copy of collection metadata.
// It can be safely modified by a caller.
//
// If database or collection does not exist, nil is returned.
//
// If the user is not authenticated, it returns error.
func (r *Registry) CollectionGet(ctx context.Context, dbName, collectionName string) (*Collection, error) {
	r.rw.RLock()
	defer r.rw.RUnlock()

	return r.collectionGet(dbName, collectionName), nil
}

// collectionGet returns a copy of collection metadata.
// It can be safely modified by a caller.
//
// If database or collection does not exist, nil is returned.
//
// It does not hold the lock.
func (r *Registry) collectionGet(dbName, collectionName string) *Collection {
	colls := r.colls[dbName]
	if colls == nil {
		return nil
	}

	return colls[collectionName].deepCopy()
}

// CollectionList returns a sorted copy of collections in the database.
//
// If database does not exist, no error is returned.
//
// If the user is not authenticated, it returns error.
func (r *Registry) CollectionList(ctx context.Context, dbName string) ([]*Collection, error) {
	if err := r.LoadMetadata(ctx, dbName); err != nil {
		return nil, lazyerrors.Error(err)
	}

	r.rw.RLock()
	defer r.rw.RUnlock()

	db := r.colls[dbName]
	if db == nil {
		return nil, nil
	}

	res := make([]*Collection, 0, len(r.colls[dbName]))
	for _, c := range r.colls[dbName] {
		res = append(res, c.deepCopy())
	}

	sort.Slice(res, func(i, j int) bool { return res[i].Name < res[j].Name })

	return res, nil
}

// CollectionDrop drops a collection in the database.
//
// Returned boolean value indicates whether the collection was dropped.
// If database or collection did not exist, (false, nil) is returned.
//
// If the user is not authenticated, it returns error.
func (r *Registry) CollectionDrop(ctx context.Context, dbName, collectionName string) (bool, error) {
	if err := r.LoadMetadata(ctx, dbName); err != nil {
		return false, lazyerrors.Error(err)
	}

	r.rw.Lock()
	defer r.rw.Unlock()

	return r.collectionDrop(ctx, dbName, collectionName)
}

// collectionDrop drops a collection in the database.
//
// Returned boolean value indicates whether the collection was dropped.
// If database or collection did not exist, (false, nil) is returned.
//
// It does not hold the lock.
func (r *Registry) collectionDrop(ctx context.Context, dbName, collectionName string) (bool, error) {
	db := r.colls[dbName]
	if db == nil {
		return false, nil
	}

	c := r.collectionGet(dbName, collectionName)
	if c == nil {
		return false, nil
	}

	id := uuid.MustParse(c.UUID)
	err := r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.DropTable(ctx, path.Join(dbName, c.TableName))
		})

	if err != nil {
		fmt.Printf("Failed to drop table: %v\n", err)
		return false, err
	}

	q := fmt.Sprintf(
		`PRAGMA TablePathPrefix("%v");

		DECLARE $meta_id AS Uuid;

		DELETE FROM %s WHERE meta_id=$meta_id`,

		dbName,
		metadataTableName,
	)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			_, _, err := s.Execute(
				ctx,
				writeTx,
				q,
				table.NewQueryParameters(table.ValueParam("$meta_id", types.UuidValue(id))))

			if err != nil {
				return err
			}
			return err
		})

	if err != nil {
		fmt.Printf("Failed to delete info from metadata table: %v\n", err)
		return false, err
	}

	delete(r.colls[dbName], collectionName)

	return true, nil
}

// CollectionRename renames a collection in the database.
//
// The collection name is updated, but original table name is kept.
//
// Returned boolean value indicates whether the collection was renamed.
// If database or collection did not exist, (false, nil) is returned.
//
// If the user is not authenticated, it returns error.
func (r *Registry) CollectionRename(ctx context.Context, dbName, oldCollectionName, newCollectionName string) (bool, error) {
	if err := r.LoadMetadata(ctx, dbName); err != nil {
		return false, lazyerrors.Error(err)
	}

	r.rw.Lock()
	defer r.rw.Unlock()

	db := r.colls[dbName]
	if db == nil {
		return false, nil
	}

	c := r.collectionGet(dbName, oldCollectionName)
	if c == nil {
		return false, nil
	}

	c.Name = newCollectionName

	b, err := json.Marshal(c)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	q := fmt.Sprintf(
		`PRAGMA TablePathPrefix("%v");
				DECLARE $meta_id AS Uuid;
				DECLARE $json AS JsonDocument;

				REPLACE INTO %s (meta_id, _jsonb) VALUES ($meta_id, $json);`,

		dbName,
		metadataTableName,
	)
	id := uuid.MustParse(c.UUID)

	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				writeTx,
				q,
				table.NewQueryParameters(
					table.ValueParam("$meta_id", types.UuidValue(id)),
					table.ValueParam("$json", types.JSONDocumentValueFromBytes(b)),
				))
			return err
		})

	if err != nil {
		return false, lazyerrors.Error(err)
	}

	r.colls[dbName][newCollectionName] = c
	delete(r.colls[dbName], oldCollectionName)

	return true, nil
}

// IndexesCreate creates indexes in the collection.
//
// Existing indexes with given names are ignored.
//
// If the user is not authenticated, it returns error.
func (r *Registry) IndexesCreate(ctx context.Context, dbName, collectionName string, indexes []IndexInfo) error {
	r.rw.Lock()
	defer r.rw.Unlock()

	return r.indexesCreate(ctx, dbName, collectionName, indexes)
}

// indexesCreate creates indexes in the collection.
//
// Existing indexes with given names are ignored.
//
// It does not hold the lock.
func (r *Registry) indexesCreate(ctx context.Context, dbName, collectionName string, indexes []IndexInfo) error {
	db := r.colls[dbName]
	if db == nil {
		panic("database does not exist")
	}

	c := r.collectionGet(dbName, collectionName)
	if c == nil {
		panic("collection does not exist")
	}

	allIndexes := make(map[string]string, len(db))

	for _, coll := range db {
		for _, index := range coll.Indexes {
			allIndexes[index.Name] = coll.Name
		}
	}

	created := make([]string, 0, len(indexes))

	for _, index := range indexes {
		if coll, ok := allIndexes[index.Name]; ok && coll == collectionName {
			continue
		}

		h := fnv.New32a()
		must.NotFail(h.Write([]byte(index.Name)))

		q := "CREATE "

		q += "INDEX %s ON %s (%s)"

		columns := make([]string, len(index.Key))

		for i, key := range index.Key {
			fs := strings.Split(key.Field, ".")
			transformedParts := make([]string, len(fs))

			for j, f := range fs {
				transformedParts[j] = f
			}

			columns[i] = fmt.Sprintf("((%s->%s))", DefaultColumn, strings.Join(transformedParts, " -> "))
			if key.Descending {
				columns[i] += " DESC"
			}
		}

		q = fmt.Sprintf(
			q,
			index.YDBType,
			dbName,
			c.TableName,
			strings.Join(columns, ", "),
		)

		created = append(created, index.Name)
		c.Indexes = append(c.Indexes, index)
		allIndexes[index.Name] = collectionName
	}

	_, err := sjson.Marshal(c.marshal())
	if err != nil {
		return lazyerrors.Error(err)
	}

	r.colls[dbName][collectionName] = c

	return nil
}

// IndexesDrop removes given connection's indexes.
//
// Non-existing indexes are ignored.
//
// If database or collection does not exist, nil is returned.
//
// If the user is not authenticated, it returns error.
func (r *Registry) IndexesDrop(ctx context.Context, dbName, collectionName string, indexNames []string) error {
	r.rw.Lock()
	defer r.rw.Unlock()

	return r.indexesDrop(ctx, dbName, collectionName, indexNames)
}

// indexesDrop removes given connection's indexes.
//
// Non-existing indexes are ignored.
//
// If database or collection does not exist, nil is returned.
//
// It does not hold the lock.
func (r *Registry) indexesDrop(ctx context.Context, dbName, collectionName string, indexNames []string) error {
	c := r.collectionGet(dbName, collectionName)
	if c == nil {
		return nil
	}

	for _, name := range indexNames {
		i := slices.IndexFunc(c.Indexes, func(i IndexInfo) bool { return name == i.Name })
		if i < 0 {
			continue
		}

		c.Indexes = slices.Delete(c.Indexes, i, i+1)
	}

	r.colls[dbName][collectionName] = c

	return nil
}

// Describe implements prometheus.Collector.
func (r *Registry) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(r, ch)
}

// Collect implements prometheus.Collector.
func (r *Registry) Collect(ch chan<- prometheus.Metric) {

	r.rw.RLock()
	defer r.rw.RUnlock()

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "databases"),
			"The current number of database in the registry.",
			nil, nil,
		),
		prometheus.GaugeValue,
		float64(len(r.colls)),
	)

	for db, colls := range r.colls {
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(
				prometheus.BuildFQName(namespace, subsystem, "collections"),
				"The current number of collections in the registry.",
				[]string{"db"}, nil,
			),
			prometheus.GaugeValue,
			float64(len(colls)),
			db,
		)
	}
}

// check interfaces
var (
	_ prometheus.Collector = (*Registry)(nil)
)
