package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata/transaction"
	"github.com/FerretDB/FerretDB/internal/util/resource"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"log/slog"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"

	"golang.org/x/exp/maps"

	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/state"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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

// Registry provides access to YDB database and collections information.
//
// Registry metadata is loaded upon first call by client, using [conninfo] in the context of the client.
//
//nolint:vet // for readability
type Registry struct {
	D         *DB
	l         *slog.Logger
	BatchSize int
	Rw        sync.RWMutex
	Colls     map[string]map[string]*Collection // database name -> collection name -> collection
	DbMapping map[string]string
}

// NewRegistry creates a registry for YDB database with a given base URI.
func NewRegistry(dsn string, batchSize int, l *slog.Logger, sp *state.Provider) (*Registry, error) {
	db, err := New(dsn, l, sp)
	if err != nil {
		return nil, err
	}

	r := &Registry{
		D:         db,
		l:         l,
		BatchSize: batchSize,
		DbMapping: make(map[string]string),
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
func (r *Registry) LoadMetadata(ctx context.Context, dbName string) (string, error) {
	ydbPath := r.DbMapping[dbName]

	r.Rw.RLock()
	if r.Colls != nil {
		r.Rw.RUnlock()
		return ydbPath, nil
	}
	r.Rw.RUnlock()

	absTablePath := path.Join(ydbPath, metadataTableName)
	exists, err := sugar.IsTableExists(ctx, r.D.Driver.Scheme(), absTablePath)
	if err != nil {
		return "", nil
	}

	if !exists {
		return "", nil
	}

	r.Colls = make(map[string]map[string]*Collection)

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			var (
				res      result.Result
				jsonData string
			)

			selectTemplate := `
				PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
				SELECT {{ .ColumnName }} FROM {{ .TableName }}
			`
			q := template.Must(template.New("select").Parse(selectTemplate))
			fmt.Println(q)

			_, res, err = s.Execute(
				ctx,
				transaction.ReadTx,
				render(q, templateConfig{
					TablePathPrefix: ydbPath,
					TableName:       metadataTableName,
					ColumnName:      DefaultColumn,
				}),
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

			r.Colls[dbName] = colls

			return nil
		})

	return ydbPath, err
}

// DatabaseList returns a sorted list of existing databases.
//
// If the user is not authenticated, it returns error.
func (r *Registry) DatabaseList(ctx context.Context, dbName string) ([]string, error) {
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return nil, lazyerrors.Error(err)
	}

	r.Rw.RLock()
	defer r.Rw.RUnlock()

	res := maps.Keys(r.Colls)
	sort.Strings(res)

	return res, nil
}

// DatabaseGetExisting returns a connection to existing database or nil if it doesn't exist.
//
// If the user is not authenticated, it returns error.
func (r *Registry) DatabaseGetExisting(ctx context.Context, dbName string) (map[string]*Collection, error) {
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return nil, lazyerrors.Error(err)
	}

	r.Rw.RLock()
	defer r.Rw.RUnlock()

	db := r.Colls[dbName]
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
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

	return r.databaseGetOrCreate(ctx, dbName)
}

// databaseGet returns a connection to existing database
//
// The dbName must be a validated database name.
//
// It does not hold the lock.
func (r *Registry) databaseGetOrCreate(ctx context.Context, dbName string) error {
	db := r.Colls[dbName]
	if db != nil {
		return nil
	}

	ydbPath := r.DbMapping[dbName]

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(ydbPath, metadataTableName),
				options.WithColumn("meta_id", ydbTypes.TypeUUID),
				options.WithColumn(DefaultColumn, ydbTypes.Optional(ydbTypes.TypeJSONDocument)),
				options.WithPrimaryKeyColumn("meta_id"),
			)
		})

	if err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
	}

	if r.Colls == nil {
		r.Colls = make(map[string]map[string]*Collection)
	}

	r.Colls[dbName] = map[string]*Collection{}

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
	if _, err := r.LoadMetadata(ctx, params.DBName); err != nil {
		return false, lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

	return r.collectionCreate(ctx, params)
}

// collectionCreate creates a collection in the database.

// Returned boolean value indicates whether the collection was created.
// If collection already exists, (false, nil) is returned.
//
// It does not hold the lock.
func (r *Registry) collectionCreate(ctx context.Context, params *CollectionCreateParams) (bool, error) {
	collectionName := params.Name
	ydbPath := r.DbMapping[params.DBName]

	err := r.databaseGetOrCreate(ctx, params.DBName)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	colls := r.Colls[params.DBName]
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
		options.WithColumn(DefaultColumn, ydbTypes.Optional(ydbTypes.TypeJSONDocument)),
	}

	var primaryKey string

	if params.Capped() {
		columns = append(columns, options.WithColumn(RecordIDColumn, ydbTypes.TypeUUID))
		primaryKey = RecordIDColumn
	}
	columns = append(columns, options.WithColumn("id", ydbTypes.TypeString))
	primaryKey = "id"

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(ydbPath, tableName),
				append(columns, options.WithPrimaryKeyColumn(primaryKey))...,
			)
		})

	if err != nil {
		return false, lazyerrors.Error(err)
	}

	q := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $meta_id as Uuid;
		DECLARE $json as JsonDocument;

		REPLACE INTO %s (meta_id, _jsonb)
		VALUES ($meta_id, $json);
		`, ydbPath, metadataTableName)

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, transaction.WriteTx, q, table.NewQueryParameters(
				table.ValueParam("$meta_id", ydbTypes.UuidValue(id)),
				table.ValueParam("$json", ydbTypes.JSONDocumentValueFromBytes(jsonData)),
			))

			return err
		})

	if err != nil {
		return false, lazyerrors.Error(err)
	}

	if r.Colls[params.DBName] == nil {
		r.Colls[params.DBName] = map[string]*Collection{}
	}
	r.Colls[params.DBName][collectionName] = c

	return true, nil
}

// CollectionGet returns a copy of collection metadata.
// It can be safely modified by a caller.
//
// If database or collection does not exist, nil is returned.
//
// If the user is not authenticated, it returns error.
func (r *Registry) CollectionGet(ctx context.Context, dbName, collectionName string) (*Collection, error) {
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return nil, lazyerrors.Error(err)
	}

	r.Rw.RLock()
	defer r.Rw.RUnlock()

	return r.collectionGet(dbName, collectionName), nil
}

// collectionGet returns a copy of collection metadata.
// It can be safely modified by a caller.
//
// If database or collection does not exist, nil is returned.
//
// It does not hold the lock.
func (r *Registry) collectionGet(dbName, collectionName string) *Collection {
	colls := r.Colls[dbName]
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
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return nil, lazyerrors.Error(err)
	}

	r.Rw.RLock()
	defer r.Rw.RUnlock()

	db := r.Colls[dbName]
	if db == nil {
		return nil, nil
	}

	res := make([]*Collection, 0, len(r.Colls[dbName]))
	for _, c := range r.Colls[dbName] {
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
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return false, lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

	return r.collectionDrop(ctx, dbName, collectionName)
}

const deleteFromMetadataTemplate = `
					PRAGMA TablePathPrefix("%v");

					DECLARE $meta_id AS Uuid;
			
					DELETE FROM %s WHERE meta_id=$meta_id
`

// collectionDrop drops a collection in the database.
//
// Returned boolean value indicates whether the collection was dropped.
// If database or collection did not exist, (false, nil) is returned.
//
// It does not hold the lock.
func (r *Registry) collectionDrop(ctx context.Context, dbName, collectionName string) (bool, error) {
	db := r.Colls[dbName]
	if db == nil {
		return false, nil
	}

	c := r.collectionGet(dbName, collectionName)
	if c == nil {
		return false, nil
	}

	ydbPath := r.DbMapping[dbName]
	id := uuid.MustParse(c.UUID)
	err := r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.DropTable(ctx, path.Join(ydbPath, c.TableName))
		})

	if err != nil {
		fmt.Printf("Failed to drop table: %v\n", err)
		return false, err
	}

	q := fmt.Sprintf(
		deleteFromMetadataTemplate,
		ydbPath,
		metadataTableName,
	)

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			_, _, err := s.Execute(
				ctx,
				transaction.WriteTx,
				q,
				table.NewQueryParameters(table.ValueParam("$meta_id", ydbTypes.UuidValue(id))))

			if err != nil {
				return err
			}
			return err
		})

	if err != nil {
		fmt.Printf("Failed to delete info from metadata table: %v\n", err)
		return false, err
	}

	delete(r.Colls[dbName], collectionName)

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
	ydbPath, err := r.LoadMetadata(ctx, dbName)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

	db := r.Colls[dbName]
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

		ydbPath,
		metadataTableName,
	)
	id := uuid.MustParse(c.UUID)

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				transaction.WriteTx,
				q,
				table.NewQueryParameters(
					table.ValueParam("$meta_id", ydbTypes.UuidValue(id)),
					table.ValueParam("$json", ydbTypes.JSONDocumentValueFromBytes(b)),
				))
			return err
		})

	if err != nil {
		return false, lazyerrors.Error(err)
	}

	r.Colls[dbName][newCollectionName] = c
	delete(r.Colls[dbName], oldCollectionName)

	return true, nil
}

// IndexesCreate creates indexes in the collection.
//
// Existing indexes with given names are ignored.
//
// If the user is not authenticated, it returns error.
func (r *Registry) IndexesCreate(ctx context.Context, dbName, collectionName string, indexes []IndexInfo) error {
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

	return r.indexesCreate(ctx, dbName, collectionName, indexes)
}

const replaceIntoMetadataTemplate = `
				PRAGMA TablePathPrefix("%v");

				DECLARE $meta_id AS Uuid;
				DECLARE $json AS JsonDocument;

				REPLACE INTO %s (meta_id, _jsonb) VALUES ($meta_id, $json);
`

// indexesCreate creates indexes in the collection.
//
// Existing indexes with given names are ignored.
//
// It does not hold the lock.
func (r *Registry) indexesCreate(ctx context.Context, dbName, collectionName string, indexes []IndexInfo) error {
	_, err := r.collectionCreate(ctx, &CollectionCreateParams{DBName: dbName, Name: collectionName})
	if err != nil {
		return lazyerrors.Error(err)
	}

	db := r.Colls[dbName]
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
	ydbPath := r.DbMapping[dbName]

	for _, index := range indexes {
		if coll, ok := allIndexes[index.Name]; ok && coll == collectionName {
			continue
		}

		for i := range index.Key {
			field := index.Key[i].Field

			jsonData, err := SelectJsonField(ctx, r.D.Driver.Table(), ydbPath, c.TableName, field)
			if err != nil {
				return lazyerrors.Error(err)
			}

			columnType, err := detectJsonFieldType(jsonData, field)
			if err != nil {
				return lazyerrors.Error(err)
			}

			index.Key[i].YdbType = columnType.String()

			err = addFieldColumn(ctx, r.D.Driver.Table(), ydbPath, c.TableName, field, columnType)
			if err != nil {
				return lazyerrors.Error(err)
			}

			err = updateColumnWithExistingValues(ctx, r.D.Driver.Table(), ydbPath, c.TableName, field, columnType)
			if err != nil {
				return lazyerrors.Error(err)
			}

			err = addIndex(ctx, r.D.Driver.Table(), ydbPath, c.TableName, field)
			if err != nil {
				return lazyerrors.Error(err)
			}

		}

		created = append(created, index.Name)
		c.Indexes = append(c.Indexes, index)
		allIndexes[index.Name] = collectionName
	}

	b, err := json.Marshal(c)
	if err != nil {
		return lazyerrors.Error(err)
	}

	q := fmt.Sprintf(
		replaceIntoMetadataTemplate,
		ydbPath,
		metadataTableName,
	)
	id := uuid.MustParse(c.UUID)

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				transaction.WriteTx,
				q,
				table.NewQueryParameters(
					table.ValueParam("$meta_id", ydbTypes.UuidValue(id)),
					table.ValueParam("$json", ydbTypes.JSONDocumentValueFromBytes(b)),
				))
			return err
		})

	if err != nil {
		return lazyerrors.Error(err)
	}

	r.Colls[dbName][collectionName] = c

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
	ydbPath, err := r.LoadMetadata(ctx, dbName)
	if err != nil {
		return lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

	return r.indexesDrop(ctx, dbName, collectionName, indexNames, ydbPath)
}

// indexesDrop removes given connection's indexes.
//
// Non-existing indexes are ignored.
//
// If database or collection does not exist, nil is returned.
//
// It does not hold the lock.
func (r *Registry) indexesDrop(
	ctx context.Context,
	dbName,
	collectionName string,
	indexNames []string,
	ydbPath string) error {
	c := r.collectionGet(dbName, collectionName)
	if c == nil {
		return nil
	}

	for _, name := range indexNames {
		i := slices.IndexFunc(c.Indexes, func(i IndexInfo) bool { return name == i.Name })
		if i < 0 {
			continue
		}

		idx := strings.Index(name, "_")
		name = name[:idx]
		err := dropIndex(ctx, r.D.Driver.Table(), ydbPath, c.TableName, name)
		if err != nil {
			return lazyerrors.Error(err)
		}

		err = dropFieldColumn(ctx, r.D.Driver.Table(), ydbPath, c.TableName, name)
		if err != nil {
			return lazyerrors.Error(err)
		}

		c.Indexes = slices.Delete(c.Indexes, i, i+1)
	}

	id := uuid.MustParse(c.UUID)
	b, err := json.Marshal(c)
	if err != nil {
		return lazyerrors.Error(err)
	}

	q := fmt.Sprintf(
		replaceIntoMetadataTemplate,
		ydbPath,
		metadataTableName,
	)

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				transaction.WriteTx,
				q,
				table.NewQueryParameters(
					table.ValueParam("$meta_id", ydbTypes.UuidValue(id)),
					table.ValueParam("$json", ydbTypes.JSONDocumentValueFromBytes(b)),
				))
			return err
		})

	if err != nil {
		return lazyerrors.Error(err)
	}

	r.Colls[dbName][collectionName] = c

	return nil
}

// Describe implements prometheus.Collector.
func (r *Registry) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(r, ch)
}

// Collect implements prometheus.Collector.
func (r *Registry) Collect(ch chan<- prometheus.Metric) {

	r.Rw.RLock()
	defer r.Rw.RUnlock()

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "databases"),
			"The current number of database in the registry.",
			nil, nil,
		),
		prometheus.GaugeValue,
		float64(len(r.Colls)),
	)

	for db, colls := range r.Colls {
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
