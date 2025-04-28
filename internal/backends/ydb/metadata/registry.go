package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata/transaction"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/FerretDB/FerretDB/internal/util/resource"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"hash/fnv"
	"log/slog"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
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

	maxObjectNameLength = 255
)

// specialCharacters are unsupported characters of YDB scheme object name that are replaced with `_`.
var specialCharacters = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

// Parts of Prometheus metric names.
const (
	namespace = "ferretdb"
	subsystem = "ydb_metadata"
)

// Registry provides access to YDB database and collections information.
//
// Registry metadata is loaded upon first call by client.
//
//nolint:vet // for readability
type Registry struct {
	D         *DB
	l         *slog.Logger
	BatchSize int
	rw        sync.RWMutex
	colls     map[string]map[string]*Collection // database name -> collection name -> collection
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
	ydbPath := path.Join(r.D.Driver.Name(), dbName)

	r.rw.RLock()
	if r.colls != nil && r.colls[dbName] != nil {
		r.rw.RUnlock()
		return ydbPath, nil
	}
	r.rw.RUnlock()

	r.rw.Lock()
	defer r.rw.Unlock()

	if err := r.initDirectory(ctx, ydbPath); err != nil {
		return "", lazyerrors.Error(err)
	}

	r.DbMapping[dbName] = ydbPath

	existsTable, err := sugar.IsTableExists(ctx, r.D.Driver.Scheme(), path.Join(ydbPath, metadataTableName))
	if err != nil || !existsTable {
		return "", nil
	}

	r.colls = make(map[string]map[string]*Collection)

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			var (
				res      result.Result
				jsonData string
			)

			q, _ := Render(SelectMetadataTmpl, TemplateConfig{
				TablePathPrefix: ydbPath,
				TableName:       metadataTableName,
				ColumnName:      DefaultColumn,
			})

			_, res, err = s.Execute(ctx, transaction.ReadTx, q, table.NewQueryParameters())

			if err != nil {
				return lazyerrors.Error(err)
			}

			defer func(res result.Result) {
				_ = res.Close()
			}(res)

			colls := map[string]*Collection{}

			err = res.NextResultSetErr(ctx)
			if err != nil {
				return lazyerrors.Error(err)
			}

			for res.NextRow() {
				err = res.ScanNamed(
					named.OptionalWithDefault(DefaultColumn, &jsonData),
				)
				if err != nil {
					return lazyerrors.Error(err)
				}

				var c Collection
				if err = json.Unmarshal([]byte(jsonData), &c); err != nil {
					return lazyerrors.Error(err)
				}

				colls[c.Name] = &c
			}

			if err = res.Err(); err != nil {
				return lazyerrors.Error(err)
			}

			r.colls[dbName] = colls

			return nil
		})

	return ydbPath, err
}

func (r *Registry) initDirectory(ctx context.Context, ydbPath string) error {
	exists, err := sugar.IsDirectoryExists(ctx, r.D.Driver.Scheme(), ydbPath)
	if err != nil {
		return lazyerrors.Error(err)
	}

	if !exists {
		err = r.D.Driver.Scheme().MakeDirectory(ctx, ydbPath)
		if err != nil {
			return lazyerrors.Error(err)
		}
	}

	return nil
}

// DatabaseList returns a sorted list of existing databases.
//
// If the user is not authenticated, it returns error.
func (r *Registry) DatabaseList(ctx context.Context, dbName string) ([]string, error) {
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return nil, lazyerrors.Error(err)
	}

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
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return nil, lazyerrors.Error(err)
	}

	r.rw.RLock()
	defer r.rw.RUnlock()

	db := r.colls[dbName]
	if db == nil {
		return nil, nil
	}

	return db, nil
}

// MetadataGetOrCreate returns a connection to existing database or newly created database.
//
// The dbName must be a validated database name.
//
// If the user is not authenticated, it returns error.
func (r *Registry) MetadataGetOrCreate(ctx context.Context, dbName string) error {
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return lazyerrors.Error(err)
	}

	r.rw.Lock()
	defer r.rw.Unlock()

	return r.metadataGetOrCreate(ctx, dbName)
}

// databaseGet returns a connection to existing database
//
// The dbName must be a validated database name.
//
// It does not hold the lock.
func (r *Registry) metadataGetOrCreate(ctx context.Context, dbName string) error {
	db := r.colls[dbName]
	if db != nil {
		return nil
	}

	ydbPath := r.DbMapping[dbName]

	err := r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(ydbPath, metadataTableName),
				options.WithColumn(DefaultIDColumn, ydbTypes.TypeUUID),
				options.WithColumn(DefaultColumn, ydbTypes.Optional(ydbTypes.TypeJSON)),
				options.WithPrimaryKeyColumn(DefaultIDColumn),
			)
		})

	if err != nil {
		return lazyerrors.Error(err)
	}

	if r.colls == nil {
		r.colls = make(map[string]map[string]*Collection)
	}

	r.colls[dbName] = map[string]*Collection{}

	return nil
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
	collectionName := params.Name
	ydbPath, exists := r.DbMapping[params.DBName]
	if !exists {
		return false, lazyerrors.Errorf("target backend scheme object not found for %q", params.DBName)
	}

	err := r.metadataGetOrCreate(ctx, params.DBName)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	if r.collectionExists(params.DBName, params.Name) {
		return false, nil
	}

	tableName := r.generateUniqueTableName(params.DBName, params.Name)

	err = r.createTable(ctx, ydbPath, tableName, params.Capped())
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	collection, err := r.storeCollectionMetadata(ctx, params, ydbPath, tableName)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	if r.colls[params.DBName] == nil {
		r.colls[params.DBName] = map[string]*Collection{}
	}
	r.colls[params.DBName][collectionName] = collection

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
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
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
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
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

	id, err := uuid.Parse(c.UUID)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	ydbPath := r.DbMapping[dbName]

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.DropTable(ctx, path.Join(ydbPath, c.TableName))
		},
	)

	if err != nil {
		return false, lazyerrors.Error(err)
	}

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			q, _ := Render(DeleteFromMetadataTmpl, ReplaceIntoMetadataConfig{
				TablePathPrefix: ydbPath,
				TableName:       metadataTableName,
			})

			_, res, err := s.Execute(ctx, transaction.WriteTx, q,
				table.NewQueryParameters(table.ValueParam("$meta_id", ydbTypes.UuidValue(id))))
			if err != nil {
				return err
			}

			err = res.Err()
			if err != nil {
				return err
			}

			return res.Close()
		},
	)

	if err != nil {
		return false, lazyerrors.Error(err)
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
	ydbPath, err := r.LoadMetadata(ctx, dbName)
	if err != nil {
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

	q, _ := Render(UpdateMedataTmpl, ReplaceIntoMetadataConfig{
		TablePathPrefix: ydbPath,
		TableName:       metadataTableName,
	})

	id := uuid.MustParse(c.UUID)

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				transaction.WriteTx,
				q,
				table.NewQueryParameters(
					table.ValueParam("$meta_id", ydbTypes.UuidValue(id)),
					table.ValueParam("$json", ydbTypes.JSONValueFromBytes(b)),
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

//func (r *Registry) handleUniqueIndex(ctx context.Context, dbName, collectionName string, index IndexInfo) error {
//
//	return nil
//}

// IndexesCreate creates indexes in the collection.
//
// Existing indexes with given names are ignored.
//
// If the user is not authenticated, it returns error.
func (r *Registry) IndexesCreate(ctx context.Context, dbName, collectionName string, indexes []IndexInfo) error {
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return lazyerrors.Error(err)
	}

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
	_, err := r.collectionCreate(ctx, &CollectionCreateParams{DBName: dbName, Name: collectionName})
	if err != nil {
		return lazyerrors.Error(err)
	}

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
	ydbPath := r.DbMapping[dbName]
	fields := map[string]string{}

	for _, index := range indexes {
		if coll, ok := allIndexes[index.Name]; ok && coll == collectionName {
			continue
		}

		supported := SupportedIndexTypes()
		definedOrder := GetColumnsInDefinedOrder()
		columnNames := make([]string, 0, len(supported))
		for i := range index.Key {
			field := index.Key[i].Field

			if _, exists := fields[field]; exists {
				continue
			}

			if strings.Contains(field, ".") {
				field = strings.ReplaceAll(field, ".", "")
			}

			columns := make([]options.AlterTableOption, 0, len(supported))
			for _, bsonType := range definedOrder {
				if t, ok := supported[bsonType]; ok {
					columnName := fmt.Sprintf("%s_%s", field, bsonType)
					columns = append(columns, options.WithAddColumn(columnName, ydbTypes.Optional(t)))
					columnNames = append(columnNames, columnName)
				}
			}

			err = addFieldColumns(ctx, r.D.Driver.Table(), ydbPath, c.TableName, columns)
			if err != nil {
				return lazyerrors.Error(err)
			}

			fields[field] = index.Name
		}

		if len(columnNames) > 0 {
			err = addIndex(ctx, r.D.Driver.Table(), ydbPath, c.TableName, columnNames, index.Name)
			if err != nil {
				return lazyerrors.Error(err)
			}
		}

		index.Ready = false
		created = append(created, index.Name)
		c.Indexes = append(c.Indexes, index)
		allIndexes[index.Name] = collectionName
	}

	b, err := json.Marshal(c)
	if err != nil {
		return lazyerrors.Error(err)
	}

	q, _ := Render(UpdateMedataTmpl, ReplaceIntoMetadataConfig{
		TablePathPrefix: ydbPath,
		TableName:       metadataTableName,
	})

	id := uuid.MustParse(c.UUID)
	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				transaction.WriteTx,
				q,
				table.NewQueryParameters(
					table.ValueParam("$meta_id", ydbTypes.UuidValue(id)),
					table.ValueParam("$json", ydbTypes.JSONValueFromBytes(b)),
				))
			return err
		})

	if err != nil {
		return lazyerrors.Error(err)
	}

	r.colls[dbName][collectionName] = c

	for _, index := range indexes {
		go func(index IndexInfo, coll *Collection, ydbPath string) {
			childCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			err := migrateIndexData(childCtx, r.D.Driver.Table(), ydbPath, coll.TableName, index.Key)
			if err != nil {
				fmt.Printf("index migration failed: %v\n", err)
				return
			}
			for i := range coll.Indexes {
				if coll.Indexes[i].Name == index.Name {
					coll.Indexes[i].Ready = true
				}
			}

			b, err := json.Marshal(coll)
			if err != nil {
				fmt.Printf("index migration marshal error: %v\n", err)
				return
			}

			q, _ := Render(UpdateMedataTmpl, ReplaceIntoMetadataConfig{
				TablePathPrefix: ydbPath,
				TableName:       metadataTableName,
			})

			id := uuid.MustParse(coll.UUID)

			_ = r.D.Driver.Table().Do(childCtx, func(ctx context.Context, s table.Session) error {
				_, _, err := s.Execute(
					ctx,
					transaction.WriteTx,
					q,
					table.NewQueryParameters(
						table.ValueParam("$meta_id", ydbTypes.UuidValue(id)),
						table.ValueParam("$json", ydbTypes.JSONValueFromBytes(b)),
					))
				return err
			})

		}(index, c, ydbPath)
	}

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

	r.rw.Lock()
	defer r.rw.Unlock()

	return r.indexesDrop(ctx, dbName, collectionName, indexNames, ydbPath)
}

// indexesDrop removes given connection's indexes.
//
// Non-existing indexes are ignored.
//
// If database or collection does not exist, nil is returned.
//
// It does not hold the lock.
func (r *Registry) indexesDrop(ctx context.Context, dbName, collectionName string, indexNames []string, ydbPath string) error {
	c := r.collectionGet(dbName, collectionName)
	if c == nil {
		return nil
	}

	fieldsPlanToDelete := make(map[string]struct{})
	for _, name := range indexNames {
		i := slices.IndexFunc(c.Indexes, func(i IndexInfo) bool { return name == i.Name })
		if i < 0 {
			continue
		}

		err := dropIndex(ctx, r.D.Driver.Table(), ydbPath, c.TableName, name)
		if err != nil {
			return lazyerrors.Error(err)
		}

		index := c.Indexes[i]
		for _, pair := range index.Key {
			fieldsPlanToDelete[pair.Field] = struct{}{}
		}

		c.Indexes = slices.Delete(c.Indexes, i, i+1)
	}

	for _, info := range c.Indexes {
		for _, pair := range info.Key {
			if _, ok := fieldsPlanToDelete[pair.Field]; ok {
				delete(fieldsPlanToDelete, pair.Field)
			}
		}
	}

	supported := SupportedIndexTypes()
	columns := make([]options.AlterTableOption, 0, len(supported))
	for field := range fieldsPlanToDelete {
		fieldName := strings.ReplaceAll(field, ".", "")
		for b, _ := range supported {
			columnName := fmt.Sprintf("%s_%s", fieldName, b)
			columns = append(columns, options.WithDropColumn(columnName))
		}
	}

	err := dropFieldColumns(ctx, r.D.Driver.Table(), ydbPath, c.TableName, columns)
	if err != nil {
		return lazyerrors.Error(err)
	}

	id := uuid.MustParse(c.UUID)
	b, err := json.Marshal(c)
	if err != nil {
		return lazyerrors.Error(err)
	}

	q, _ := Render(UpdateMedataTmpl, ReplaceIntoMetadataConfig{
		TablePathPrefix: ydbPath,
		TableName:       metadataTableName,
	})

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				transaction.WriteTx,
				q,
				table.NewQueryParameters(
					table.ValueParam("$meta_id", ydbTypes.UuidValue(id)),
					table.ValueParam("$json", ydbTypes.JSONValueFromBytes(b)),
				))
			return err
		})

	if err != nil {
		return lazyerrors.Error(err)
	}

	r.colls[dbName][collectionName] = c

	return nil
}

// DatabaseDrop drops the database.
//
// Returned boolean value indicates whether the database was dropped.
// If database does not exist, (false, nil) is returned.
//
// If the user is not authenticated, it returns error.
func (r *Registry) DatabaseDrop(ctx context.Context, dbName string) (bool, error) {
	if _, err := r.LoadMetadata(ctx, dbName); err != nil {
		return false, lazyerrors.Error(err)
	}

	r.rw.Lock()
	defer r.rw.Unlock()

	return r.databaseDrop(ctx, dbName)
}

// DatabaseDrop drops the database.
//
// Returned boolean value indicates whether the database was dropped.
// If database does not exist, (false, nil) is returned.
//
// It does not hold the lock.
func (r *Registry) databaseDrop(ctx context.Context, dbName string) (bool, error) {
	db := r.colls[dbName]
	if db == nil {
		return false, nil
	}

	err := sugar.RemoveRecursive(ctx, r.D.Driver, dbName)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	delete(r.colls, dbName)

	return true, nil
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

func (r *Registry) createTable(ctx context.Context, ydbPath, tableName string, isCapped bool) error {
	columns := []options.CreateTableOption{
		options.WithColumn(DefaultColumn, ydbTypes.Optional(ydbTypes.TypeJSON)),
		options.WithColumn(DefaultIDColumn, ydbTypes.TypeString),
	}

	if isCapped {
		columns = append(columns, options.WithColumn(RecordIDColumn, ydbTypes.TypeInt64))
	}

	return r.D.Driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, path.Join(ydbPath, tableName),
			append(columns, options.WithPrimaryKeyColumn(DefaultIDColumn))...,
		)
	})
}

func (r *Registry) collectionExists(dbName, collectionName string) bool {
	colls := r.colls[dbName]
	return colls != nil && colls[collectionName] != nil
}

func (r *Registry) generateUniqueTableName(dbName, collectionName string) string {
	h := fnv.New32a()
	must.NotFail(h.Write([]byte(collectionName)))
	s := h.Sum32()

	var tableName string
	list := maps.Values(r.colls[dbName])

	for {
		tableName = specialCharacters.ReplaceAllString(strings.ToLower(collectionName), "_")

		suffixHash := fmt.Sprintf("_%08x", s)
		if l := maxObjectNameLength - len(suffixHash); len(tableName) > l {
			tableName = tableName[:l]
		}

		tableName = fmt.Sprintf("%s%s", tableName, suffixHash)

		if !slices.ContainsFunc(list, func(c *Collection) bool { return c.TableName == tableName }) {
			break
		}

		// table already exists, generate a new table name by incrementing the hash
		s++
	}

	return tableName
}

func (r *Registry) storeCollectionMetadata(ctx context.Context, params *CollectionCreateParams, ydbPath, tableName string) (*Collection, error) {
	collection := &Collection{
		Name:      params.Name,
		UUID:      uuid.New().String(),
		TableName: tableName,
		Indexes: []IndexInfo{
			{
				Name:   fmt.Sprintf("_%s_", DefaultIDColumn),
				Ready:  true,
				Key:    []IndexKeyPair{{Field: "_id"}},
				Unique: true,
			},
		},
		Settings: Settings{
			CappedSize:      params.CappedSize,
			CappedDocuments: params.CappedDocuments,
		},
	}

	jsonData, err := json.Marshal(collection)
	if err != nil {
		return nil, err
	}

	q, _ := Render(UpdateMedataTmpl, ReplaceIntoMetadataConfig{
		TablePathPrefix: ydbPath,
		TableName:       metadataTableName,
	})

	err = r.D.Driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, res, err := s.Execute(ctx, transaction.WriteTx, q, table.NewQueryParameters(
			table.ValueParam("$meta_id", ydbTypes.UuidValue(uuid.MustParse(collection.UUID))),
			table.ValueParam("$json", ydbTypes.JSONValueFromBytes(jsonData)),
		))
		if err != nil {
			return err
		}

		err = res.Err()
		if err != nil {
			return err
		}

		return res.Close()
	},
		table.WithIdempotent(),
	)

	return collection, err
}

// check interfaces
var (
	_ prometheus.Collector = (*Registry)(nil)
)
