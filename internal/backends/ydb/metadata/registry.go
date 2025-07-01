package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata/transaction"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"golang.org/x/exp/maps"
	"log/slog"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/state"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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
	Rw        sync.RWMutex
	colls     map[string]map[string]*Collection // database name -> collection name -> collection
	DbMapping map[string]string
}

// NewRegistry creates a registry for YDB database with a given base URI.
func NewRegistry(dsn, auth, ca string, batchSize int, l *slog.Logger, sp *state.Provider) (*Registry, error) {
	db, err := New(dsn, auth, ca, l, sp)
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
	r.D.Close()
}

// TODO: add centralized cache, e.g. https://github.com/valkey-io/valkey
// LoadMetadataTable loads collections metadata from the database
// It uses paginated metadata loading in batches of 1000 records
func (r *Registry) LoadMetadataTable(ctx context.Context, dbName string) error {
	if shouldSkipDatabase(dbName) {
		return nil
	}

	r.Rw.RLock()
	if r.colls != nil && r.colls[dbName] != nil {
		r.Rw.RUnlock()
		return nil
	}
	r.Rw.RUnlock()

	r.Rw.Lock()
	defer r.Rw.Unlock()

	ydbPath := path.Join(r.D.Driver.Name(), dbName)
	r.DbMapping[dbName] = ydbPath

	r.colls = make(map[string]map[string]*Collection)

	var (
		lastKey = ""
		limit   = defaultBatchSize
		empty   = false
		err     error
	)

	for !empty {
		empty, err = r.loadMetadataPage(ctx, ydbPath, dbName, limit, &lastKey)
		if err != nil {
			return lazyerrors.Error(err)
		}
	}

	return nil
}

// TODO: add centralized cache, e.g. https://github.com/valkey-io/valkey
func (r *Registry) LoadCollectionMetadata(ctx context.Context, dbName, collectionName string) error {
	r.Rw.RLock()
	if r.colls != nil {
		if dbColls, ok := r.colls[dbName]; ok && dbColls != nil {
			if dbColls[collectionName] != nil {
				r.Rw.RUnlock()
				return nil
			}
		}
	}
	r.Rw.RUnlock()

	r.Rw.Lock()
	defer r.Rw.Unlock()

	ydbPath := path.Join(r.D.Driver.Name(), dbName)
	r.DbMapping[dbName] = ydbPath

	err := r.fetchCollectionMetadata(ctx, ydbPath, dbName, collectionName)
	if err != nil {
		return lazyerrors.Error(err)
	}

	return nil
}

// DatabaseList returns a sorted list of existing databases.
func (r *Registry) DatabaseList(ctx context.Context, dbName string) ([]string, error) {
	if err := r.LoadMetadataTable(ctx, dbName); err != nil {
		return nil, lazyerrors.Error(err)
	}

	r.Rw.RLock()
	defer r.Rw.RUnlock()

	res := maps.Keys(r.colls)
	sort.Strings(res)

	return res, nil
}

// DatabaseGetExisting retrieves collections for the specified database or nil if it doesn't exist.
func (r *Registry) DatabaseGetExisting(ctx context.Context, dbName string) (map[string]*Collection, error) {
	if err := r.LoadMetadataTable(ctx, dbName); err != nil {
		return nil, lazyerrors.Error(err)
	}

	r.Rw.RLock()
	defer r.Rw.RUnlock()

	db := r.colls[dbName]
	if db == nil {
		return nil, nil
	}

	return db, nil
}

// DatabaseGetOrCreate retrieves collections for the existing database or newly created database.
//
// The dbName must be a validated database name.
func (r *Registry) DatabaseGetOrCreate(ctx context.Context, dbName string) error {
	if err := r.LoadMetadataTable(ctx, dbName); err != nil {
		return lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

	return r.databaseGetOrCreate(ctx, dbName)
}

// databaseGetOrCreate returns a connection to existing database
//
// The dbName must be a validated database name.
//
// It does not hold the lock.
func (r *Registry) databaseGetOrCreate(ctx context.Context, dbName string) error {
	db := r.colls[dbName]
	if db != nil {
		return nil
	}

	ydbPath := r.DbMapping[dbName]
	if err := r.initDirectory(ctx, ydbPath); err != nil {
		return lazyerrors.Error(err)
	}

	err := r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(ydbPath, metadataTableName),
				options.WithColumn(DefaultIdColumn, ydbTypes.TypeString),
				options.WithColumn(DefaultColumn, ydbTypes.TypeJSON),
				options.WithPrimaryKeyColumn(DefaultIdColumn),
				options.WithProfile(
					options.WithReplicationPolicy(
						options.WithReplicationPolicyReplicasCount(1),
						options.WithReplicationPolicyCreatePerAZ(options.FeatureEnabled),
					),
					options.WithPartitioningPolicy(
						options.WithPartitioningPolicyMode(options.PartitioningAutoSplit),
					),
				),
			)
		})

	if err != nil {
		_, dropErr := r.databaseDrop(ctx, dbName)
		if dropErr != nil {
			return lazyerrors.Error(dropErr)
		}

		return lazyerrors.Error(err)
	}

	if r.colls == nil {
		r.colls = make(map[string]map[string]*Collection)
	}

	r.colls[dbName] = map[string]*Collection{}

	return nil
}

// DatabaseDrop drops the database.
//
// Returned boolean value indicates whether the database was dropped.
// If database does not exist, (false, nil) is returned.
func (r *Registry) DatabaseDrop(ctx context.Context, dbName string) (bool, error) {
	if err := r.LoadMetadataTable(ctx, dbName); err != nil {
		return false, lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

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
	delete(r.DbMapping, dbName)

	return true, nil
}

// CollectionList returns a sorted copy of collections in the database.
//
// If database does not exist, no error is returned.
func (r *Registry) CollectionList(ctx context.Context, dbName string) ([]*Collection, error) {
	if err := r.LoadMetadataTable(ctx, dbName); err != nil {
		return nil, lazyerrors.Error(err)
	}

	r.Rw.RLock()
	defer r.Rw.RUnlock()

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

// CollectionCreate creates a collection in the database.
//
// Returned boolean value indicates whether the collection was created.
// If collection already exists, (false, nil) is returned.
func (r *Registry) CollectionCreate(ctx context.Context, params *CollectionCreateParams) (bool, error) {
	if err := r.LoadCollectionMetadata(ctx, params.DBName, params.Name); err != nil {
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
	dbName, collectionName := params.DBName, params.Name

	err := r.databaseGetOrCreate(ctx, dbName)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	ydbPath, exists := r.DbMapping[dbName]
	if !exists {
		return false, lazyerrors.Errorf("target backend scheme object not found for %q", params.DBName)
	}

	exists = r.collectionExists(dbName, collectionName)
	if exists {
		return false, nil
	}

	tableName := r.GenerateUniqueTableName(dbName, collectionName)

	var columnDefs []string
	var pkColumnNames []string
	var secondaryIndexDefs []SecondaryIndexDef

	indexSet := make(map[string]struct{})
	fieldSet := make(map[string]struct{})

	var primaryKeyColumns = BuildPrimaryKeyColumns()

	for i := range primaryKeyColumns {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", primaryKeyColumns[i].Name, primaryKeyColumns[i].Type.String()))
		pkColumnNames = append(pkColumnNames, primaryKeyColumns[i].Name)
	}

	if params.Capped() {
		primaryKeyColumns = append(primaryKeyColumns, PrimaryKeyColumn{
			Name: RecordIDColumn,
			Type: ydbTypes.Optional(ydbTypes.TypeInt64),
		})

		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", RecordIDColumn, ydbTypes.Optional(ydbTypes.TypeInt64)))
		pkColumnNames = append(pkColumnNames, RecordIDColumn)
	}

	indexSet[backends.DefaultIndexName] = struct{}{}
	fieldSet[IdMongoField] = struct{}{}

	for i, index := range params.Indexes {
		if _, seen := indexSet[index.Name]; seen {
			continue
		}

		indexName := generateIndexName(index.Name)
		params.Indexes[i].SanitizedName = indexName

		var indexColumns []string
		for _, pair := range index.Key {
			fieldName := columnNameCharacters.ReplaceAllString(pair.Field, "_")

			for _, suffix := range ColumnOrder {
				columnName := fmt.Sprintf("%s_%s", fieldName, suffix)
				if _, seen := fieldSet[pair.Field]; !seen {
					columnDefs = append(columnDefs, fmt.Sprintf("%s %s", columnName, ydbTypes.Optional(ColumnStoreToYdbType(suffix))))
				}
				indexColumns = append(indexColumns, columnName)
			}
			fieldSet[pair.Field] = struct{}{}
		}

		secondaryIndexDefs = append(secondaryIndexDefs, SecondaryIndexDef{
			Name:    indexName,
			Unique:  index.Unique,
			Columns: indexColumns,
		})

		params.Indexes[i].Ready = true
		indexSet[index.Name] = struct{}{}
	}

	q, err := Render(CreateTableTmpl, CreateTableTemplateConfig{
		TablePathPrefix:   ydbPath,
		TableName:         tableName,
		ColumnDefs:        strings.Join(columnDefs, ", "),
		PrimaryKeyColumns: pkColumnNames,
		Indexes:           secondaryIndexDefs,
	})

	if err != nil {
		return false, lazyerrors.Error(err)
	}

	err = r.D.Driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, q)
	})

	if err != nil {
		return false, lazyerrors.Error(err)
	}

	collection, err := r.storeCollectionMetadata(ctx, params, ydbPath, tableName)
	if err != nil {
		dropErr := r.D.Driver.Table().Do(ctx,
			func(ctx context.Context, s table.Session) error {
				return s.DropTable(ctx, path.Join(ydbPath, tableName))
			},
		)
		if dropErr != nil {
			return false, lazyerrors.Error(dropErr)
		}

		return false, lazyerrors.Error(err)
	}

	if r.colls[dbName] == nil {
		r.colls[dbName] = map[string]*Collection{}
	}
	r.colls[dbName][collectionName] = collection

	return true, nil
}

func (r *Registry) GenerateUniqueTableName(dbName, collectionName string) string {
	s := fnv32Hash(collectionName)
	var tableName string
	list := maps.Values(r.colls[dbName])

	for {
		tableName = objectNameCharacters.ReplaceAllString(strings.ToLower(collectionName), "_")

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

// CollectionGet returns a copy of collection metadata.
func (r *Registry) CollectionGet(ctx context.Context, dbName, collectionName string) (*Collection, error) {
	if err := r.LoadCollectionMetadata(ctx, dbName, collectionName); err != nil {
		return nil, lazyerrors.Error(err)
	}

	r.Rw.RLock()
	defer r.Rw.RUnlock()

	return r.collectionGet(dbName, collectionName), nil
}

func (r *Registry) CollectionGetCopy(dbName, collectionName string) (*Collection, error) {
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
	colls := r.colls[dbName]
	if colls == nil {
		return nil
	}

	return colls[collectionName].deepCopy()
}

// CollectionDrop drops a collection in the database.
//
// Returned boolean value indicates whether the collection was dropped.
// If database or collection did not exist, (false, nil) is returned.
func (r *Registry) CollectionDrop(ctx context.Context, dbName, collectionName string) (bool, error) {
	if err := r.LoadCollectionMetadata(ctx, dbName, collectionName); err != nil {
		return false, lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

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

	ydbPath := r.DbMapping[dbName]
	err := r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.DropTable(ctx, path.Join(ydbPath, c.TableName))
		},
	)

	if err != nil {
		return false, lazyerrors.Error(err)
	}

	var p Placeholder
	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			q, _ := Render(DeleteFromMetadataTmpl, ReplaceIntoMetadataConfig{
				TablePathPrefix: ydbPath,
				TableName:       metadataTableName,
			})

			_, res, err := s.Execute(ctx, transaction.WriteTx, q,
				table.NewQueryParameters(table.ValueParam(p.Named("id"), ydbTypes.BytesValueFromString(collectionName))),
			)
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
func (r *Registry) CollectionRename(ctx context.Context, dbName, oldCollectionName, newCollectionName string) (bool, error) {
	err := r.LoadCollectionMetadata(ctx, dbName, oldCollectionName)
	if err != nil {
		return false, lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

	ydbPath := r.DbMapping[dbName]
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

	var p Placeholder

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				transaction.WriteTx,
				q,
				table.NewQueryParameters(
					table.ValueParam(p.Named("id"), ydbTypes.BytesValueFromString(newCollectionName)),
					table.ValueParam(p.Named("json"), ydbTypes.JSONValueFromBytes(b)),
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
	if err := r.LoadCollectionMetadata(ctx, dbName, collectionName); err != nil {
		return lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

	return r.indexesCreate(ctx, dbName, collectionName, indexes)
}

// indexesCreate creates indexes in the collection.
//
// Existing indexes with given names are ignored.
//
// It does not hold the lock.
func (r *Registry) indexesCreate(ctx context.Context, dbName, collectionName string, indexesReq []IndexInfo) error {
	newlyCreated, err := r.collectionCreate(ctx, &CollectionCreateParams{DBName: dbName, Name: collectionName, Indexes: indexesReq})
	if err != nil {
		return lazyerrors.Error(err)
	}

	if newlyCreated {
		return nil
	}

	db := r.colls[dbName]
	if db == nil {
		panic("database does not exist")
	}

	targetColl := r.collectionGet(dbName, collectionName)
	if targetColl == nil {
		panic("collection does not exist")
	}

	allIndexes := make(map[string]string, len(db))
	existingFields := make(map[string]string, len(indexesReq))

	for _, coll := range db {
		for _, index := range coll.Indexes {
			allIndexes[index.Name] = coll.Name
		}
	}

	for _, index := range targetColl.Indexes {
		for _, pair := range index.Key {
			existingFields[pair.Field] = index.Name
		}
	}

	ydbPath := r.DbMapping[dbName]

	for _, index := range indexesReq {
		if coll, ok := allIndexes[index.Name]; ok && coll == collectionName {
			continue
		}

		if index.Unique {
			continue
		}

		indexName := generateIndexName(index.Name)

		columnNames := make([]string, 0, len(ColumnOrder))

		for i := range index.Key {
			field := index.Key[i].Field
			field = columnNameCharacters.ReplaceAllString(strings.ToLower(field), "_")

			columns := make([]options.AlterTableOption, 0, len(ColumnOrder))
			for _, suffix := range ColumnOrder {
				columnName := fmt.Sprintf("%s_%s", field, suffix)
				columns = append(columns, options.WithAddColumn(columnName, ydbTypes.Optional(ColumnStoreToYdbType(suffix))))
				columnNames = append(columnNames, columnName)

			}

			if _, exists := existingFields[field]; exists {
				continue
			}

			err = addFieldColumns(ctx, r.D.Driver.Table(), ydbPath, targetColl.TableName, columns)
			if err != nil {
				return lazyerrors.Error(err)
			}

			existingFields[field] = index.Name
		}

		if len(columnNames) > 0 {
			err = addIndex(ctx, r.D.Driver.Table(), ydbPath, targetColl.TableName, columnNames, indexName)
			if err != nil {
				return lazyerrors.Error(err)
			}
		}

		index.Ready = false
		index.SanitizedName = indexName
		targetColl.Indexes = append(targetColl.Indexes, index)
		allIndexes[index.Name] = collectionName
	}

	b, err := json.Marshal(targetColl)
	if err != nil {
		return lazyerrors.Error(err)
	}

	q, _ := Render(UpdateMedataTmpl, ReplaceIntoMetadataConfig{
		TablePathPrefix: ydbPath,
		TableName:       metadataTableName,
	})

	var p Placeholder

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				transaction.WriteTx,
				q,
				table.NewQueryParameters(
					table.ValueParam(p.Named("id"), ydbTypes.BytesValueFromString(collectionName)),
					table.ValueParam(p.Named("json"), ydbTypes.JSONValueFromBytes(b)),
				))
			return err
		})

	if err != nil {
		return lazyerrors.Error(err)
	}

	r.colls[dbName][collectionName] = targetColl

	for _, index := range indexesReq {
		if index.Unique {
			continue
		}
		go func(index IndexInfo, coll *Collection, ydbPath string) {
			childCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			err := migrateIndexData(childCtx, r.D.Driver, ydbPath, coll.TableName, index.Key)
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

			var p Placeholder

			_ = r.D.Driver.Table().Do(childCtx, func(ctx context.Context, s table.Session) error {
				_, _, err := s.Execute(
					ctx,
					transaction.WriteTx,
					q,
					table.NewQueryParameters(
						table.ValueParam(p.Named("id"), ydbTypes.BytesValueFromString(collectionName)),
						table.ValueParam(p.Named("json"), ydbTypes.JSONValueFromBytes(b)),
					))
				return err
			})

		}(index, targetColl, ydbPath)
	}

	return nil
}

// IndexesDrop removes given connection's indexes.
//
// Non-existing indexes are ignored.
//
// If database or collection does not exist, nil is returned.
func (r *Registry) IndexesDrop(ctx context.Context, dbName, collectionName string, indexNames []string) error {
	err := r.LoadCollectionMetadata(ctx, dbName, collectionName)
	if err != nil {
		return lazyerrors.Error(err)
	}

	r.Rw.Lock()
	defer r.Rw.Unlock()

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

	ydbPath := r.DbMapping[dbName]
	fieldsPlanToDelete := make(map[string]struct{})
	for _, name := range indexNames {
		i := slices.IndexFunc(c.Indexes, func(i IndexInfo) bool { return name == i.Name })
		if i < 0 {
			continue
		}

		err := dropIndex(ctx, r.D.Driver.Table(), ydbPath, c.TableName, c.Indexes[i].SanitizedName)
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

	columns := make([]options.AlterTableOption, 0, len(ColumnOrder))
	for field := range fieldsPlanToDelete {
		fieldName := columnNameCharacters.ReplaceAllString(field, "_")
		for _, colType := range ColumnOrder {
			columnName := fmt.Sprintf("%s_%s", fieldName, colType)
			columns = append(columns, options.WithDropColumn(columnName))
		}
	}

	if len(columns) > 0 {
		err := dropFieldColumns(ctx, r.D.Driver.Table(), ydbPath, c.TableName, columns)
		if err != nil {
			return lazyerrors.Error(err)
		}
	}

	b, err := json.Marshal(c)
	if err != nil {
		return lazyerrors.Error(err)
	}

	q, _ := Render(UpdateMedataTmpl, ReplaceIntoMetadataConfig{
		TablePathPrefix: ydbPath,
		TableName:       metadataTableName,
	})

	var p Placeholder

	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				transaction.WriteTx,
				q,
				table.NewQueryParameters(
					table.ValueParam(p.Named("id"), ydbTypes.BytesValueFromString(collectionName)),
					table.ValueParam(p.Named("json"), ydbTypes.JSONValueFromBytes(b)),
				))
			return err
		})

	if err != nil {
		return lazyerrors.Error(err)
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

	r.Rw.RLock()
	defer r.Rw.RUnlock()

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
