package ydb

import (
	"cmp"
	"context"
	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"slices"
)

// database implements backends.Database interface.
type database struct {
	r    *metadata.Registry
	name string
}

// newDatabase creates a new Database.
func newDatabase(r *metadata.Registry, name string) backends.Database {
	return backends.DatabaseContract(&database{
		r:    r,
		name: name,
	})
}

// Collection implements backends.Database interface.
func (db *database) Collection(name string) (backends.Collection, error) {
	return newCollection(db.r, db.name, name), nil
}

// ListCollections implements backends.Database interface.
//
//nolint:lll // for readability
func (db *database) ListCollections(ctx context.Context, params *backends.ListCollectionsParams) (*backends.ListCollectionsResult, error) {
	list, err := db.r.CollectionList(ctx, db.name)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	var res []backends.CollectionInfo

	if params != nil && len(params.Name) > 0 {
		nameList := make([]string, len(list))
		for i, c := range list {
			nameList[i] = c.Name
		}

		i, found := slices.BinarySearchFunc(nameList, params.Name, func(collectionName, t string) int {
			return cmp.Compare(collectionName, t)
		})
		var filteredList []*metadata.Collection

		if found {
			filteredList = append(filteredList, list[i])
		}
		list = filteredList
	}

	res = make([]backends.CollectionInfo, len(list))

	for i, c := range list {
		res[i] = backends.CollectionInfo{
			Name:            c.Name,
			UUID:            c.UUID,
			CappedSize:      c.Settings.CappedSize,
			CappedDocuments: c.Settings.CappedDocuments,
		}
	}

	return &backends.ListCollectionsResult{
		Collections: res,
	}, nil
}

// CreateCollection implements backends.Database interface.
func (db *database) CreateCollection(ctx context.Context, params *backends.CreateCollectionParams) error {
	created, err := db.r.CollectionCreate(ctx, &metadata.CollectionCreateParams{
		DBName:          db.name,
		Name:            params.Name,
		CappedSize:      params.CappedSize,
		CappedDocuments: params.CappedDocuments,
	})
	if err != nil {
		return lazyerrors.Error(err)
	}

	if !created {
		return backends.NewError(backends.ErrorCodeCollectionAlreadyExists, err)
	}

	return nil
}

// DropCollection implements backends.Database interface.
func (db *database) DropCollection(ctx context.Context, params *backends.DropCollectionParams) error {
	dropped, err := db.r.CollectionDrop(ctx, db.name, params.Name)
	if err != nil {
		return lazyerrors.Error(err)
	}

	if !dropped {
		return backends.NewError(backends.ErrorCodeCollectionDoesNotExist, err)
	}

	return nil
}

// RenameCollection implements backends.Database interface.
func (db *database) RenameCollection(ctx context.Context, params *backends.RenameCollectionParams) error {
	c, err := db.r.CollectionGet(ctx, db.name, params.OldName)
	if err != nil {
		return lazyerrors.Error(err)
	}

	if c == nil {
		return backends.NewError(
			backends.ErrorCodeCollectionDoesNotExist,
			lazyerrors.Errorf("old database %q or collection %q does not exist", db.name, params.OldName),
		)
	}

	c, err = db.r.CollectionGet(ctx, db.name, params.NewName)
	if err != nil {
		return lazyerrors.Error(err)
	}

	if c != nil {
		return backends.NewError(
			backends.ErrorCodeCollectionAlreadyExists,
			lazyerrors.Errorf("new database %q and collection %q already exists", db.name, params.NewName),
		)
	}

	renamed, err := db.r.CollectionRename(ctx, db.name, params.OldName, params.NewName)
	if err != nil {
		return lazyerrors.Error(err)
	}

	if !renamed {
		return backends.NewError(backends.ErrorCodeCollectionDoesNotExist, err)
	}

	return nil
}

// Stats implements backends.Database interface.
func (db *database) Stats(ctx context.Context, params *backends.DatabaseStatsParams) (*backends.DatabaseStatsResult, error) {
	if params == nil {
		params = new(backends.DatabaseStatsParams)
	}

	p, err := db.r.DatabaseGetExisting(ctx, db.name)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if p == nil {
		return nil, backends.NewError(backends.ErrorCodeDatabaseDoesNotExist, lazyerrors.Errorf("no database %s", db.name))
	}

	list, err := db.r.CollectionList(ctx, db.name)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	var totalDocs, totalSizeTables, totalSizeIndexes, totalFreeStorage int64

	stats, err := collectionsStats(ctx, db.r.D.Driver, db.name, list, params.Refresh)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	totalDocs += stats.countDocuments
	totalSizeTables += stats.sizeTables
	totalSizeIndexes += stats.sizeIndexes
	totalFreeStorage += stats.sizeFreeStorage

	return &backends.DatabaseStatsResult{
		CountDocuments:  totalDocs,
		SizeTotal:       totalSizeTables,
		SizeIndexes:     totalSizeIndexes,
		SizeCollections: totalSizeTables,
		SizeFreeStorage: totalFreeStorage,
	}, nil
}

// check interfaces
var (
	_ backends.Database = (*database)(nil)
)
