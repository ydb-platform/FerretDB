package metadata

import (
	"context"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/clientconn/conninfo"
	"github.com/FerretDB/FerretDB/internal/util/state"
	"github.com/FerretDB/FerretDB/internal/util/testutil"
	"github.com/FerretDB/FerretDB/internal/util/testutil/teststress"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"sync/atomic"
	"testing"
)

// createDatabase creates a new provider and registry required for creating a database and
// returns registry, db and created database name.
func createDatabase(t *testing.T, ctx context.Context) (*Registry, string) {
	t.Helper()

	sp, err := state.NewProvider("")
	require.NoError(t, err)

	base := testutil.TestBaseYdbURI(t, "")
	r, err := NewRegistry(base, 100, testutil.Logger(t), sp)
	require.NoError(t, err)

	u := testutil.TestYdbDirectory(t, base)

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Keeping database %s for debugging.", u)
			return
		}

		err = sugar.RemoveRecursive(ctx, r.D.Driver, u)
		require.NoError(t, err)

		r.Close()
	})

	return r, u
}

// testCollection creates, tests, and drops a unique collection in the existing database.
func testCollection(t *testing.T, ctx context.Context, r *Registry, dbName, collectionName string) {
	t.Helper()

	c, err := r.CollectionGet(ctx, dbName, collectionName)
	require.NoError(t, err)
	require.Nil(t, c)

	created, err := r.CollectionCreate(ctx, &CollectionCreateParams{DBName: dbName, Name: collectionName})
	require.NoError(t, err)
	require.True(t, created)

	created, err = r.CollectionCreate(ctx, &CollectionCreateParams{DBName: dbName, Name: collectionName})
	require.NoError(t, err)
	require.False(t, created)

	c, err = r.CollectionGet(ctx, dbName, collectionName)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, collectionName, c.Name)

	list, err := r.CollectionList(ctx, dbName)
	require.NoError(t, err)
	require.Contains(t, list, c)

	dropped, err := r.CollectionDrop(ctx, dbName, collectionName)
	require.NoError(t, err)
	require.True(t, dropped)

	dropped, err = r.CollectionDrop(ctx, dbName, collectionName)
	require.NoError(t, err)
	require.False(t, dropped)

	c, err = r.CollectionGet(ctx, dbName, collectionName)
	require.NoError(t, err)
	require.Nil(t, c)
}

func TestCreateDropStress(t *testing.T) {
	ctx := conninfo.Ctx(testutil.Ctx(t), conninfo.New())
	r, dbName := createDatabase(t, ctx)

	var i atomic.Int32

	teststress.Stress(t, func(ready chan<- struct{}, start <-chan struct{}) {
		collectionName := fmt.Sprintf("collection_%03d", i.Add(1))

		ready <- struct{}{}
		<-start

		testCollection(t, ctx, r, dbName, collectionName)
	})
}

func TestCreateSameStress(t *testing.T) {
	ctx := conninfo.Ctx(testutil.Ctx(t), conninfo.New())
	r, dbName := createDatabase(t, ctx)
	collectionName := testutil.CollectionName(t)

	var createdTotal atomic.Int32

	teststress.Stress(t, func(ready chan<- struct{}, start <-chan struct{}) {

		ready <- struct{}{}
		<-start

		created, err := r.CollectionCreate(ctx, &CollectionCreateParams{DBName: dbName, Name: collectionName})
		require.NoError(t, err)
		if created {
			createdTotal.Add(1)
		}

		created, err = r.CollectionCreate(ctx, &CollectionCreateParams{DBName: dbName, Name: collectionName})
		require.NoError(t, err)
		require.False(t, created)

		c, err := r.CollectionGet(ctx, dbName, collectionName)
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, collectionName, c.Name)

		list, err := r.CollectionList(ctx, dbName)
		require.NoError(t, err)
		require.Contains(t, list, c)
	})

	require.Equal(t, int32(1), createdTotal.Load())
}

func TestDropSameStress(t *testing.T) {
	ctx := conninfo.Ctx(testutil.Ctx(t), conninfo.New())
	r, dbName := createDatabase(t, ctx)
	collectionName := testutil.CollectionName(t)

	created, err := r.CollectionCreate(ctx, &CollectionCreateParams{DBName: dbName, Name: collectionName})
	require.NoError(t, err)
	require.True(t, created)

	var droppedTotal atomic.Int32

	teststress.Stress(t, func(ready chan<- struct{}, start <-chan struct{}) {
		ready <- struct{}{}
		<-start

		dropped, err := r.CollectionDrop(ctx, dbName, collectionName)
		require.NoError(t, err)
		if dropped {
			droppedTotal.Add(1)
		}
	})

	require.Equal(t, int32(1), droppedTotal.Load())
}

func TestCreateDropSameStress(t *testing.T) {
	ctx := conninfo.Ctx(testutil.Ctx(t), conninfo.New())
	r, dbName := createDatabase(t, ctx)
	collectionName := testutil.CollectionName(t)

	var i, createdTotal, droppedTotal atomic.Int32

	teststress.Stress(t, func(ready chan<- struct{}, start <-chan struct{}) {
		id := i.Add(1)

		ready <- struct{}{}
		<-start

		if id%2 == 0 {
			created, err := r.CollectionCreate(ctx, &CollectionCreateParams{DBName: dbName, Name: collectionName})
			require.NoError(t, err)
			if created {
				createdTotal.Add(1)
			}
		} else {
			dropped, err := r.CollectionDrop(ctx, dbName, collectionName)
			require.NoError(t, err)
			if dropped {
				droppedTotal.Add(1)
			}
		}
	})

	require.Less(t, int32(1), createdTotal.Load())
	require.Less(t, int32(1), droppedTotal.Load())
}

func TestRenameCollection(t *testing.T) {
	t.Parallel()

	ctx := conninfo.Ctx(testutil.Ctx(t), conninfo.New())
	r, dbName := createDatabase(t, ctx)

	oldCollectionName := testutil.CollectionName(t)
	newCollectionName := "new"

	created, err := r.CollectionCreate(ctx, &CollectionCreateParams{DBName: dbName, Name: oldCollectionName})
	require.NoError(t, err)
	require.True(t, created)

	oldCollection, err := r.CollectionGet(ctx, dbName, oldCollectionName)
	require.NoError(t, err)

	t.Run("CollectionRename", func(t *testing.T) {
		var renamed bool
		renamed, err = r.CollectionRename(ctx, dbName, oldCollectionName, newCollectionName)
		require.NoError(t, err)
		require.True(t, renamed)
	})

	t.Run("CheckCollectionRenamed", func(t *testing.T) {
		_, err = r.LoadMetadata(ctx, dbName)
		require.NoError(t, err)

		expected := &Collection{
			Name:      newCollectionName,
			UUID:      oldCollection.UUID,
			TableName: oldCollection.TableName,
			Indexes:   oldCollection.Indexes,
		}

		actual, err := r.CollectionGet(ctx, dbName, newCollectionName)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	})
}
