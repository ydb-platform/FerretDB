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

	base := testutil.TestBaseYDBURI(t, ctx, "")
	r, err := NewRegistry(base, 100, testutil.Logger(t), sp)
	require.NoError(t, err)

	u := testutil.TestYDBURI(t, ctx, base)

	err = r.D.Driver.Scheme().MakeDirectory(ctx, u)
	if err != nil {
		fmt.Printf("failed to make directory: %v", err)
	}

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Keeping database %s for debugging.", u)
			return
		}

		err = sugar.RemoveRecursive(ctx, r.D.Driver, u)
		if err != nil {
			fmt.Printf("failed to remove dirs recursively: %v", err)
		}
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
