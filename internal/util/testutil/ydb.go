package testutil

import (
	"context"
	"net/url"
	"testing"

	"github.com/FerretDB/FerretDB/internal/util/testutil/testtb"
	"github.com/stretchr/testify/require"
)

// TestPostgreSQLURI returns PostgreSQL URI with test-specific database.
// It will be created before test and dropped after unless test fails.
//
// Base URI may be empty.
func TestYDBURI(tb testtb.TB, ctx context.Context, baseURI string) string {
	tb.Helper()

	if testing.Short() {
		tb.Skip("skipping in -short mode")
	}

	if baseURI == "" {
		baseURI = "grpc://127.0.0.1:2136/local"
	}

	u, err := url.Parse(baseURI)
	require.NoError(tb, err)

	require.True(tb, u.Opaque == "")

	name := DirectoryName(tb)
	u.Path = name
	res := u.String()

	tb.Cleanup(func() {
		if tb.Failed() {
			tb.Logf("Keeping database %s (%s) for debugging.", name, res)
			return
		}
		require.NoError(tb, err)
	})

	return res
}
