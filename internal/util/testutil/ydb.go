package testutil

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/FerretDB/FerretDB/internal/util/testutil/testtb"
	"github.com/stretchr/testify/require"
)

// TestBaseYDBURI returns base YDB URI.
// Base URI may be empty.
func TestBaseYDBURI(tb testtb.TB, ctx context.Context, baseURI string) string {
	tb.Helper()

	if testing.Short() {
		tb.Skip("skipping in -short mode")
	}

	if baseURI == "" {
		baseURI = "grpc://127.0.0.1:2136/local"
	}

	return baseURI
}

// TestYDBURI returns YDB URI with test-specific database.
// It will be created before test and dropped after unless test fails.
func TestYDBURI(tb testtb.TB, ctx context.Context, baseURI string) string {
	tb.Helper()

	if testing.Short() {
		tb.Skip("skipping in -short mode")
	}

	u, err := url.Parse(baseURI)
	require.NoError(tb, err)

	require.True(tb, u.Opaque == "")

	name := DirectoryName(tb)
	fullName := filepath.Join(u.Path, name)

	return fullName
}
