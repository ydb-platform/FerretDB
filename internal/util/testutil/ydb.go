package testutil

import (
	"github.com/FerretDB/FerretDB/internal/util/testutil/testtb"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
)

// TestBaseYdbURI returns base YDB URI.
// Base URI may be empty.
func TestBaseYdbURI(tb testtb.TB, baseURI string) string {
	tb.Helper()

	if testing.Short() {
		tb.Skip("skipping in -short mode")
	}

	if baseURI == "" {
		baseURI = "grpc://127.0.0.1:2136/local"
	}

	return baseURI
}

// TestYdbDirectory returns YDB URI with test-specific database.
// It will be created before test and dropped after unless test fails.
func TestYdbDirectory(tb testtb.TB, baseURI string) string {
	tb.Helper()

	if testing.Short() {
		tb.Skip("skipping in -short mode")
	}

	u, err := url.Parse(baseURI)
	require.NoError(tb, err)

	require.True(tb, u.Path != "")
	require.True(tb, u.Opaque == "")

	name := DirectoryName(tb)

	return name
}
