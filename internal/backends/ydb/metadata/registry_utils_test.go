package metadata

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldSkipDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dbName   string
		expected bool
	}{
		{
			name:     "local database should be skipped",
			dbName:   "local",
			expected: true,
		},
		{
			name:     "empty database should be skipped",
			dbName:   "",
			expected: true,
		},
		{
			name:     "regular database should not be skipped",
			dbName:   "mydb",
			expected: false,
		},
		{
			name:     "admin database should not be skipped",
			dbName:   "admin",
			expected: false,
		},
		{
			name:     "test database should not be skipped",
			dbName:   "test",
			expected: false,
		},
		{
			name:     "database with local prefix should not be skipped",
			dbName:   "localhost",
			expected: false,
		},
		{
			name:     "database with local suffix should not be skipped",
			dbName:   "dblocal",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := shouldSkipDatabase(tt.dbName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFnv32Hash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "simple string",
			input: "test",
		},
		{
			name:  "empty string",
			input: "",
		},
		{
			name:  "long string",
			input: "this is a very long string that should still produce a consistent hash",
		},
		{
			name:  "unicode string",
			input: "привет мир",
		},
		{
			name:  "special characters",
			input: "!@#$%^&*()",
		},
		{
			name:  "numbers",
			input: "1234567890",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			hash1 := fnv32Hash(tt.input)
			hash2 := fnv32Hash(tt.input)

			// Hash should be consistent
			assert.Equal(t, hash1, hash2)

			// Hash should be a uint32 (always fits)
			assert.IsType(t, uint32(0), hash1)
		})
	}

	t.Run("different strings produce different hashes", func(t *testing.T) {
		t.Parallel()
		hash1 := fnv32Hash("string1")
		hash2 := fnv32Hash("string2")

		assert.NotEqual(t, hash1, hash2)
	})

	t.Run("hash is deterministic", func(t *testing.T) {
		t.Parallel()
		input := "consistent"

		// Hash the same string multiple times
		hashes := make([]uint32, 10)
		for i := range hashes {
			hashes[i] = fnv32Hash(input)
		}

		// All hashes should be identical
		for i := 1; i < len(hashes); i++ {
			assert.Equal(t, hashes[0], hashes[i])
		}
	})
}

func TestGenerateIndexName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		originalName  string
		checkContains string
		checkSuffix   string
	}{
		{
			name:          "simple name",
			originalName:  "myindex",
			checkContains: "myindex",
			checkSuffix:   "_idx",
		},
		{
			name:          "name with special characters",
			originalName:  "my-index@name",
			checkContains: "my-index_name", // hyphen is allowed, @ is replaced with _
			checkSuffix:   "_idx",
		},
		{
			name:          "name with spaces",
			originalName:  "my index name",
			checkContains: "my_index_name",
			checkSuffix:   "_idx",
		},
		{
			name:          "empty name",
			originalName:  "",
			checkContains: "_",
			checkSuffix:   "_idx",
		},
		{
			name:          "unicode name",
			originalName:  "индекс",
			checkContains: "_",
			checkSuffix:   "_idx",
		},
		{
			name:          "name with dots",
			originalName:  "my.index.name",
			checkContains: "my.index.name",
			checkSuffix:   "_idx",
		},
		{
			name:          "name with underscores",
			originalName:  "my_index_name",
			checkContains: "my_index_name",
			checkSuffix:   "_idx",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := generateIndexName(tt.originalName)

			// Check that result contains expected parts
			if tt.checkContains != "" {
				assert.Contains(t, result, tt.checkContains)
			}

			// Check that result ends with suffix
			assert.True(t, strings.HasSuffix(result, tt.checkSuffix))

			// Check that result contains hash (8 hex digits)
			assert.Regexp(t, `_[0-9a-f]{8}_idx$`, result)
		})
	}

	t.Run("very long name is truncated", func(t *testing.T) {
		t.Parallel()
		// Create a very long name
		longName := strings.Repeat("a", 300)
		result := generateIndexName(longName)

		// Result should not exceed maxObjectNameLength
		assert.LessOrEqual(t, len(result), maxObjectNameLength)

		// Result should still end with hash and suffix
		assert.Regexp(t, `_[0-9a-f]{8}_idx$`, result)
	})

	t.Run("consistent hash for same name", func(t *testing.T) {
		t.Parallel()
		name := "myindex"

		result1 := generateIndexName(name)
		result2 := generateIndexName(name)

		// Should produce same result
		assert.Equal(t, result1, result2)
	})

	t.Run("different names produce different results", func(t *testing.T) {
		t.Parallel()
		name1 := "index1"
		name2 := "index2"

		result1 := generateIndexName(name1)
		result2 := generateIndexName(name2)

		// Should produce different results (different hashes)
		assert.NotEqual(t, result1, result2)
	})

	t.Run("sanitization replaces unsupported characters", func(t *testing.T) {
		t.Parallel()
		name := "my@index#name$"
		result := generateIndexName(name)

		// @ # $ should be replaced with _
		assert.NotContains(t, result, "@")
		assert.NotContains(t, result, "#")
		assert.NotContains(t, result, "$")
	})

	t.Run("keeps allowed characters", func(t *testing.T) {
		t.Parallel()
		name := "my_index.name-123"
		result := generateIndexName(name)

		// Underscores, dots, hyphens, and numbers should be kept
		assert.Contains(t, result, "my_index.name-123")
	})

	t.Run("result length is reasonable", func(t *testing.T) {
		t.Parallel()
		name := "test"
		result := generateIndexName(name)

		// Should have: name + _ + 8-char hash + _idx
		// For "test": test_12345678_idx (about 17 chars)
		assert.Greater(t, len(result), len(name))
		assert.LessOrEqual(t, len(result), maxObjectNameLength)
	})
}

func TestGenerateIndexNameEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("single character", func(t *testing.T) {
		t.Parallel()
		result := generateIndexName("a")
		assert.NotEmpty(t, result)
		assert.Contains(t, result, "a")
		assert.Regexp(t, `_[0-9a-f]{8}_idx$`, result)
	})

	t.Run("all special characters", func(t *testing.T) {
		t.Parallel()
		result := generateIndexName("!@#$%^&*()")
		assert.NotEmpty(t, result)
		assert.Regexp(t, `_[0-9a-f]{8}_idx$`, result)
	})

	t.Run("newlines and tabs", func(t *testing.T) {
		t.Parallel()
		result := generateIndexName("index\nwith\ttabs")
		assert.NotEmpty(t, result)
		assert.NotContains(t, result, "\n")
		assert.NotContains(t, result, "\t")
	})

	t.Run("name at max length boundary", func(t *testing.T) {
		t.Parallel()
		// Create name that's exactly at the boundary
		maxBase := maxObjectNameLength/2 - len("_12345678_idx")
		name := strings.Repeat("a", maxBase)
		result := generateIndexName(name)

		assert.LessOrEqual(t, len(result), maxObjectNameLength)
	})
}

func TestCollectionCreateParamsCapped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cappedSize int64
		expected   bool
	}{
		{
			name:       "zero size is not capped",
			cappedSize: 0,
			expected:   false,
		},
		{
			name:       "positive size is capped",
			cappedSize: 1024,
			expected:   true,
		},
		{
			name:       "small positive size is capped",
			cappedSize: 1,
			expected:   true,
		},
		{
			name:       "large size is capped",
			cappedSize: 1000000000,
			expected:   true,
		},
		{
			name:       "negative size is not capped",
			cappedSize: -1,
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			params := &CollectionCreateParams{
				CappedSize: tt.cappedSize,
			}
			result := params.Capped()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConstants(t *testing.T) {
	t.Parallel()

	t.Run("defaultBatchSize", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 1000, defaultBatchSize)
		assert.Greater(t, defaultBatchSize, 0)
	})

	t.Run("localDBName", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "local", localDBName)
	})

	t.Run("maxObjectNameLength", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 255, maxObjectNameLength)
		assert.Greater(t, maxObjectNameLength, 0)
	})
}

func TestSelectParams(t *testing.T) {
	t.Parallel()

	t.Run("default values", func(t *testing.T) {
		t.Parallel()
		params := SelectParams{}
		assert.Empty(t, params.Schema)
		assert.Empty(t, params.Table)
		assert.Empty(t, params.Comment)
		assert.False(t, params.Capped)
		assert.False(t, params.OnlyRecordIDs)
	})

	t.Run("with values", func(t *testing.T) {
		t.Parallel()
		params := SelectParams{
			Schema:        "myschema",
			Table:         "mytable",
			Comment:       "test comment",
			Capped:        true,
			OnlyRecordIDs: true,
		}
		assert.Equal(t, "myschema", params.Schema)
		assert.Equal(t, "mytable", params.Table)
		assert.Equal(t, "test comment", params.Comment)
		assert.True(t, params.Capped)
		assert.True(t, params.OnlyRecordIDs)
	})
}
