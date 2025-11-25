package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollectionDeepCopy tests the deepCopy method of Collection
func TestCollectionDeepCopy(t *testing.T) {
	t.Parallel()

	t.Run("nil collection", func(t *testing.T) {
		t.Parallel()
		var c *Collection
		result := c.deepCopy()
		assert.Nil(t, result)
	})

	t.Run("empty collection", func(t *testing.T) {
		t.Parallel()
		c := &Collection{}
		result := c.deepCopy()
		require.NotNil(t, result)
		assert.Equal(t, "", result.Name)
		assert.Equal(t, "", result.TableName)
		assert.Empty(t, result.Indexes) // deepCopy creates empty slice, not nil
	})

	t.Run("collection with basic fields", func(t *testing.T) {
		t.Parallel()
		c := &Collection{
			Name:      "test_collection",
			TableName: "test_table",
		}
		result := c.deepCopy()
		require.NotNil(t, result)
		assert.Equal(t, "test_collection", result.Name)
		assert.Equal(t, "test_table", result.TableName)

		// Ensure it's a deep copy - modifying original shouldn't affect copy
		c.Name = "modified"
		assert.Equal(t, "test_collection", result.Name)
	})

	t.Run("collection with indexes", func(t *testing.T) {
		t.Parallel()
		c := &Collection{
			Name:      "test_collection",
			TableName: "test_table",
			Indexes: Indexes{
				{
					Name:          "idx1",
					SanitizedName: "idx1_sanitized",
					Ready:         true,
					Unique:        false,
					Key: []IndexKeyPair{
						{Field: "field1", Descending: false},
					},
				},
			},
		}
		result := c.deepCopy()
		require.NotNil(t, result)
		assert.Len(t, result.Indexes, 1)
		assert.Equal(t, "idx1", result.Indexes[0].Name)

		// Deep copy check - modifying original shouldn't affect copy
		c.Indexes[0].Name = "modified"
		assert.Equal(t, "idx1", result.Indexes[0].Name)
	})

	t.Run("collection with settings", func(t *testing.T) {
		t.Parallel()
		c := &Collection{
			Name:      "test_collection",
			TableName: "test_table",
			Settings: Settings{
				CappedSize:      1024,
				CappedDocuments: 100,
			},
		}
		result := c.deepCopy()
		require.NotNil(t, result)
		assert.Equal(t, int64(1024), result.Settings.CappedSize)
		assert.Equal(t, int64(100), result.Settings.CappedDocuments)

		// Deep copy check
		c.Settings.CappedSize = 2048
		assert.Equal(t, int64(1024), result.Settings.CappedSize)
	})

	t.Run("collection with all fields", func(t *testing.T) {
		t.Parallel()
		c := &Collection{
			Name:      "test_collection",
			TableName: "test_table",
			Indexes: Indexes{
				{
					Name:          "idx1",
					SanitizedName: "idx1_sanitized",
					Ready:         true,
					Unique:        true,
					Key: []IndexKeyPair{
						{Field: "field1", Descending: false},
						{Field: "field2", Descending: true},
					},
				},
			},
			Settings: Settings{
				CappedSize:      1024,
				CappedDocuments: 100,
			},
		}
		result := c.deepCopy()
		require.NotNil(t, result)
		assert.Equal(t, "test_collection", result.Name)
		assert.Equal(t, "test_table", result.TableName)
		assert.Len(t, result.Indexes, 1)
		assert.Len(t, result.Indexes[0].Key, 2)
		assert.Equal(t, int64(1024), result.Settings.CappedSize)
	})
}

// TestSettingsDeepCopy tests the deepCopy method of Settings
func TestSettingsDeepCopy(t *testing.T) {
	t.Parallel()

	t.Run("nil settings", func(t *testing.T) {
		t.Parallel()
		var s *Settings
		result := s.deepCopy()
		assert.Nil(t, result)
	})

	t.Run("empty settings", func(t *testing.T) {
		t.Parallel()
		s := &Settings{}
		result := s.deepCopy()
		require.NotNil(t, result)
		assert.Equal(t, int64(0), result.CappedSize)
		assert.Equal(t, int64(0), result.CappedDocuments)
	})

	t.Run("settings with values", func(t *testing.T) {
		t.Parallel()
		s := &Settings{
			CappedSize:      1024,
			CappedDocuments: 100,
		}
		result := s.deepCopy()
		require.NotNil(t, result)
		assert.Equal(t, int64(1024), result.CappedSize)
		assert.Equal(t, int64(100), result.CappedDocuments)

		// Deep copy check
		s.CappedSize = 2048
		assert.Equal(t, int64(1024), result.CappedSize)
	})

	t.Run("settings with negative values", func(t *testing.T) {
		t.Parallel()
		s := &Settings{
			CappedSize:      -1,
			CappedDocuments: -1,
		}
		result := s.deepCopy()
		require.NotNil(t, result)
		assert.Equal(t, int64(-1), result.CappedSize)
		assert.Equal(t, int64(-1), result.CappedDocuments)
	})

	t.Run("settings with large values", func(t *testing.T) {
		t.Parallel()
		s := &Settings{
			CappedSize:      9223372036854775807, // max int64
			CappedDocuments: 9223372036854775807,
		}
		result := s.deepCopy()
		require.NotNil(t, result)
		assert.Equal(t, int64(9223372036854775807), result.CappedSize)
		assert.Equal(t, int64(9223372036854775807), result.CappedDocuments)
	})
}

// TestCollectionCapped tests the Capped method
func TestCollectionCapped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		cappedSize     int64
		cappedDocs     int64
		expectedCapped bool
	}{
		{
			name:           "not capped - zero size",
			cappedSize:     0,
			cappedDocs:     0,
			expectedCapped: false,
		},
		{
			name:           "capped - positive size",
			cappedSize:     1024,
			cappedDocs:     0,
			expectedCapped: true,
		},
		{
			name:           "capped - both positive",
			cappedSize:     1024,
			cappedDocs:     100,
			expectedCapped: true,
		},
		{
			name:           "not capped - negative size",
			cappedSize:     -1,
			cappedDocs:     100,
			expectedCapped: false,
		},
		{
			name:           "capped - only docs set (but size is what matters)",
			cappedSize:     0,
			cappedDocs:     100,
			expectedCapped: false,
		},
		{
			name:           "capped - size 1",
			cappedSize:     1,
			cappedDocs:     0,
			expectedCapped: true,
		},
		{
			name:           "capped - large size",
			cappedSize:     9223372036854775807,
			cappedDocs:     0,
			expectedCapped: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &Collection{
				Settings: Settings{
					CappedSize:      tt.cappedSize,
					CappedDocuments: tt.cappedDocs,
				},
			}
			result := c.Capped()
			assert.Equal(t, tt.expectedCapped, result)
		})
	}
}

// TestBuildPrimaryKeyColumns tests the BuildPrimaryKeyColumns function
func TestBuildPrimaryKeyColumns(t *testing.T) {
	t.Parallel()

	columns := BuildPrimaryKeyColumns()

	require.NotNil(t, columns)
	require.NotEmpty(t, columns)

	// First column should be id_hash
	assert.Equal(t, IdHashColumn, columns[0].Name)
	assert.NotNil(t, columns[0].Type)

	// Should have columns for all ColumnOrder types
	expectedCount := 1 + len(ColumnOrder) // id_hash + one for each column type
	assert.Equal(t, expectedCount, len(columns))

	// Check that all column names are unique
	names := make(map[string]bool)
	for _, col := range columns {
		assert.False(t, names[col.Name], "Duplicate column name: %s", col.Name)
		names[col.Name] = true
		assert.NotEmpty(t, col.Name)
		assert.NotNil(t, col.Type)
	}
}

// TestBuildPrimaryKeyColumnsConsistency tests that BuildPrimaryKeyColumns returns consistent results
func TestBuildPrimaryKeyColumnsConsistency(t *testing.T) {
	t.Parallel()

	columns1 := BuildPrimaryKeyColumns()
	columns2 := BuildPrimaryKeyColumns()

	require.Equal(t, len(columns1), len(columns2))

	for i := range columns1 {
		assert.Equal(t, columns1[i].Name, columns2[i].Name)
		assert.Equal(t, columns1[i].Type, columns2[i].Type)
	}
}

// TestBuildPrimaryKeyColumnsOrder tests that columns follow expected order
func TestBuildPrimaryKeyColumnsOrder(t *testing.T) {
	t.Parallel()

	columns := BuildPrimaryKeyColumns()

	// First column must be id_hash
	require.Greater(t, len(columns), 0)
	assert.Equal(t, IdHashColumn, columns[0].Name)

	// Remaining columns should follow ColumnOrder
	for i, suffix := range ColumnOrder {
		expectedName := IdMongoField + "_" + string(suffix)
		// i+1 because id_hash is at index 0
		assert.Equal(t, expectedName, columns[i+1].Name, "Column at index %d should be %s", i+1, expectedName)
	}
}

// TestMetadataConstants tests metadata-specific constants
func TestMetadataConstants(t *testing.T) {
	t.Parallel()

	t.Run("table names", func(t *testing.T) {
		t.Parallel()
		assert.NotEmpty(t, metadataTableName)
	})

	t.Run("column names", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "_jsonb", DefaultColumn)
		assert.Equal(t, "id", DefaultIdColumn)
		assert.Equal(t, "id_hash", IdHashColumn)
		assert.Equal(t, "_id", IdMongoField)
		assert.NotEmpty(t, RecordIDColumn)
	})
}

// TestCollectionCreation tests creating Collection instances
func TestCollectionCreation(t *testing.T) {
	t.Parallel()

	t.Run("zero value collection", func(t *testing.T) {
		t.Parallel()
		c := Collection{}
		assert.Equal(t, "", c.Name)
		assert.Equal(t, "", c.TableName)
		assert.Nil(t, c.Indexes)
		assert.False(t, c.Capped())
	})

	t.Run("collection with name only", func(t *testing.T) {
		t.Parallel()
		c := Collection{Name: "test"}
		assert.Equal(t, "test", c.Name)
		assert.False(t, c.Capped())
	})
}
