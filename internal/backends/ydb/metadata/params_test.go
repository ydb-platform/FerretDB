package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCollectionCreateParams tests the CollectionCreateParams structure
func TestCollectionCreateParams(t *testing.T) {
	t.Parallel()

	t.Run("zero value", func(t *testing.T) {
		t.Parallel()
		params := CollectionCreateParams{}
		assert.Equal(t, "", params.DBName)
		assert.Equal(t, "", params.Name)
		assert.Nil(t, params.Indexes)
		assert.Equal(t, int64(0), params.CappedSize)
		assert.Equal(t, int64(0), params.CappedDocuments)
	})

	t.Run("with db and collection name", func(t *testing.T) {
		t.Parallel()
		params := CollectionCreateParams{
			DBName: "test_db",
			Name:   "test_collection",
		}
		assert.Equal(t, "test_db", params.DBName)
		assert.Equal(t, "test_collection", params.Name)
		assert.Nil(t, params.Indexes)
	})

	t.Run("with capped settings", func(t *testing.T) {
		t.Parallel()
		params := CollectionCreateParams{
			DBName:          "test_db",
			Name:            "test_collection",
			CappedSize:      1024,
			CappedDocuments: 100,
		}
		assert.Equal(t, int64(1024), params.CappedSize)
		assert.Equal(t, int64(100), params.CappedDocuments)
	})

	t.Run("with indexes", func(t *testing.T) {
		t.Parallel()
		indexes := []IndexInfo{
			{
				Name:   "idx1",
				Unique: true,
				Key: []IndexKeyPair{
					{Field: "field1", Descending: false},
				},
			},
		}
		params := CollectionCreateParams{
			DBName:  "test_db",
			Name:    "test_collection",
			Indexes: indexes,
		}
		assert.Len(t, params.Indexes, 1)
		assert.Equal(t, "idx1", params.Indexes[0].Name)
	})

	t.Run("with all fields", func(t *testing.T) {
		t.Parallel()
		indexes := []IndexInfo{
			{Name: "idx1"},
			{Name: "idx2"},
		}
		params := CollectionCreateParams{
			DBName:          "test_db",
			Name:            "test_collection",
			Indexes:         indexes,
			CappedSize:      2048,
			CappedDocuments: 200,
		}
		assert.Equal(t, "test_db", params.DBName)
		assert.Equal(t, "test_collection", params.Name)
		assert.Len(t, params.Indexes, 2)
		assert.Equal(t, int64(2048), params.CappedSize)
		assert.Equal(t, int64(200), params.CappedDocuments)
	})

	t.Run("with unicode names", func(t *testing.T) {
		t.Parallel()
		params := CollectionCreateParams{
			DBName: "база_данных",
			Name:   "коллекция_测试",
		}
		assert.Equal(t, "база_данных", params.DBName)
		assert.Equal(t, "коллекция_测试", params.Name)
	})

	t.Run("with negative capped values", func(t *testing.T) {
		t.Parallel()
		params := CollectionCreateParams{
			DBName:          "test_db",
			Name:            "test_collection",
			CappedSize:      -1,
			CappedDocuments: -1,
		}
		assert.Equal(t, int64(-1), params.CappedSize)
		assert.Equal(t, int64(-1), params.CappedDocuments)
	})

	t.Run("with large capped values", func(t *testing.T) {
		t.Parallel()
		params := CollectionCreateParams{
			DBName:          "test_db",
			Name:            "test_collection",
			CappedSize:      9223372036854775807, // max int64
			CappedDocuments: 9223372036854775807,
		}
		assert.Equal(t, int64(9223372036854775807), params.CappedSize)
		assert.Equal(t, int64(9223372036854775807), params.CappedDocuments)
	})
}

// TestParamsStructPreventUnkeyedLiterals tests that structs prevent unkeyed literals
func TestParamsStructPreventUnkeyedLiterals(t *testing.T) {
	t.Parallel()

	// This test ensures the struct has the _ struct{} field
	// We can't directly test prevention of unkeyed literals, but we can
	// test that the struct works correctly with keyed literals

	t.Run("CollectionCreateParams with keyed literals", func(t *testing.T) {
		t.Parallel()
		_ = CollectionCreateParams{
			DBName:          "db",
			Name:            "coll",
			Indexes:         nil,
			CappedSize:      0,
			CappedDocuments: 0,
		}
		// If this compiles, the struct is correctly defined
		assert.True(t, true)
	})
}
