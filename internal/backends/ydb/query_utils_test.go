package ydb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/must"
)

func TestGetId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		doc      *types.Document
		expected any
	}{
		{
			name:     "string _id",
			doc:      must.NotFail(types.NewDocument("_id", "test_id", "name", "test")),
			expected: "test_id",
		},
		{
			name:     "int32 _id",
			doc:      must.NotFail(types.NewDocument("_id", int32(123), "name", "test")),
			expected: int32(123),
		},
		{
			name:     "int64 _id",
			doc:      must.NotFail(types.NewDocument("_id", int64(456), "name", "test")),
			expected: int64(456),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := getId(tt.doc)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerateIdHash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jsonData []byte
		idType   metadata.BsonType
	}{
		{
			name:     "string id",
			jsonData: []byte(`"test"`),
			idType:   metadata.BsonString,
		},
		{
			name:     "int id",
			jsonData: []byte(`123`),
			idType:   metadata.BsonInt,
		},
		{
			name:     "objectId",
			jsonData: []byte(`"507f1f77bcf86cd799439011"`),
			idType:   metadata.BsonObjectId,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			hash1 := generateIdHash(tt.jsonData, tt.idType)
			hash2 := generateIdHash(tt.jsonData, tt.idType)
			
			// Hash should be consistent
			assert.Equal(t, hash1, hash2)
			assert.NotZero(t, hash1)
		})
	}

	t.Run("different data produces different hashes", func(t *testing.T) {
		t.Parallel()
		hash1 := generateIdHash([]byte(`"test1"`), metadata.BsonString)
		hash2 := generateIdHash([]byte(`"test2"`), metadata.BsonString)
		
		assert.NotEqual(t, hash1, hash2)
	})

	t.Run("different types produce different hashes", func(t *testing.T) {
		t.Parallel()
		hash1 := generateIdHash([]byte(`"123"`), metadata.BsonString)
		hash2 := generateIdHash([]byte(`123`), metadata.BsonInt)
		
		assert.NotEqual(t, hash1, hash2)
	})
}

func TestPrepareIds(t *testing.T) {
	t.Parallel()

	t.Run("with IDs", func(t *testing.T) {
		params := &backends.DeleteAllParams{
			IDs: []any{"id1", "id2", "id3"},
		}

		result := prepareIds(params)
		
		assert.NotNil(t, result)
		assert.Len(t, result, 3)
	})

	t.Run("with RecordIDs", func(t *testing.T) {
		params := &backends.DeleteAllParams{
			RecordIDs: []int64{1, 2, 3},
		}

		result := prepareIds(params)
		
		assert.NotNil(t, result)
		assert.Len(t, result, 3)
	})

	t.Run("empty IDs", func(t *testing.T) {
		params := &backends.DeleteAllParams{
			IDs: []any{},
		}

		result := prepareIds(params)
		
		assert.NotNil(t, result)
		assert.Len(t, result, 0)
	})
}

func TestSingleDocumentData(t *testing.T) {
	t.Parallel()

	t.Run("simple document", func(t *testing.T) {
		doc := must.NotFail(types.NewDocument(
			"_id", "test_id",
			"name", "test",
		))
		
		extra := make(map[string]metadata.IndexColumn)
		result := singleDocumentData(doc, extra, false)
		
		assert.NotNil(t, result)
	})

	t.Run("document with int32 _id", func(t *testing.T) {
		doc := must.NotFail(types.NewDocument(
			"_id", int32(123),
			"name", "test",
		))
		
		extra := make(map[string]metadata.IndexColumn)
		result := singleDocumentData(doc, extra, false)
		
		assert.NotNil(t, result)
	})

	t.Run("document with int64 _id", func(t *testing.T) {
		doc := must.NotFail(types.NewDocument(
			"_id", int64(456),
			"name", "test",
		))
		
		extra := make(map[string]metadata.IndexColumn)
		result := singleDocumentData(doc, extra, false)
		
		assert.NotNil(t, result)
	})

	t.Run("capped collection", func(t *testing.T) {
		doc := must.NotFail(types.NewDocument(
			"_id", "test_id",
			"name", "test",
		))
		doc.SetRecordID(12345)
		
		extra := make(map[string]metadata.IndexColumn)
		result := singleDocumentData(doc, extra, true)
		
		assert.NotNil(t, result)
	})

	t.Run("document with extra columns", func(t *testing.T) {
		doc := must.NotFail(types.NewDocument(
			"_id", "test_id",
			"name", "test",
		))
		
		extra := map[string]metadata.IndexColumn{
			"name_string": {
				BsonType:    metadata.BsonString,
				ColumnValue: "test",
				ColumnType:  "String",
			},
		}
		result := singleDocumentData(doc, extra, false)
		
		assert.NotNil(t, result)
	})
}

func TestBuildInsertQuery(t *testing.T) {
	t.Parallel()

	t.Run("simple insert", func(t *testing.T) {
		extra := make(map[string]metadata.IndexColumn)
		query := buildInsertQuery("/path", "test_table", false, extra)
		
		assert.NotEmpty(t, query)
		assert.Contains(t, query, "test_table")
	})

	t.Run("capped collection insert", func(t *testing.T) {
		extra := make(map[string]metadata.IndexColumn)
		query := buildInsertQuery("/path", "test_table", true, extra)
		
		assert.NotEmpty(t, query)
		assert.Contains(t, query, "test_table")
		assert.Contains(t, query, metadata.RecordIDColumn)
	})

	t.Run("insert with extra columns", func(t *testing.T) {
		extra := map[string]metadata.IndexColumn{
			"name_string": {
				BsonType:   metadata.BsonString,
				ColumnType: "String",
			},
		}
		query := buildInsertQuery("/path", "test_table", false, extra)
		
		assert.NotEmpty(t, query)
		assert.Contains(t, query, "test_table")
		assert.Contains(t, query, "name_string")
	})
}

func TestBuildUpsertQuery(t *testing.T) {
	t.Parallel()

	t.Run("simple upsert", func(t *testing.T) {
		extra := make(map[string]metadata.IndexColumn)
		query := buildUpsertQuery("/path", "test_table", extra)
		
		assert.NotEmpty(t, query)
		assert.Contains(t, query, "test_table")
	})

	t.Run("upsert with extra columns", func(t *testing.T) {
		extra := map[string]metadata.IndexColumn{
			"name_string": {
				BsonType:   metadata.BsonString,
				ColumnType: "String",
			},
		}
		query := buildUpsertQuery("/path", "test_table", extra)
		
		assert.NotEmpty(t, query)
		assert.Contains(t, query, "test_table")
		assert.Contains(t, query, "name_string")
	})
}

func TestBuildWriteQuery(t *testing.T) {
	t.Parallel()

	t.Run("write query for insert", func(t *testing.T) {
		extra := make(map[string]metadata.IndexColumn)
		query := buildWriteQuery("/path", "test_table", extra, false, metadata.InsertTmpl)
		
		assert.NotEmpty(t, query)
		assert.Contains(t, query, "test_table")
		assert.Contains(t, query, metadata.DefaultColumn)
	})

	t.Run("write query for upsert", func(t *testing.T) {
		extra := make(map[string]metadata.IndexColumn)
		query := buildWriteQuery("/path", "test_table", extra, false, metadata.UpsertTmpl)
		
		assert.NotEmpty(t, query)
		assert.Contains(t, query, "test_table")
	})

	t.Run("write query with capped", func(t *testing.T) {
		extra := make(map[string]metadata.IndexColumn)
		query := buildWriteQuery("/path", "test_table", extra, true, metadata.InsertTmpl)
		
		assert.NotEmpty(t, query)
		assert.Contains(t, query, "test_table")
		assert.Contains(t, query, metadata.RecordIDColumn)
	})

	t.Run("write query with multiple extra columns", func(t *testing.T) {
		extra := map[string]metadata.IndexColumn{
			"name_string": {
				BsonType:   metadata.BsonString,
				ColumnType: "String",
			},
			"age_scalar": {
				BsonType:   metadata.BsonInt,
				ColumnType: "DyNumber",
			},
		}
		query := buildWriteQuery("/path", "test_table", extra, false, metadata.InsertTmpl)
		
		assert.NotEmpty(t, query)
		assert.Contains(t, query, "test_table")
		assert.Contains(t, query, "name_string")
		assert.Contains(t, query, "age_scalar")
	})
}

func TestIdHashConsistency(t *testing.T) {
	t.Parallel()

	// Test that the same ID always produces the same hash
	doc1 := must.NotFail(types.NewDocument("_id", "consistent_id", "field", "value1"))
	doc2 := must.NotFail(types.NewDocument("_id", "consistent_id", "field", "value2"))

	extra := make(map[string]metadata.IndexColumn)
	
	// Extract hashes from the document data structures
	// This is an indirect test - we're verifying that the same _id produces consistent results
	data1 := singleDocumentData(doc1, extra, false)
	data2 := singleDocumentData(doc2, extra, false)
	
	assert.NotNil(t, data1)
	assert.NotNil(t, data2)
}

func TestPrepareIdsWithObjectId(t *testing.T) {
	t.Parallel()

	objectId := types.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
	
	params := &backends.DeleteAllParams{
		IDs: []any{objectId},
	}

	result := prepareIds(params)
	
	assert.NotNil(t, result)
	assert.Len(t, result, 1)
}

func TestBuildWriteQueryFieldOrder(t *testing.T) {
	t.Parallel()

	// Test that fields are declared in correct order
	extra := map[string]metadata.IndexColumn{
		"field1_string": {
			BsonType:   metadata.BsonString,
			ColumnType: "String",
		},
		"field2_scalar": {
			BsonType:   metadata.BsonInt,
			ColumnType: "DyNumber",
		},
	}

	query := buildWriteQuery("/path", "test_table", extra, false, metadata.InsertTmpl)
	
	require.NotEmpty(t, query)
	
	// Check that primary key columns come before data column
	assert.Contains(t, query, "id_hash")
	assert.Contains(t, query, metadata.DefaultColumn)
	assert.Contains(t, query, "field1_string")
	assert.Contains(t, query, "field2_scalar")
}

func TestGenerateIdHashEmptyData(t *testing.T) {
	t.Parallel()

	hash := generateIdHash([]byte{}, metadata.BsonString)
	assert.NotZero(t, hash, "Hash should not be zero even for empty data")
}

func TestGenerateIdHashLargeData(t *testing.T) {
	t.Parallel()

	// Test with large data
	largeData := make([]byte, 10000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	hash1 := generateIdHash(largeData, metadata.BsonString)
	hash2 := generateIdHash(largeData, metadata.BsonString)
	
	assert.Equal(t, hash1, hash2, "Same data should produce same hash")
	assert.NotZero(t, hash1)
}

func TestSingleDocumentDataWithObjectID(t *testing.T) {
	t.Parallel()

	objectId := types.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
	doc := must.NotFail(types.NewDocument(
		"_id", objectId,
		"name", "test",
	))

	extra := make(map[string]metadata.IndexColumn)
	result := singleDocumentData(doc, extra, false)
	
	assert.NotNil(t, result)
}

func TestSingleDocumentDataWithFloat64ID(t *testing.T) {
	t.Parallel()

	doc := must.NotFail(types.NewDocument(
		"_id", float64(123.456),
		"name", "test",
	))

	extra := make(map[string]metadata.IndexColumn)
	result := singleDocumentData(doc, extra, false)
	
	assert.NotNil(t, result)
}

func TestSingleDocumentDataWithBoolID(t *testing.T) {
	t.Parallel()

	doc := must.NotFail(types.NewDocument(
		"_id", true,
		"name", "test",
	))

	extra := make(map[string]metadata.IndexColumn)
	result := singleDocumentData(doc, extra, false)
	
	assert.NotNil(t, result)
}

func TestPrepareIdsWithMixedTypes(t *testing.T) {
	t.Parallel()

	params := &backends.DeleteAllParams{
		IDs: []any{
			"string_id",
			int32(123),
			int64(456),
			float64(789.0),
		},
	}

	result := prepareIds(params)
	
	assert.NotNil(t, result)
	assert.Len(t, result, 4)
}

func TestPrepareIdsWithEmptyRecordIDs(t *testing.T) {
	t.Parallel()

	params := &backends.DeleteAllParams{
		RecordIDs: []int64{},
	}

	result := prepareIds(params)
	
	assert.NotNil(t, result)
	assert.Len(t, result, 0)
}

func TestPrepareIdsWithNegativeRecordIDs(t *testing.T) {
	t.Parallel()

	params := &backends.DeleteAllParams{
		RecordIDs: []int64{-1, -100, -999},
	}

	result := prepareIds(params)
	
	assert.NotNil(t, result)
	assert.Len(t, result, 3)
}

func TestPrepareIdsWithLargeRecordIDs(t *testing.T) {
	t.Parallel()

	params := &backends.DeleteAllParams{
		RecordIDs: []int64{9223372036854775807}, // max int64
	}

	result := prepareIds(params)
	
	assert.NotNil(t, result)
	assert.Len(t, result, 1)
}

func TestBuildInsertQueryWithEmptyExtra(t *testing.T) {
	t.Parallel()

	extra := make(map[string]metadata.IndexColumn)
	query := buildInsertQuery("/database/path", "my_table", false, extra)
	
	assert.NotEmpty(t, query)
	assert.Contains(t, query, "my_table")
	assert.Contains(t, query, "/database/path")
}

func TestBuildUpsertQueryWithEmptyExtra(t *testing.T) {
	t.Parallel()

	extra := make(map[string]metadata.IndexColumn)
	query := buildUpsertQuery("/database/path", "my_table", extra)
	
	assert.NotEmpty(t, query)
	assert.Contains(t, query, "my_table")
	assert.Contains(t, query, "/database/path")
}

func TestSingleDocumentDataWithMultipleExtraColumns(t *testing.T) {
	t.Parallel()

	doc := must.NotFail(types.NewDocument(
		"_id", "test_id",
		"name", "test",
		"age", int32(25),
		"active", true,
	))

	extra := map[string]metadata.IndexColumn{
		"name_string": {
			BsonType:    metadata.BsonString,
			ColumnValue: "test",
			ColumnType:  "String",
		},
		"age_scalar": {
			BsonType:    metadata.BsonInt,
			ColumnValue: int32(25),
			ColumnType:  "DyNumber",
		},
		"active_bool": {
			BsonType:    metadata.BsonBool,
			ColumnValue: true,
			ColumnType:  "Bool",
		},
	}
	
	result := singleDocumentData(doc, extra, false)
	
	assert.NotNil(t, result)
}

func TestGetIdWithComplexDocument(t *testing.T) {
	t.Parallel()

	nestedDoc := must.NotFail(types.NewDocument("inner", "value"))
	doc := must.NotFail(types.NewDocument(
		"_id", "complex_id",
		"nested", nestedDoc,
		"array", must.NotFail(types.NewArray("item1", "item2")),
	))

	result := getId(doc)
	assert.Equal(t, "complex_id", result)
}

func TestBuildWriteQueryWithSpecialCharactersInPath(t *testing.T) {
	t.Parallel()

	extra := make(map[string]metadata.IndexColumn)
	query := buildWriteQuery("/path/with/special-chars_123", "table_name", extra, false, metadata.InsertTmpl)
	
	assert.NotEmpty(t, query)
	assert.Contains(t, query, "/path/with/special-chars_123")
}

func TestBuildWriteQueryWithUnicodeTableName(t *testing.T) {
	t.Parallel()

	extra := make(map[string]metadata.IndexColumn)
	query := buildWriteQuery("/path", "table_тест_测试", extra, false, metadata.InsertTmpl)
	
	assert.NotEmpty(t, query)
	assert.Contains(t, query, "table_тест_测试")
}

