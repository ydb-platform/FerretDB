package ydb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/must"
)

func TestIsSupportedForPushdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		opStr    string
		expected bool
	}{
		{
			name:     "eq operator",
			opStr:    "$eq",
			expected: true,
		},
		{
			name:     "ne operator",
			opStr:    "$ne",
			expected: true,
		},
		{
			name:     "gt operator not supported",
			opStr:    "$gt",
			expected: false,
		},
		{
			name:     "lt operator not supported",
			opStr:    "$lt",
			expected: false,
		},
		{
			name:     "unknown operator",
			opStr:    "$unknown",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := IsSupportedForPushdown(tt.opStr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetCompareOp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       MongoOp
		expected CompareOp
	}{
		{
			name:     "eq to ==",
			op:       FieldOpEq,
			expected: CompareOpEq,
		},
		{
			name:     "ne to !=",
			op:       FieldOpNe,
			expected: CompareOpNe,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := GetCompareOp(tt.op)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsIndexableOp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       MongoOp
		expected bool
	}{
		{
			name:     "eq is indexable",
			op:       FieldOpEq,
			expected: true,
		},
		{
			name:     "ne is not indexable",
			op:       FieldOpNe,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := IsIndexableOp(tt.op)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPrepareSelectClause(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   *metadata.SelectParams
		expected string
	}{
		{
			name:     "nil params",
			params:   nil,
			expected: "SELECT  _jsonb FROM ``",
		},
		{
			name: "simple select",
			params: &metadata.SelectParams{
				Table: "test_table",
			},
			expected: "SELECT  _jsonb FROM `test_table`",
		},
		{
			name: "select with comment",
			params: &metadata.SelectParams{
				Table:   "test_table",
				Comment: "test comment",
			},
			expected: "SELECT /* test comment */ _jsonb FROM `test_table`",
		},
		{
			name: "select with comment containing /* */",
			params: &metadata.SelectParams{
				Table:   "test_table",
				Comment: "test /* inner */ comment",
			},
			expected: "SELECT /* test / * inner * / comment */ _jsonb FROM `test_table`",
		},
		{
			name: "capped collection with only record IDs",
			params: &metadata.SelectParams{
				Table:         "test_table",
				Capped:        true,
				OnlyRecordIDs: true,
			},
			expected: "SELECT  _ferretdb_record_id FROM `test_table`",
		},
		{
			name: "capped collection",
			params: &metadata.SelectParams{
				Table:  "test_table",
				Capped: true,
			},
			expected: "SELECT  _ferretdb_record_id, _jsonb FROM `test_table`",
		},
		{
			name: "empty table name",
			params: &metadata.SelectParams{
				Table: "",
			},
			expected: "SELECT  _jsonb FROM ``",
		},
		{
			name: "table with special characters",
			params: &metadata.SelectParams{
				Table: "test-table_123",
			},
			expected: "SELECT  _jsonb FROM `test-table_123`",
		},
		{
			name: "unicode table name",
			params: &metadata.SelectParams{
				Table: "таблица_测试",
			},
			expected: "SELECT  _jsonb FROM `таблица_测试`",
		},
		{
			name: "comment with only spaces",
			params: &metadata.SelectParams{
				Table:   "test_table",
				Comment: "   ",
			},
			expected: "SELECT /*     */ _jsonb FROM `test_table`",
		},
		{
			name: "long comment",
			params: &metadata.SelectParams{
				Table:   "test_table",
				Comment: "This is a very long comment that contains a lot of text to test how the function handles longer strings in comments",
			},
			expected: "SELECT /* This is a very long comment that contains a lot of text to test how the function handles longer strings in comments */ _jsonb FROM `test_table`",
		},
		{
			name: "comment with multiple /* */ pairs",
			params: &metadata.SelectParams{
				Table:   "test_table",
				Comment: "start /* first */ middle /* second */ end",
			},
			expected: "SELECT /* start / * first * / middle / * second * / end */ _jsonb FROM `test_table`",
		},
		{
			name: "capped with comment",
			params: &metadata.SelectParams{
				Table:   "test_table",
				Comment: "capped test",
				Capped:  true,
			},
			expected: "SELECT /* capped test */ _ferretdb_record_id, _jsonb FROM `test_table`",
		},
		{
			name: "non-capped with onlyRecordIDs should be ignored",
			params: &metadata.SelectParams{
				Table:         "test_table",
				Capped:        false,
				OnlyRecordIDs: true,
			},
			expected: "SELECT  _jsonb FROM `test_table`",
		},
		{
			name: "comment with newlines",
			params: &metadata.SelectParams{
				Table:   "test_table",
				Comment: "line1\nline2\nline3",
			},
			expected: "SELECT /* line1\nline2\nline3 */ _jsonb FROM `test_table`",
		},
		{
			name: "comment with tabs",
			params: &metadata.SelectParams{
				Table:   "test_table",
				Comment: "field1\tfield2\tfield3",
			},
			expected: "SELECT /* field1\tfield2\tfield3 */ _jsonb FROM `test_table`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := prepareSelectClause(tt.params)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildPathToField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "simple field",
			key:      "name",
			expected: "$.name",
		},
		{
			name:     "empty key",
			key:      "",
			expected: `$.""`,
		},
		{
			name:     "key with hyphen",
			key:      "some-key",
			expected: `$."some-key"`,
		},
		{
			name:     "key with spaces",
			key:      "  name  ",
			expected: "$.name",
		},
		{
			name:     "key with multiple hyphens",
			key:      "user-full-name",
			expected: `$."user-full-name"`,
		},
		{
			name:     "key with underscore",
			key:      "user_name",
			expected: "$.user_name",
		},
		{
			name:     "key with numbers",
			key:      "field123",
			expected: "$.field123",
		},
		{
			name:     "key with leading spaces",
			key:      "   field",
			expected: "$.field",
		},
		{
			name:     "key with trailing spaces",
			key:      "field   ",
			expected: "$.field",
		},
		{
			name:     "key with spaces in middle",
			key:      " field name ",
			expected: "$.field name",
		},
		{
			name:     "single character key",
			key:      "a",
			expected: "$.a",
		},
		{
			name:     "key with special characters",
			key:      "field-name_123",
			expected: `$."field-name_123"`,
		},
		{
			name:     "unicode key",
			key:      "имя",
			expected: "$.имя",
		},
		{
			name:     "unicode key with hyphen",
			key:      "имя-пользователя",
			expected: `$."имя-пользователя"`,
		},
		{
			name:     "key with only spaces",
			key:      "   ",
			expected: `$.""`,
		},
		{
			name:     "key with dot notation",
			key:      "user.name",
			expected: "$.user.name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := buildPathToField(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPrepareOrderByClause(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sort     *types.Document
		expected string
	}{
		{
			name:     "empty sort",
			sort:     must.NotFail(types.NewDocument()),
			expected: "",
		},
		{
			name:     "natural ascending",
			sort:     must.NotFail(types.NewDocument("$natural", int64(1))),
			expected: " ORDER BY _ferretdb_record_id",
		},
		{
			name:     "natural descending",
			sort:     must.NotFail(types.NewDocument("$natural", int64(-1))),
			expected: " ORDER BY _ferretdb_record_id DESC",
		},
		{
			name: "multiple fields - ignored",
			sort: must.NotFail(types.NewDocument(
				"field1", int64(1),
				"field2", int64(-1),
			)),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := prepareOrderByClause(tt.sort)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAdjustInt64Value(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      int64
		expectedOp CompareOp
	}{
		{
			name:       "value within safe range",
			value:      100,
			expectedOp: CompareOpEq,
		},
		{
			name:       "value at max safe",
			value:      9007199254740991, // MaxSafeDouble
			expectedOp: CompareOpEq,
		},
		{
			name:       "value above max safe",
			value:      9007199254740992,
			expectedOp: CompareOpGt,
		},
		{
			name:       "value at min safe",
			value:      -9007199254740991,
			expectedOp: CompareOpEq,
		},
		{
			name:       "value below min safe",
			value:      -9007199254740992,
			expectedOp: CompareOpLt,
		},
		{
			name:       "zero value",
			value:      0,
			expectedOp: CompareOpEq,
		},
		{
			name:       "negative value within range",
			value:      -100,
			expectedOp: CompareOpEq,
		},
		{
			name:       "max int64",
			value:      9223372036854775807,
			expectedOp: CompareOpGt,
		},
		{
			name:       "min int64",
			value:      -9223372036854775808,
			expectedOp: CompareOpLt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			adjustedVal, op := adjustInt64Value(tt.value)
			assert.Equal(t, tt.expectedOp, op)

			// Verify adjusted value is reasonable
			switch op {
			case CompareOpEq:
				assert.Equal(t, tt.value, adjustedVal)
			case CompareOpGt:
				assert.Greater(t, tt.value, adjustedVal.(int64))
			case CompareOpLt:
				assert.Less(t, tt.value, adjustedVal.(int64))
			}
		})
	}
}

func TestAdjustFloat64Value(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      float64
		expectedOp CompareOp
	}{
		{
			name:       "value within safe range",
			value:      100.5,
			expectedOp: CompareOpEq,
		},
		{
			name:       "value at max safe",
			value:      9007199254740991.0,
			expectedOp: CompareOpEq,
		},
		{
			name:       "value above max safe",
			value:      9007199254740992.0,
			expectedOp: CompareOpGt,
		},
		{
			name:       "value below min safe",
			value:      -9007199254740992.0,
			expectedOp: CompareOpLt,
		},
		{
			name:       "zero value",
			value:      0.0,
			expectedOp: CompareOpEq,
		},
		{
			name:       "negative value within range",
			value:      -100.5,
			expectedOp: CompareOpEq,
		},
		{
			name:       "very small positive value",
			value:      0.0000001,
			expectedOp: CompareOpEq,
		},
		{
			name:       "very small negative value",
			value:      -0.0000001,
			expectedOp: CompareOpEq,
		},
		{
			name:       "value at negative max safe",
			value:      -9007199254740991.0,
			expectedOp: CompareOpEq,
		},
		{
			name:       "large positive value",
			value:      1e308,
			expectedOp: CompareOpGt,
		},
		{
			name:       "large negative value",
			value:      -1e308,
			expectedOp: CompareOpLt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			adjustedVal, op := adjustFloat64Value(tt.value)
			assert.Equal(t, tt.expectedOp, op)

			// Verify adjusted value is reasonable
			switch op {
			case CompareOpEq:
				assert.Equal(t, tt.value, adjustedVal)
			case CompareOpGt:
				assert.Greater(t, tt.value, adjustedVal.(float64))
			case CompareOpLt:
				assert.Less(t, tt.value, adjustedVal.(float64))
			}
		})
	}
}

func TestGetDefaultJsonFilterExpr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		paramName string
		op        CompareOp
		expected  string
	}{
		{
			name:      "equals comparison",
			path:      "$.name",
			paramName: "$param1",
			op:        CompareOpEq,
			expected:  `JSON_EXISTS(_jsonb, '$.name ? (@ == $param)' PASSING $param1 AS "param")`,
		},
		{
			name:      "not equals comparison",
			path:      "$.age",
			paramName: "$param2",
			op:        CompareOpNe,
			expected:  `JSON_EXISTS(_jsonb, '$.age ? (@ != $param)' PASSING $param2 AS "param")`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := getDefaultJsonFilterExpr(tt.path, tt.paramName, tt.op)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetNotEqualJsonFilterExpr(t *testing.T) {
	t.Parallel()

	rootKey := "field"
	bsonType := metadata.BsonString
	paramName := "$param1"

	result := getNotEqualJsonFilterExpr(rootKey, bsonType, paramName)

	assert.Contains(t, result, "NOT JSON_EXISTS")
	assert.Contains(t, result, rootKey)
	assert.Contains(t, result, paramName)
	assert.Contains(t, result, string(bsonType))
}

func TestIsIndexableType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bsonType metadata.BsonType
		expected bool
	}{
		{
			name:     "string is indexable",
			bsonType: metadata.BsonString,
			expected: true,
		},
		{
			name:     "objectId is indexable",
			bsonType: metadata.BsonObjectId,
			expected: true,
		},
		{
			name:     "bool is indexable",
			bsonType: metadata.BsonBool,
			expected: true,
		},
		{
			name:     "date is indexable",
			bsonType: metadata.BsonDate,
			expected: true,
		},
		{
			name:     "int is indexable",
			bsonType: metadata.BsonInt,
			expected: true,
		},
		{
			name:     "long is indexable",
			bsonType: metadata.BsonLong,
			expected: true,
		},
		{
			name:     "double is indexable",
			bsonType: metadata.BsonDouble,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isIndexableType(tt.bsonType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFindSecondaryIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rootKey  string
		bsonType metadata.BsonType
		mongoOp  MongoOp
		indexes  []metadata.IndexInfo
		expected bool // true if index is found
	}{
		{
			name:     "_id field returns nil",
			rootKey:  "_id",
			bsonType: metadata.BsonString,
			mongoOp:  FieldOpEq,
			indexes:  []metadata.IndexInfo{},
			expected: false,
		},
		{
			name:     "non-indexable op returns nil",
			rootKey:  "field1",
			bsonType: metadata.BsonString,
			mongoOp:  FieldOpNe,
			indexes:  []metadata.IndexInfo{},
			expected: false,
		},
		{
			name:     "field with matching index",
			rootKey:  "field1",
			bsonType: metadata.BsonString,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "idx1",
					SanitizedName: "idx1_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "field1"},
					},
				},
			},
			expected: true,
		},
		{
			name:     "field with non-ready index",
			rootKey:  "field1",
			bsonType: metadata.BsonString,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "idx1",
					SanitizedName: "idx1_sanitized",
					Ready:         false,
					Key: []metadata.IndexKeyPair{
						{Field: "field1"},
					},
				},
			},
			expected: false,
		},
		{
			name:     "multiple indexes - finds first matching",
			rootKey:  "field1",
			bsonType: metadata.BsonString,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "idx1",
					SanitizedName: "idx1_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "field2"},
					},
				},
				{
					Name:          "idx2",
					SanitizedName: "idx2_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "field1"},
					},
				},
			},
			expected: true,
		},
		{
			name:     "compound index - matches field",
			rootKey:  "field1",
			bsonType: metadata.BsonString,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "compound_idx",
					SanitizedName: "compound_idx_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "field1"},
						{Field: "field2"},
					},
				},
			},
			expected: true,
		},
		{
			name:     "compound index - matches second field",
			rootKey:  "field2",
			bsonType: metadata.BsonString,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "compound_idx",
					SanitizedName: "compound_idx_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "field1"},
						{Field: "field2"},
					},
				},
			},
			expected: true,
		},
		{
			name:     "no matching index",
			rootKey:  "field3",
			bsonType: metadata.BsonString,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "idx1",
					SanitizedName: "idx1_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "field1"},
					},
				},
			},
			expected: false,
		},
		{
			name:     "empty indexes list",
			rootKey:  "field1",
			bsonType: metadata.BsonString,
			mongoOp:  FieldOpEq,
			indexes:  []metadata.IndexInfo{},
			expected: false,
		},
		{
			name:     "non-indexable type returns nil",
			rootKey:  "field1",
			bsonType: "unsupported_type",
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "idx1",
					SanitizedName: "idx1_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "field1"},
					},
				},
			},
			expected: false,
		},
		{
			name:     "ObjectID type",
			rootKey:  "field1",
			bsonType: metadata.BsonObjectId,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "idx1",
					SanitizedName: "idx1_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "field1"},
					},
				},
			},
			expected: true,
		},
		{
			name:     "int type",
			rootKey:  "count",
			bsonType: metadata.BsonInt,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "idx_count",
					SanitizedName: "idx_count_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "count"},
					},
				},
			},
			expected: true,
		},
		{
			name:     "long type",
			rootKey:  "bignum",
			bsonType: metadata.BsonLong,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "idx_bignum",
					SanitizedName: "idx_bignum_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "bignum"},
					},
				},
			},
			expected: true,
		},
		{
			name:     "double type",
			rootKey:  "price",
			bsonType: metadata.BsonDouble,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "idx_price",
					SanitizedName: "idx_price_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "price"},
					},
				},
			},
			expected: true,
		},
		{
			name:     "bool type",
			rootKey:  "active",
			bsonType: metadata.BsonBool,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "idx_active",
					SanitizedName: "idx_active_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "active"},
					},
				},
			},
			expected: true,
		},
		{
			name:     "date type",
			rootKey:  "created",
			bsonType: metadata.BsonDate,
			mongoOp:  FieldOpEq,
			indexes: []metadata.IndexInfo{
				{
					Name:          "idx_created",
					SanitizedName: "idx_created_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "created"},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := findSecondaryIndex(tt.rootKey, tt.bsonType, tt.mongoOp, tt.indexes)
			if tt.expected {
				assert.NotNil(t, result)
				assert.NotNil(t, result.idxName)
			} else {
				if result != nil {
					assert.Nil(t, result.idxName)
				}
			}
		})
	}
}

func TestBuildIndexedFieldExpr(t *testing.T) {
	t.Parallel()

	t.Run("simple field", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("name", metadata.BsonString, CompareOpEq, "test", placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "name_string")
		assert.NotEmpty(t, params)
	})

	t.Run("_id field with eq", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("_id", metadata.BsonString, CompareOpEq, "test", placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "id_hash")
		assert.NotEmpty(t, params)
	})

	t.Run("_id field with ne", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("_id", metadata.BsonString, CompareOpNe, "test", placeholder)

		assert.NotEmpty(t, query)
		assert.NotContains(t, query, "id_hash")
		assert.NotEmpty(t, params)
	})

	t.Run("int field", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("count", metadata.BsonInt, CompareOpEq, int32(42), placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "count_scalar")
		assert.NotEmpty(t, params)
	})

	t.Run("long field", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("bignum", metadata.BsonLong, CompareOpEq, int64(12345), placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "bignum_scalar")
		assert.NotEmpty(t, params)
	})

	t.Run("double field", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("price", metadata.BsonDouble, CompareOpEq, float64(99.99), placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "price_scalar")
		assert.NotEmpty(t, params)
	})

	t.Run("bool field", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("active", metadata.BsonBool, CompareOpEq, true, placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "active_bool")
		assert.NotEmpty(t, params)
	})

	t.Run("objectid field", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		objectId := types.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
		query, params := buildIndexedFieldExpr("obj_id", metadata.BsonObjectId, CompareOpEq, objectId, placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "obj_id_objectId")
		assert.NotEmpty(t, params)
	})

	t.Run("ne operator", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("status", metadata.BsonString, CompareOpNe, "inactive", placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "status_string")
		assert.Contains(t, query, "!=")
		assert.NotEmpty(t, params)
	})

	t.Run("gt operator", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("count", metadata.BsonInt, CompareOpGt, int32(10), placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "count_scalar")
		assert.Contains(t, query, ">")
		assert.NotEmpty(t, params)
	})

	t.Run("lt operator", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("count", metadata.BsonInt, CompareOpLt, int32(100), placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "count_scalar")
		assert.Contains(t, query, "<")
		assert.NotEmpty(t, params)
	})

	t.Run("field with special characters", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("field_name", metadata.BsonString, CompareOpEq, "test", placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "field_name_string")
		assert.NotEmpty(t, params)
	})

	t.Run("empty string value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("name", metadata.BsonString, CompareOpEq, "", placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "name_string")
		assert.NotEmpty(t, params)
	})

	t.Run("zero int value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("count", metadata.BsonInt, CompareOpEq, int32(0), placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "count_scalar")
		assert.NotEmpty(t, params)
	})

	t.Run("negative int value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("balance", metadata.BsonInt, CompareOpEq, int32(-100), placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "balance_scalar")
		assert.NotEmpty(t, params)
	})

	t.Run("false bool value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("active", metadata.BsonBool, CompareOpEq, false, placeholder)

		assert.NotEmpty(t, query)
		assert.Contains(t, query, "active_bool")
		assert.NotEmpty(t, params)
	})

	t.Run("checks all column types are NULL except target", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		query, params := buildIndexedFieldExpr("field", metadata.BsonString, CompareOpEq, "value", placeholder)

		assert.NotEmpty(t, query)
		// Should contain IS NULL checks for other column types
		assert.Contains(t, query, "IS NULL")
		assert.Contains(t, query, "AND")
		assert.NotEmpty(t, params)
	})
}

func TestPrepareWhereClause(t *testing.T) {
	t.Parallel()

	t.Run("empty filter", func(t *testing.T) {
		filter := must.NotFail(types.NewDocument())
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.Empty(t, query)
		assert.Empty(t, args)
		assert.NotNil(t, secIdx)
	})

	t.Run("simple string filter", func(t *testing.T) {
		filter := must.NotFail(types.NewDocument("name", "test"))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		// secIdx can be nil when no indexes are defined
		_ = secIdx
	})

	t.Run("filter with operator", func(t *testing.T) {
		opDoc := must.NotFail(types.NewDocument("$eq", "test"))
		filter := must.NotFail(types.NewDocument("name", opDoc))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		// secIdx can be nil when no indexes are defined
		_ = secIdx
	})

	t.Run("filter with unsupported operator", func(t *testing.T) {
		opDoc := must.NotFail(types.NewDocument("$gt", int64(10)))
		filter := must.NotFail(types.NewDocument("age", opDoc))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.Empty(t, query)
		assert.Empty(t, args)
		assert.NotNil(t, secIdx)
	})

	t.Run("filter with int32", func(t *testing.T) {
		filter := must.NotFail(types.NewDocument("count", int32(42)))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		_ = secIdx
	})

	t.Run("filter with int64", func(t *testing.T) {
		filter := must.NotFail(types.NewDocument("count", int64(12345)))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		_ = secIdx
	})

	t.Run("filter with float64", func(t *testing.T) {
		filter := must.NotFail(types.NewDocument("price", float64(99.99)))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		_ = secIdx
	})

	t.Run("filter with bool", func(t *testing.T) {
		filter := must.NotFail(types.NewDocument("active", true))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		_ = secIdx
	})

	t.Run("filter with time", func(t *testing.T) {
		now := time.Now()
		filter := must.NotFail(types.NewDocument("created", now))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		_ = secIdx
	})

	t.Run("filter with ObjectID", func(t *testing.T) {
		objectId := types.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
		filter := must.NotFail(types.NewDocument("_id", objectId))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		_ = secIdx
	})

	t.Run("filter with multiple fields", func(t *testing.T) {
		filter := must.NotFail(types.NewDocument(
			"name", "test",
			"age", int32(25),
		))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		assert.Contains(t, query, "AND")
		_ = secIdx
	})

	t.Run("filter with $ne operator", func(t *testing.T) {
		opDoc := must.NotFail(types.NewDocument("$ne", "test"))
		filter := must.NotFail(types.NewDocument("name", opDoc))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		_ = secIdx
	})

	t.Run("filter with indexed field", func(t *testing.T) {
		filter := must.NotFail(types.NewDocument("indexed_field", "test"))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{
				{
					Name:          "idx1",
					SanitizedName: "idx1_sanitized",
					Ready:         true,
					Key: []metadata.IndexKeyPair{
						{Field: "indexed_field"},
					},
				},
			},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		assert.NotNil(t, secIdx)
		if secIdx != nil && secIdx.idxName != nil {
			assert.Equal(t, "idx1_sanitized", *secIdx.idxName)
		}
	})

	t.Run("filter with dot notation", func(t *testing.T) {
		filter := must.NotFail(types.NewDocument("user.name", "test"))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		_ = secIdx
	})

	t.Run("filter with system key $natural", func(t *testing.T) {
		filter := must.NotFail(types.NewDocument("$natural", int64(1)))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		// System keys should be skipped
		assert.Empty(t, query)
		assert.Empty(t, args)
		_ = secIdx
	})

	t.Run("filter with empty string key", func(t *testing.T) {
		filter := must.NotFail(types.NewDocument("", "value"))
		meta := &metadata.Collection{
			Indexes: []metadata.IndexInfo{},
		}
		placeholder := new(metadata.Placeholder)

		query, args, secIdx, err := prepareWhereClause(filter, meta, placeholder)

		require.NoError(t, err)
		assert.NotEmpty(t, query)
		assert.NotNil(t, args)
		_ = secIdx
	})
}

func TestBuildJsonPathExpr(t *testing.T) {
	t.Parallel()

	t.Run("string value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.name", metadata.BsonString, "test", "name", CompareOpEq, placeholder)

		assert.NotEmpty(t, expr)
		assert.Contains(t, expr, "JSON_EXISTS")
		assert.NotEmpty(t, params)
	})

	t.Run("int64 value in safe range", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.count", metadata.BsonLong, int64(100), "count", CompareOpEq, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})

	t.Run("not equal operator", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.name", metadata.BsonString, "test", "name", CompareOpNe, placeholder)

		assert.NotEmpty(t, expr)
		assert.Contains(t, expr, "NOT JSON_EXISTS")
		assert.NotEmpty(t, params)
	})

	t.Run("int64 above max safe", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.count", metadata.BsonLong, int64(9007199254740992), "count", CompareOpEq, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
		assert.Contains(t, expr, ">")
	})

	t.Run("int64 below min safe", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.count", metadata.BsonLong, int64(-9007199254740992), "count", CompareOpEq, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
		assert.Contains(t, expr, "<")
	})

	t.Run("float64 above max safe", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.price", metadata.BsonDouble, float64(1e308), "price", CompareOpEq, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
		assert.Contains(t, expr, ">")
	})

	t.Run("float64 below min safe", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.price", metadata.BsonDouble, float64(-1e308), "price", CompareOpEq, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
		assert.Contains(t, expr, "<")
	})

	t.Run("bool value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.active", metadata.BsonBool, true, "active", CompareOpEq, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})

	t.Run("int32 value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.count", metadata.BsonInt, int32(42), "count", CompareOpEq, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})

	t.Run("empty string value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.name", metadata.BsonString, "", "name", CompareOpEq, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})

	t.Run("nested path", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.user.name", metadata.BsonString, "test", "user.name", CompareOpEq, placeholder)

		assert.NotEmpty(t, expr)
		assert.Contains(t, expr, "$.user.name")
		assert.NotEmpty(t, params)
	})

	t.Run("ne operator with string", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.status", metadata.BsonString, "inactive", "status", CompareOpNe, placeholder)

		assert.NotEmpty(t, expr)
		assert.Contains(t, expr, "NOT JSON_EXISTS")
		assert.NotEmpty(t, params)
	})

	t.Run("ne operator with int", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.count", metadata.BsonInt, int32(0), "count", CompareOpNe, placeholder)

		assert.NotEmpty(t, expr)
		assert.Contains(t, expr, "NOT JSON_EXISTS")
		assert.NotEmpty(t, params)
	})

	t.Run("zero values", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)

		// Zero int
		expr, params := buildJsonPathExpr("$.count", metadata.BsonInt, int32(0), "count", CompareOpEq, placeholder)
		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)

		// Zero float
		placeholder = new(metadata.Placeholder)
		expr, params = buildJsonPathExpr("$.price", metadata.BsonDouble, float64(0.0), "price", CompareOpEq, placeholder)
		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})

	t.Run("negative values", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		expr, params := buildJsonPathExpr("$.balance", metadata.BsonDouble, float64(-100.50), "balance", CompareOpEq, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})
}

func TestBuildWhereExpression(t *testing.T) {
	t.Parallel()

	t.Run("indexed field", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		info := whereExpressionParams{
			rootKey:       "name",
			bsonType:      metadata.BsonString,
			path:          "$.name",
			mongoOperator: FieldOpEq,
			useIndex:      true,
			value:         "test",
		}

		expr, params := buildWhereExpression(info, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})

	t.Run("non-indexed field", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		info := whereExpressionParams{
			rootKey:       "name",
			bsonType:      metadata.BsonString,
			path:          "$.name",
			mongoOperator: FieldOpEq,
			useIndex:      false,
			value:         "test",
		}

		expr, params := buildWhereExpression(info, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})

	t.Run("indexed field with int", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		info := whereExpressionParams{
			rootKey:       "count",
			bsonType:      metadata.BsonInt,
			path:          "$.count",
			mongoOperator: FieldOpEq,
			useIndex:      true,
			value:         int32(42),
		}

		expr, params := buildWhereExpression(info, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})

	t.Run("non-indexed field with float", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		info := whereExpressionParams{
			rootKey:       "price",
			bsonType:      metadata.BsonDouble,
			path:          "$.price",
			mongoOperator: FieldOpEq,
			useIndex:      false,
			value:         float64(99.99),
		}

		expr, params := buildWhereExpression(info, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})

	t.Run("ne operator indexed", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		info := whereExpressionParams{
			rootKey:       "status",
			bsonType:      metadata.BsonString,
			path:          "$.status",
			mongoOperator: FieldOpNe,
			useIndex:      true,
			value:         "inactive",
		}

		expr, params := buildWhereExpression(info, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})

	t.Run("ne operator non-indexed", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		info := whereExpressionParams{
			rootKey:       "status",
			bsonType:      metadata.BsonString,
			path:          "$.status",
			mongoOperator: FieldOpNe,
			useIndex:      false,
			value:         "inactive",
		}

		expr, params := buildWhereExpression(info, placeholder)

		assert.NotEmpty(t, expr)
		assert.NotEmpty(t, params)
	})
}

func TestPrepareLimitClause(t *testing.T) {
	t.Parallel()

	t.Run("with limit", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		params := &backends.QueryParams{
			Limit: 100,
		}

		paramName, paramOption := prepareLimitClause(params, placeholder)

		assert.NotEmpty(t, paramName)
		assert.NotNil(t, paramOption)
		assert.Equal(t, "$f1", paramName)
	})

	t.Run("without limit - uses default", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		params := &backends.QueryParams{
			Limit: 0,
		}

		paramName, paramOption := prepareLimitClause(params, placeholder)

		assert.NotEmpty(t, paramName)
		assert.NotNil(t, paramOption)
		assert.Equal(t, "$f1", paramName)
	})

	t.Run("with large limit", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		params := &backends.QueryParams{
			Limit: 999999,
		}

		paramName, paramOption := prepareLimitClause(params, placeholder)

		assert.NotEmpty(t, paramName)
		assert.NotNil(t, paramOption)
	})
}

func TestGetConditionExpr(t *testing.T) {
	t.Parallel()

	t.Run("string value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		result := getConditionExpr("name", []metadata.IndexInfo{}, "test", FieldOpEq, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
		assert.NotEmpty(t, result.ParamOptions)
		// SecondaryIdx can be nil or have nil idxName when no index is found
		if result.SecondaryIdx != nil && result.SecondaryIdx.idxName != nil {
			assert.NotEmpty(t, *result.SecondaryIdx.idxName)
		}
	})

	t.Run("int32 value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		result := getConditionExpr("count", []metadata.IndexInfo{}, int32(42), FieldOpEq, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
		assert.NotEmpty(t, result.ParamOptions)
	})

	t.Run("bool value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		result := getConditionExpr("active", []metadata.IndexInfo{}, true, FieldOpEq, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
		assert.NotEmpty(t, result.ParamOptions)
	})

	t.Run("time value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		now := time.Now()
		result := getConditionExpr("created", []metadata.IndexInfo{}, now, FieldOpEq, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
		assert.NotEmpty(t, result.ParamOptions)
	})

	t.Run("int64 value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		result := getConditionExpr("count", []metadata.IndexInfo{}, int64(12345), FieldOpEq, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
		assert.NotEmpty(t, result.ParamOptions)
	})

	t.Run("float64 value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		result := getConditionExpr("price", []metadata.IndexInfo{}, float64(99.99), FieldOpEq, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
		assert.NotEmpty(t, result.ParamOptions)
	})

	t.Run("ObjectID value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		objectId := types.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
		result := getConditionExpr("_id", []metadata.IndexInfo{}, objectId, FieldOpEq, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
		assert.NotEmpty(t, result.ParamOptions)
	})

	t.Run("_id field special handling", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		result := getConditionExpr("_id", []metadata.IndexInfo{}, "test_id", FieldOpEq, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
		assert.NotEmpty(t, result.ParamOptions)
		// _id uses primary key, not secondary index
		// SecondaryIdx can be nil or have nil idxName
		if result.SecondaryIdx != nil {
			assert.Nil(t, result.SecondaryIdx.idxName)
		}
	})

	t.Run("unsupported type returns nil", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		// Binary type is not supported for pushdown
		binaryData := types.Binary{Subtype: 0x00, B: []byte{0x01, 0x02}}
		result := getConditionExpr("data", []metadata.IndexInfo{}, binaryData, FieldOpEq, placeholder)

		assert.Nil(t, result)
	})

	t.Run("empty string value", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		result := getConditionExpr("name", []metadata.IndexInfo{}, "", FieldOpEq, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
	})

	t.Run("zero int32", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		result := getConditionExpr("count", []metadata.IndexInfo{}, int32(0), FieldOpEq, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
	})

	t.Run("negative int64", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		result := getConditionExpr("count", []metadata.IndexInfo{}, int64(-100), FieldOpEq, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
	})

	t.Run("ne operator", func(t *testing.T) {
		placeholder := new(metadata.Placeholder)
		result := getConditionExpr("name", []metadata.IndexInfo{}, "test", FieldOpNe, placeholder)

		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Expression)
		assert.NotEmpty(t, result.ParamOptions)
	})
}
