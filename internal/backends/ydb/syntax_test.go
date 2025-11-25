package ydb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSyntaxConstants tests that all SQL syntax constants are defined correctly
func TestSyntaxConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{
			name:     "SELECT keyword",
			constant: SelectWord,
			expected: "SELECT",
		},
		{
			name:     "WHERE keyword",
			constant: WhereWord,
			expected: "WHERE",
		},
		{
			name:     "VIEW keyword",
			constant: ViewWord,
			expected: "VIEW",
		},
		{
			name:     "ORDER BY keyword",
			constant: OrderByWord,
			expected: "ORDER BY",
		},
		{
			name:     "LIMIT keyword",
			constant: LimitWord,
			expected: "LIMIT",
		},
		{
			name:     "AND keyword",
			constant: AndWord,
			expected: "AND",
		},
		{
			name:     "FROM keyword",
			constant: FromWord,
			expected: "FROM",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.constant)
		})
	}
}

// TestSyntaxConstantsNotEmpty tests that all constants are non-empty
func TestSyntaxConstantsNotEmpty(t *testing.T) {
	t.Parallel()

	constants := []string{
		SelectWord,
		WhereWord,
		ViewWord,
		OrderByWord,
		LimitWord,
		AndWord,
		FromWord,
	}

	for i, constant := range constants {
		t.Run("constant_"+string(rune('0'+i)), func(t *testing.T) {
			t.Parallel()
			assert.NotEmpty(t, constant, "Constant should not be empty")
		})
	}
}

// TestSyntaxConstantsUpperCase tests that SQL keywords are uppercase
func TestSyntaxConstantsUpperCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
	}{
		{"SELECT", SelectWord},
		{"WHERE", WhereWord},
		{"VIEW", ViewWord},
		{"ORDER BY", OrderByWord},
		{"LIMIT", LimitWord},
		{"AND", AndWord},
		{"FROM", FromWord},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.name, tt.constant, "Keyword should be uppercase")
		})
	}
}

// TestMongoOpConstants tests MongoDB operator constants
func TestMongoOpConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, MongoOp("$eq"), FieldOpEq)
	assert.Equal(t, MongoOp("$ne"), FieldOpNe)
}

// TestCompareOpConstants tests comparison operator constants
func TestCompareOpConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, CompareOp("=="), CompareOpEq)
	assert.Equal(t, CompareOp("!="), CompareOpNe)
	assert.Equal(t, CompareOp(">"), CompareOpGt)
	assert.Equal(t, CompareOp("<"), CompareOpLt)
}

// TestOperatorMappings tests that operator mappings are complete
func TestOperatorMappings(t *testing.T) {
	t.Parallel()

	t.Run("pushdown operators", func(t *testing.T) {
		t.Parallel()

		// Test $eq mapping
		op, ok := pushdownOperators[FieldOpEq]
		assert.True(t, ok)
		assert.Equal(t, CompareOpEq, op)

		// Test $ne mapping
		op, ok = pushdownOperators[FieldOpNe]
		assert.True(t, ok)
		assert.Equal(t, CompareOpNe, op)

		// Test non-existent operator
		_, ok = pushdownOperators[MongoOp("$gt")]
		assert.False(t, ok)
	})

	t.Run("indexing operators", func(t *testing.T) {
		t.Parallel()

		// Test $eq is indexable
		op, ok := operatorsSupportedForIndexing[FieldOpEq]
		assert.True(t, ok)
		assert.Equal(t, CompareOpEq, op)

		// Test $ne is not indexable
		_, ok = operatorsSupportedForIndexing[FieldOpNe]
		assert.False(t, ok)
	})
}

// TestJsonPathRoot tests JSON path root constant
func TestJsonPathRoot(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "$", jsonPathRoot)
}

// TestDefaultRowsLimit tests default rows limit constant
func TestDefaultRowsLimit(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 1000, defaultRowsLimit)
	assert.Greater(t, defaultRowsLimit, 0, "Default rows limit should be positive")
}

