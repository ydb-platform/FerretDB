package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestObjectNameCharacters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple name",
			input:    "tableName",
			expected: "tableName",
		},
		{
			name:     "name with underscore",
			input:    "table_name",
			expected: "table_name",
		},
		{
			name:     "name with hyphen",
			input:    "table-name",
			expected: "table-name",
		},
		{
			name:     "name with dot",
			input:    "table.name",
			expected: "table.name",
		},
		{
			name:     "name with numbers",
			input:    "table123",
			expected: "table123",
		},
		{
			name:     "name with spaces",
			input:    "table name",
			expected: "table_name",
		},
		{
			name:     "name with special characters",
			input:    "table@name#test",
			expected: "table_name_test",
		},
		{
			name:     "name with unicode",
			input:    "таблица",
			expected: "_______",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only special characters",
			input:    "@#$%",
			expected: "____",
		},
		{
			name:     "mixed valid and invalid",
			input:    "my@table_name.123",
			expected: "my_table_name.123",
		},
		{
			name:     "uppercase letters",
			input:    "TABLE",
			expected: "TABLE",
		},
		{
			name:     "mixed case",
			input:    "MyTableName",
			expected: "MyTableName",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := objectNameCharacters.ReplaceAllString(tt.input, "_")
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestColumnNameCharacters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple name",
			input:    "columnName",
			expected: "columnName",
		},
		{
			name:     "name with underscore",
			input:    "column_name",
			expected: "column_name",
		},
		{
			name:     "name with hyphen",
			input:    "column-name",
			expected: "column-name",
		},
		{
			name:     "name with dot should be replaced",
			input:    "column.name",
			expected: "column_name",
		},
		{
			name:     "name with numbers",
			input:    "column123",
			expected: "column123",
		},
		{
			name:     "name with spaces",
			input:    "column name",
			expected: "column_name",
		},
		{
			name:     "name with special characters",
			input:    "column@name#test",
			expected: "column_name_test",
		},
		{
			name:     "name with unicode",
			input:    "колонка",
			expected: "_______",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only special characters",
			input:    "@#$%",
			expected: "____",
		},
		{
			name:     "mixed valid and invalid",
			input:    "my@column_name-123",
			expected: "my_column_name-123",
		},
		{
			name:     "uppercase letters",
			input:    "COLUMN",
			expected: "COLUMN",
		},
		{
			name:     "mixed case",
			input:    "MyColumnName",
			expected: "MyColumnName",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := columnNameCharacters.ReplaceAllString(tt.input, "_")
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestObjectNameVsColumnNameDifference(t *testing.T) {
	t.Parallel()

	// The main difference is that object names allow dots, but column names don't
	t.Run("dot character", func(t *testing.T) {
		t.Parallel()
		input := "name.with.dots"

		objectResult := objectNameCharacters.ReplaceAllString(input, "_")
		columnResult := columnNameCharacters.ReplaceAllString(input, "_")

		// Object names keep dots
		assert.Equal(t, "name.with.dots", objectResult)

		// Column names replace dots
		assert.Equal(t, "name_with_dots", columnResult)
	})
}

func TestMaxObjectNameLength(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 255, maxObjectNameLength)
	assert.Greater(t, maxObjectNameLength, 0)
	assert.LessOrEqual(t, maxObjectNameLength, 1000) // Sanity check
}

func TestRegexpPatterns(t *testing.T) {
	t.Parallel()

	t.Run("objectNameCharacters pattern", func(t *testing.T) {
		t.Parallel()
		// Test that the pattern is not nil
		assert.NotNil(t, objectNameCharacters)

		// Test some specific matches
		assert.True(t, objectNameCharacters.MatchString("@"))
		assert.True(t, objectNameCharacters.MatchString("#"))
		assert.True(t, objectNameCharacters.MatchString(" "))

		// Test some specific non-matches
		assert.False(t, objectNameCharacters.MatchString("a"))
		assert.False(t, objectNameCharacters.MatchString("Z"))
		assert.False(t, objectNameCharacters.MatchString("0"))
		assert.False(t, objectNameCharacters.MatchString("_"))
		assert.False(t, objectNameCharacters.MatchString("."))
		assert.False(t, objectNameCharacters.MatchString("-"))
	})

	t.Run("columnNameCharacters pattern", func(t *testing.T) {
		t.Parallel()
		// Test that the pattern is not nil
		assert.NotNil(t, columnNameCharacters)

		// Test some specific matches
		assert.True(t, columnNameCharacters.MatchString("@"))
		assert.True(t, columnNameCharacters.MatchString("#"))
		assert.True(t, columnNameCharacters.MatchString(" "))
		assert.True(t, columnNameCharacters.MatchString(".")) // Dots not allowed in columns

		// Test some specific non-matches
		assert.False(t, columnNameCharacters.MatchString("a"))
		assert.False(t, columnNameCharacters.MatchString("Z"))
		assert.False(t, columnNameCharacters.MatchString("0"))
		assert.False(t, columnNameCharacters.MatchString("_"))
		assert.False(t, columnNameCharacters.MatchString("-"))
	})
}

func TestCharacterSetsConsistency(t *testing.T) {
	t.Parallel()

	// Test that all alphanumeric characters are allowed
	alphanumeric := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	t.Run("alphanumeric in object names", func(t *testing.T) {
		t.Parallel()
		result := objectNameCharacters.ReplaceAllString(alphanumeric, "_")
		assert.Equal(t, alphanumeric, result, "All alphanumeric should be preserved")
	})

	t.Run("alphanumeric in column names", func(t *testing.T) {
		t.Parallel()
		result := columnNameCharacters.ReplaceAllString(alphanumeric, "_")
		assert.Equal(t, alphanumeric, result, "All alphanumeric should be preserved")
	})

	t.Run("underscore and hyphen allowed in both", func(t *testing.T) {
		t.Parallel()
		input := "name_with-hyphen"

		objectResult := objectNameCharacters.ReplaceAllString(input, "*")
		columnResult := columnNameCharacters.ReplaceAllString(input, "*")

		assert.Equal(t, input, objectResult)
		assert.Equal(t, input, columnResult)
	})
}

func TestEdgeCasesForRegex(t *testing.T) {
	t.Parallel()

	t.Run("consecutive special characters", func(t *testing.T) {
		t.Parallel()
		input := "name@@@name"
		result := objectNameCharacters.ReplaceAllString(input, "_")
		assert.Equal(t, "name___name", result)
	})

	t.Run("special characters at boundaries", func(t *testing.T) {
		t.Parallel()
		input := "@name@"
		result := objectNameCharacters.ReplaceAllString(input, "_")
		assert.Equal(t, "_name_", result)
	})

	t.Run("mixed valid invalid valid", func(t *testing.T) {
		t.Parallel()
		input := "a@b#c$d"
		result := objectNameCharacters.ReplaceAllString(input, "_")
		assert.Equal(t, "a_b_c_d", result)
	})

	t.Run("newline and tab characters", func(t *testing.T) {
		t.Parallel()
		input := "name\nwith\ttabs"
		result := objectNameCharacters.ReplaceAllString(input, "_")
		assert.Equal(t, "name_with_tabs", result)
	})
}
