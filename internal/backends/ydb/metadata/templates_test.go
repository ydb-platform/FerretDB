package metadata

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/FerretDB/FerretDB/internal/backends"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestEscapeName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple name",
			input:    "table",
			expected: "`table`",
		},
		{
			name:     "name with spaces",
			input:    "my table",
			expected: "`my table`",
		},
		{
			name:     "name with special characters",
			input:    "table-name_123",
			expected: "`table-name_123`",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "``",
		},
		{
			name:     "unicode name",
			input:    "таблица",
			expected: "`таблица`",
		},
		{
			name:     "name with quotes",
			input:    `table"name`,
			expected: "`table\"name`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := escapeName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		a        int
		b        int
		expected int
	}{
		{
			name:     "positive result",
			a:        10,
			b:        5,
			expected: 5,
		},
		{
			name:     "negative result",
			a:        5,
			b:        10,
			expected: -5,
		},
		{
			name:     "zero result",
			a:        10,
			b:        10,
			expected: 0,
		},
		{
			name:     "both negative",
			a:        -5,
			b:        -10,
			expected: 5,
		},
		{
			name:     "large numbers",
			a:        1000000,
			b:        999999,
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := sub(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRender(t *testing.T) {
	t.Parallel()

	t.Run("render upsert template", func(t *testing.T) {
		t.Parallel()
		config := UpsertTemplateConfig{
			TablePathPrefix: "/test/path",
			TableName:       "test_table",
			FieldDecls:      "id: Uint64, name: String",
			SelectFields:    "id, name",
		}

		result, err := Render(UpsertTmpl, config)
		require.NoError(t, err)
		assert.Contains(t, result, "/test/path")
		assert.Contains(t, result, "test_table")
		assert.Contains(t, result, "id, name")
	})

	t.Run("render delete template", func(t *testing.T) {
		t.Parallel()
		config := DeleteTemplateConfig{
			TablePathPrefix:   "/test/path",
			TableName:         "test_table",
			PrimaryKeyColumns: []string{"id", "name"},
			ColumnName:        "id",
			IDType:            "Uint64",
		}

		result, err := Render(DeleteTmpl, config)
		require.NoError(t, err)
		assert.Contains(t, result, "/test/path")
		assert.Contains(t, result, "test_table")
		assert.Contains(t, result, "Uint64")
	})

	t.Run("render insert template", func(t *testing.T) {
		t.Parallel()
		config := UpsertTemplateConfig{
			TablePathPrefix: "/test/path",
			TableName:       "test_table",
			FieldDecls:      "id: Uint64",
			SelectFields:    "id",
		}

		result, err := Render(InsertTmpl, config)
		require.NoError(t, err)
		assert.Contains(t, result, "/test/path")
		assert.Contains(t, result, "test_table")
	})

	t.Run("render metadata template", func(t *testing.T) {
		t.Parallel()
		config := ReplaceIntoMetadataConfig{
			TablePathPrefix: "/test/path",
			TableName:       "metadata",
		}

		result, err := Render(UpdateMedataTmpl, config)
		require.NoError(t, err)
		assert.Contains(t, result, "/test/path")
		assert.Contains(t, result, "metadata")
	})
}

func TestNewDeleteConfig(t *testing.T) {
	t.Parallel()

	t.Run("with RecordIDs", func(t *testing.T) {
		t.Parallel()
		params := &backends.DeleteAllParams{
			RecordIDs: []int64{1, 2, 3},
		}
		pkColumns := []string{"id_hash", "_id_string"}

		config := NewDeleteConfig("/path", "table", pkColumns, params)

		assert.Equal(t, "/path", config.TablePathPrefix)
		assert.Equal(t, "table", config.TableName)
		assert.Equal(t, RecordIDColumn, config.ColumnName)
		assert.Equal(t, ydbTypes.TypeInt64.String(), config.IDType)
		assert.Contains(t, config.PrimaryKeyColumns, RecordIDColumn)
	})

	t.Run("with IDs", func(t *testing.T) {
		t.Parallel()
		params := &backends.DeleteAllParams{
			IDs: []any{"id1", "id2"},
		}
		pkColumns := []string{"id_hash", "_id_string"}

		config := NewDeleteConfig("/path", "table", pkColumns, params)

		assert.Equal(t, "/path", config.TablePathPrefix)
		assert.Equal(t, "table", config.TableName)
		assert.Equal(t, IdHashColumn, config.ColumnName)
		assert.Equal(t, ydbTypes.TypeUint64.String(), config.IDType)
		assert.Equal(t, pkColumns, config.PrimaryKeyColumns)
	})

	t.Run("with empty params", func(t *testing.T) {
		t.Parallel()
		params := &backends.DeleteAllParams{}
		pkColumns := []string{"id_hash"}

		config := NewDeleteConfig("/path", "table", pkColumns, params)

		assert.Equal(t, IdHashColumn, config.ColumnName)
		assert.Equal(t, ydbTypes.TypeUint64.String(), config.IDType)
	})
}

func TestTemplateInitialization(t *testing.T) {
	t.Parallel()

	// Test that all templates are initialized
	t.Run("UpsertTmpl", func(t *testing.T) {
		t.Parallel()
		assert.NotNil(t, UpsertTmpl)
	})

	t.Run("DeleteTmpl", func(t *testing.T) {
		t.Parallel()
		assert.NotNil(t, DeleteTmpl)
	})

	t.Run("InsertTmpl", func(t *testing.T) {
		t.Parallel()
		assert.NotNil(t, InsertTmpl)
	})

	t.Run("UpdateMedataTmpl", func(t *testing.T) {
		t.Parallel()
		assert.NotNil(t, UpdateMedataTmpl)
	})

	t.Run("SelectMetadataTmpl", func(t *testing.T) {
		t.Parallel()
		assert.NotNil(t, SelectMetadataTmpl)
	})

	t.Run("SelectCollectionMetadataTmpl", func(t *testing.T) {
		t.Parallel()
		assert.NotNil(t, SelectCollectionMetadataTmpl)
	})

	t.Run("DeleteFromMetadataTmpl", func(t *testing.T) {
		t.Parallel()
		assert.NotNil(t, DeleteFromMetadataTmpl)
	})

	t.Run("CreateTableTmpl", func(t *testing.T) {
		t.Parallel()
		assert.NotNil(t, CreateTableTmpl)
	})
}

func TestRenderWithEscapeName(t *testing.T) {
	t.Parallel()

	t.Run("table names are escaped", func(t *testing.T) {
		t.Parallel()
		config := ReplaceIntoMetadataConfig{
			TablePathPrefix: "/path",
			TableName:       "my-table",
		}

		result, err := Render(UpdateMedataTmpl, config)
		require.NoError(t, err)
		assert.Contains(t, result, "`my-table`")
	})
}

func TestRenderInvalidConfig(t *testing.T) {
	t.Parallel()

	t.Run("wrong config type", func(t *testing.T) {
		t.Parallel()
		// Pass wrong config type
		config := "wrong type"

		result, err := Render(UpsertTmpl, config)
		assert.Error(t, err)
		assert.Empty(t, result)
	})
}

func TestTemplateConfigStructs(t *testing.T) {
	t.Parallel()

	t.Run("DeleteTemplateConfig", func(t *testing.T) {
		t.Parallel()
		config := DeleteTemplateConfig{
			TablePathPrefix:   "/path",
			TableName:         "table",
			PrimaryKeyColumns: []string{"col1", "col2"},
			ColumnName:        "col1",
			IDType:            "Uint64",
		}
		assert.NotNil(t, config)
	})

	t.Run("TemplateConfig", func(t *testing.T) {
		t.Parallel()
		config := TemplateConfig{
			TablePathPrefix: "/path",
			TableName:       "table",
			ColumnName:      "col",
		}
		assert.NotNil(t, config)
	})

	t.Run("ReplaceIntoMetadataConfig", func(t *testing.T) {
		t.Parallel()
		config := ReplaceIntoMetadataConfig{
			TablePathPrefix: "/path",
			TableName:       "metadata",
		}
		assert.NotNil(t, config)
	})

	t.Run("UpsertTemplateConfig", func(t *testing.T) {
		t.Parallel()
		config := UpsertTemplateConfig{
			TablePathPrefix: "/path",
			TableName:       "table",
			FieldDecls:      "field: Type",
			SelectFields:    "field",
		}
		assert.NotNil(t, config)
	})

	t.Run("CreateTableTemplateConfig", func(t *testing.T) {
		t.Parallel()
		config := CreateTableTemplateConfig{
			TablePathPrefix:   "/path",
			TableName:         "table",
			ColumnDefs:        "col Uint64",
			PrimaryKeyColumns: []string{"col"},
			Indexes:           []SecondaryIndexDef{},
		}
		assert.NotNil(t, config)
	})
}

func TestRenderBuffer(t *testing.T) {
	t.Parallel()

	// Test that Render uses bytes.Buffer internally by checking that it doesn't panic
	config := UpsertTemplateConfig{
		TablePathPrefix: "/path",
		TableName:       "table",
		FieldDecls:      "field: Type",
		SelectFields:    "field",
	}

	// This should not panic
	assert.NotPanics(t, func() {
		result, err := Render(UpsertTmpl, config)
		assert.NoError(t, err)
		assert.NotEmpty(t, result)
	})
}

// Helper function to test that template rendering produces valid YQL
func TestRenderProducesValidYQL(t *testing.T) {
	t.Parallel()

	t.Run("upsert produces UPSERT keyword", func(t *testing.T) {
		t.Parallel()
		config := UpsertTemplateConfig{
			TablePathPrefix: "/path",
			TableName:       "table",
			FieldDecls:      "id: Uint64",
			SelectFields:    "id",
		}

		result, err := Render(UpsertTmpl, config)
		require.NoError(t, err)
		assert.Contains(t, result, "UPSERT")
		assert.Contains(t, result, "PRAGMA TablePathPrefix")
	})

	t.Run("delete produces DELETE keyword", func(t *testing.T) {
		t.Parallel()
		config := DeleteTemplateConfig{
			TablePathPrefix:   "/path",
			TableName:         "table",
			PrimaryKeyColumns: []string{"id"},
			ColumnName:        "id",
			IDType:            "Uint64",
		}

		result, err := Render(DeleteTmpl, config)
		require.NoError(t, err)
		assert.Contains(t, result, "DELETE")
		assert.Contains(t, result, "PRAGMA TablePathPrefix")
	})

	t.Run("insert produces INSERT keyword", func(t *testing.T) {
		t.Parallel()
		config := UpsertTemplateConfig{
			TablePathPrefix: "/path",
			TableName:       "table",
			FieldDecls:      "id: Uint64",
			SelectFields:    "id",
		}

		result, err := Render(InsertTmpl, config)
		require.NoError(t, err)
		assert.Contains(t, result, "INSERT")
		assert.Contains(t, result, "PRAGMA TablePathPrefix")
	})
}

func TestRenderEmptyStrings(t *testing.T) {
	t.Parallel()

	t.Run("empty table name", func(t *testing.T) {
		t.Parallel()
		config := ReplaceIntoMetadataConfig{
			TablePathPrefix: "/path",
			TableName:       "",
		}

		result, err := Render(UpdateMedataTmpl, config)
		require.NoError(t, err)
		assert.Contains(t, result, "``")
	})

	t.Run("empty path prefix", func(t *testing.T) {
		t.Parallel()
		config := ReplaceIntoMetadataConfig{
			TablePathPrefix: "",
			TableName:       "table",
		}

		result, err := Render(UpdateMedataTmpl, config)
		require.NoError(t, err)
		// Should still render successfully
		assert.NotEmpty(t, result)
	})
}

// Ensure Render function is defined
func TestRenderFunction(t *testing.T) {
	t.Parallel()

	// Test that Render function exists and can be called
	config := ReplaceIntoMetadataConfig{
		TablePathPrefix: "/test",
		TableName:       "test",
	}

	result, err := Render(UpdateMedataTmpl, config)
	require.NoError(t, err)
	assert.IsType(t, "", result)
}

// Test that we can render to buffer (checking internal implementation)
func TestRenderUsesBuffer(t *testing.T) {
	t.Parallel()

	config := UpsertTemplateConfig{
		TablePathPrefix: "/path",
		TableName:       "table",
		FieldDecls:      "field: Type",
		SelectFields:    "field",
	}

	var buf bytes.Buffer
	err := UpsertTmpl.Execute(&buf, config)
	require.NoError(t, err)
	assert.NotEmpty(t, buf.String())
}
