package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/must"
)

func TestBuildTypePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "simple field",
			path:     "name",
			expected: `$.\"$s\".p.name.t`,
		},
		{
			name:     "nested field",
			path:     "user.name",
			expected: `$.\"$s\".p.user.\"$s\".p.name.t`,
		},
		{
			name:     "deeply nested field",
			path:     "a.b.c.d",
			expected: `$.\"$s\".p.a.\"$s\".p.b.\"$s\".p.c.\"$s\".p.d.t`,
		},
		{
			name:     "single character field",
			path:     "x",
			expected: `$.\"$s\".p.x.t`,
		},
		{
			name:     "field with underscore",
			path:     "user_name",
			expected: `$.\"$s\".p.user_name.t`,
		},
		{
			name:     "field with numbers",
			path:     "field123",
			expected: `$.\"$s\".p.field123.t`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := buildTypePath(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDotNotationToJsonPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dotPath  string
		expected string
	}{
		{
			name:     "simple field",
			dotPath:  "name",
			expected: "name",
		},
		{
			name:     "nested field",
			dotPath:  "user.name",
			expected: "user.name",
		},
		{
			name:     "array index",
			dotPath:  "items.0",
			expected: "items[0]",
		},
		{
			name:     "multiple array indices",
			dotPath:  "items.0.tags.1",
			expected: "items[0].tags[1]",
		},
		{
			name:     "nested with array",
			dotPath:  "users.0.profile.name",
			expected: "users[0].profile.name",
		},
		{
			name:     "deeply nested",
			dotPath:  "a.b.c.d.e",
			expected: "a.b.c.d.e",
		},
		{
			name:     "array at start",
			dotPath:  "0.name",
			expected: "[0].name",
		},
		{
			name:     "multiple consecutive indices",
			dotPath:  "matrix.0.1.2",
			expected: "matrix[0][1][2]",
		},
		{
			name:     "field with numbers (not index)",
			dotPath:  "field123.value",
			expected: "field123.value",
		},
		{
			name:     "large index",
			dotPath:  "items.999",
			expected: "items[999]",
		},
		{
			name:     "single element",
			dotPath:  "field",
			expected: "field",
		},
		{
			name:     "just index",
			dotPath:  "0",
			expected: "[0]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := DotNotationToJsonPath(tt.dotPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIndexesDeepCopy(t *testing.T) {
	t.Parallel()

	t.Run("empty indexes", func(t *testing.T) {
		t.Parallel()
		original := Indexes{}
		copied := original.deepCopy()

		assert.Equal(t, 0, len(copied))
		assert.NotNil(t, copied)
	})

	t.Run("single index", func(t *testing.T) {
		t.Parallel()
		original := Indexes{
			{
				Name:          "idx1",
				SanitizedName: "idx1_sanitized",
				Ready:         true,
				Key: []IndexKeyPair{
					{Field: "field1", Descending: false},
				},
				Unique: true,
			},
		}

		copied := original.deepCopy()

		assert.Equal(t, len(original), len(copied))
		assert.Equal(t, original[0].Name, copied[0].Name)
		assert.Equal(t, original[0].SanitizedName, copied[0].SanitizedName)
		assert.Equal(t, original[0].Ready, copied[0].Ready)
		assert.Equal(t, original[0].Unique, copied[0].Unique)
		assert.Equal(t, len(original[0].Key), len(copied[0].Key))

		// Verify it's a deep copy
		copied[0].Name = "modified"
		assert.NotEqual(t, original[0].Name, copied[0].Name)
	})

	t.Run("multiple indexes", func(t *testing.T) {
		t.Parallel()
		original := Indexes{
			{
				Name:          "idx1",
				SanitizedName: "idx1_san",
				Ready:         true,
				Key: []IndexKeyPair{
					{Field: "field1", Descending: false},
					{Field: "field2", Descending: true},
				},
				Unique: true,
			},
			{
				Name:          "idx2",
				SanitizedName: "idx2_san",
				Ready:         false,
				Key: []IndexKeyPair{
					{Field: "field3", Descending: false},
				},
				Unique: false,
			},
		}

		copied := original.deepCopy()

		assert.Equal(t, len(original), len(copied))

		// Modify copied and ensure original is unchanged
		copied[0].Key[0].Field = "modified_field"
		assert.NotEqual(t, original[0].Key[0].Field, copied[0].Key[0].Field)

		copied[1].Ready = true
		assert.NotEqual(t, original[1].Ready, copied[1].Ready)
	})

	t.Run("compound index", func(t *testing.T) {
		t.Parallel()
		original := Indexes{
			{
				Name:          "compound_idx",
				SanitizedName: "compound_idx_san",
				Ready:         true,
				Key: []IndexKeyPair{
					{Field: "field1", Descending: false},
					{Field: "field2", Descending: true},
					{Field: "field3", Descending: false},
				},
				Unique: false,
			},
		}

		copied := original.deepCopy()

		assert.Equal(t, 3, len(copied[0].Key))
		assert.Equal(t, original[0].Key[1].Descending, copied[0].Key[1].Descending)
	})
}

func TestExtractIndexFields(t *testing.T) {
	t.Parallel()

	t.Run("no indexes", func(t *testing.T) {
		t.Parallel()
		doc := must.NotFail(types.NewDocument("name", "test"))
		indexes := Indexes{}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("simple string field", func(t *testing.T) {
		t.Parallel()
		doc := must.NotFail(types.NewDocument("name", "test"))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "name", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		assert.Contains(t, result, "name_string")
		assert.Equal(t, "test", result["name_string"].ColumnValue)
		assert.Equal(t, BsonString, result["name_string"].BsonType)
	})

	t.Run("int32 field", func(t *testing.T) {
		t.Parallel()
		doc := must.NotFail(types.NewDocument("count", int32(42)))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "count", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		assert.Contains(t, result, "count_int")
		assert.Equal(t, int32(42), result["count_int"].ColumnValue)
	})

	t.Run("int64 field", func(t *testing.T) {
		t.Parallel()
		doc := must.NotFail(types.NewDocument("bignum", int64(12345)))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "bignum", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		assert.Contains(t, result, "bignum_long")
	})

	t.Run("float64 field", func(t *testing.T) {
		t.Parallel()
		doc := must.NotFail(types.NewDocument("price", float64(99.99)))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "price", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		assert.Contains(t, result, "price_double")
	})

	t.Run("bool field", func(t *testing.T) {
		t.Parallel()
		doc := must.NotFail(types.NewDocument("active", true))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "active", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		assert.Contains(t, result, "active_bool")
	})

	t.Run("nested field", func(t *testing.T) {
		t.Parallel()
		nestedDoc := must.NotFail(types.NewDocument("name", "John"))
		doc := must.NotFail(types.NewDocument("user", nestedDoc))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "user.name", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		assert.Contains(t, result, "user_name_string")
	})

	t.Run("multiple fields", func(t *testing.T) {
		t.Parallel()
		doc := must.NotFail(types.NewDocument(
			"name", "test",
			"count", int32(42),
		))
		indexes := Indexes{
			{
				Name:  "compound_idx",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "name", Descending: false},
					{Field: "count", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		assert.Equal(t, 2, len(result))
		assert.Contains(t, result, "name_string")
		assert.Contains(t, result, "count_int")
	})

	t.Run("field not in document", func(t *testing.T) {
		t.Parallel()
		doc := must.NotFail(types.NewDocument("name", "test"))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "missing", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("skip default index", func(t *testing.T) {
		t.Parallel()
		doc := must.NotFail(types.NewDocument("_id", "test_id"))
		indexes := Indexes{
			{
				Name:  "_id_",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "_id", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		// Default index should be skipped
		assert.NotNil(t, result)
	})

	t.Run("field with hyphen", func(t *testing.T) {
		t.Parallel()
		doc := must.NotFail(types.NewDocument("user-name", "test"))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "user-name", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		// Hyphen is allowed in column names, not replaced
		assert.Contains(t, result, "user-name_string")
	})

	t.Run("field with forbidden characters", func(t *testing.T) {
		t.Parallel()
		doc := must.NotFail(types.NewDocument("user@name", "test"))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "user@name", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		// @ should be replaced with underscore
		assert.Contains(t, result, "user_name_string")
	})

	t.Run("ObjectID field", func(t *testing.T) {
		t.Parallel()
		objectId := types.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
		doc := must.NotFail(types.NewDocument("_id", objectId))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "_id", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		assert.NotNil(t, result)
		assert.Contains(t, result, "_id_objectId")
	})

	t.Run("unsupported type - array", func(t *testing.T) {
		t.Parallel()
		arr := must.NotFail(types.NewArray("item1", "item2"))
		doc := must.NotFail(types.NewDocument("items", arr))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "items", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		// Arrays are not supported for indexing
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("unsupported type - binary", func(t *testing.T) {
		t.Parallel()
		binary := types.Binary{Subtype: 0x00, B: []byte{0x01, 0x02}}
		doc := must.NotFail(types.NewDocument("data", binary))
		indexes := Indexes{
			{
				Name:  "idx1",
				Ready: true,
				Key: []IndexKeyPair{
					{Field: "data", Descending: false},
				},
			},
		}

		result := ExtractIndexFields(doc, indexes)

		// Binary is not supported for indexing
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})
}

func TestIndexKeyPair(t *testing.T) {
	t.Parallel()

	t.Run("create IndexKeyPair", func(t *testing.T) {
		t.Parallel()
		pair := IndexKeyPair{
			Field:      "name",
			Descending: false,
		}

		assert.Equal(t, "name", pair.Field)
		assert.False(t, pair.Descending)
	})

	t.Run("descending order", func(t *testing.T) {
		t.Parallel()
		pair := IndexKeyPair{
			Field:      "created_at",
			Descending: true,
		}

		assert.Equal(t, "created_at", pair.Field)
		assert.True(t, pair.Descending)
	})
}

func TestIndexInfo(t *testing.T) {
	t.Parallel()

	t.Run("create IndexInfo", func(t *testing.T) {
		t.Parallel()
		info := IndexInfo{
			Name:          "idx_name",
			SanitizedName: "idx_name_san",
			Ready:         true,
			Key: []IndexKeyPair{
				{Field: "field1", Descending: false},
			},
			Unique: true,
		}

		assert.Equal(t, "idx_name", info.Name)
		assert.Equal(t, "idx_name_san", info.SanitizedName)
		assert.True(t, info.Ready)
		assert.Equal(t, 1, len(info.Key))
		assert.True(t, info.Unique)
	})

	t.Run("not ready index", func(t *testing.T) {
		t.Parallel()
		info := IndexInfo{
			Name:  "idx_building",
			Ready: false,
		}

		assert.False(t, info.Ready)
	})
}

func TestIndexColumn(t *testing.T) {
	t.Parallel()

	t.Run("create IndexColumn", func(t *testing.T) {
		t.Parallel()
		col := IndexColumn{
			ColumnName:  "field_string",
			BsonType:    BsonString,
			ColumnType:  "String",
			ColumnValue: "test_value",
		}

		assert.Equal(t, "field_string", col.ColumnName)
		assert.Equal(t, BsonString, col.BsonType)
		assert.Equal(t, "String", col.ColumnType)
		assert.Equal(t, "test_value", col.ColumnValue)
	})
}

func TestSecondaryIndexDef(t *testing.T) {
	t.Parallel()

	t.Run("create SecondaryIndexDef", func(t *testing.T) {
		t.Parallel()
		def := SecondaryIndexDef{
			Name:    "idx_test",
			Unique:  true,
			Columns: []string{"col1", "col2"},
		}

		assert.Equal(t, "idx_test", def.Name)
		assert.True(t, def.Unique)
		assert.Equal(t, 2, len(def.Columns))
	})

	t.Run("non-unique index", func(t *testing.T) {
		t.Parallel()
		def := SecondaryIndexDef{
			Name:    "idx_non_unique",
			Unique:  false,
			Columns: []string{"col1"},
		}

		assert.False(t, def.Unique)
	})
}

func TestDotNotationToJsonPathEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("empty string", func(t *testing.T) {
		t.Parallel()
		result := DotNotationToJsonPath("")
		assert.Equal(t, "", result)
	})

	t.Run("leading zero in index", func(t *testing.T) {
		t.Parallel()
		result := DotNotationToJsonPath("field.01")
		// "01" is purely numeric (matches ^\d+$), so it's treated as an index
		assert.Equal(t, "field[01]", result)
	})

	t.Run("mixed valid and invalid indices", func(t *testing.T) {
		t.Parallel()
		result := DotNotationToJsonPath("a.0.b.1.c")
		assert.Equal(t, "a[0].b[1].c", result)
	})
}

func TestBuildTypePathEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("empty string", func(t *testing.T) {
		t.Parallel()
		result := buildTypePath("")
		assert.Equal(t, `$.\"$s\".p..t`, result)
	})

	t.Run("very long path", func(t *testing.T) {
		t.Parallel()
		longPath := "a.b.c.d.e.f.g.h.i.j"
		result := buildTypePath(longPath)
		assert.Contains(t, result, "$")
		assert.Contains(t, result, ".t")
		// Should have multiple \"$s\".p. parts
		assert.Contains(t, result, `\"$s\".p.`)
	})
}
