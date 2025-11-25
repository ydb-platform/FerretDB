package ydb

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/FerretDB/FerretDB/internal/types"
)

func TestConvertJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    any
		validate func(t *testing.T, result any)
	}{
		{
			name:  "nil value",
			input: nil,
			validate: func(t *testing.T, result any) {
				assert.Equal(t, types.Null, result)
			},
		},
		{
			name:  "string value",
			input: "test string",
			validate: func(t *testing.T, result any) {
				assert.Equal(t, "test string", result)
			},
		},
		{
			name:  "float64 value",
			input: 42.5,
			validate: func(t *testing.T, result any) {
				assert.Equal(t, 42.5, result)
			},
		},
		{
			name:  "bool value",
			input: true,
			validate: func(t *testing.T, result any) {
				assert.Equal(t, true, result)
			},
		},
		{
			name: "map value",
			input: map[string]any{
				"name": "test",
				"age":  float64(25),
			},
			validate: func(t *testing.T, result any) {
				doc, ok := result.(*types.Document)
				require.True(t, ok, "result should be *types.Document")
				assert.Equal(t, 2, doc.Len())

				name, err := doc.Get("name")
				require.NoError(t, err)
				assert.Equal(t, "test", name)

				age, err := doc.Get("age")
				require.NoError(t, err)
				assert.Equal(t, float64(25), age)
			},
		},
		{
			name: "array value",
			input: []any{
				"string",
				float64(42),
				true,
			},
			validate: func(t *testing.T, result any) {
				arr, ok := result.(*types.Array)
				require.True(t, ok, "result should be *types.Array")
				assert.Equal(t, 3, arr.Len())

				val0, err := arr.Get(0)
				require.NoError(t, err)
				assert.Equal(t, "string", val0)

				val1, err := arr.Get(1)
				require.NoError(t, err)
				assert.Equal(t, float64(42), val1)

				val2, err := arr.Get(2)
				require.NoError(t, err)
				assert.Equal(t, true, val2)
			},
		},
		{
			name: "nested map",
			input: map[string]any{
				"outer": map[string]any{
					"inner": "value",
				},
			},
			validate: func(t *testing.T, result any) {
				doc, ok := result.(*types.Document)
				require.True(t, ok)

				outer, err := doc.Get("outer")
				require.NoError(t, err)

				outerDoc, ok := outer.(*types.Document)
				require.True(t, ok)

				inner, err := outerDoc.Get("inner")
				require.NoError(t, err)
				assert.Equal(t, "value", inner)
			},
		},
		{
			name: "nested array",
			input: []any{
				[]any{"nested", float64(1)},
				float64(2),
			},
			validate: func(t *testing.T, result any) {
				arr, ok := result.(*types.Array)
				require.True(t, ok)
				assert.Equal(t, 2, arr.Len())

				nested, err := arr.Get(0)
				require.NoError(t, err)

				nestedArr, ok := nested.(*types.Array)
				require.True(t, ok)
				assert.Equal(t, 2, nestedArr.Len())
			},
		},
		{
			name:  "empty map",
			input: map[string]any{},
			validate: func(t *testing.T, result any) {
				doc, ok := result.(*types.Document)
				require.True(t, ok)
				assert.Equal(t, 0, doc.Len())
			},
		},
		{
			name:  "empty array",
			input: []any{},
			validate: func(t *testing.T, result any) {
				arr, ok := result.(*types.Array)
				require.True(t, ok)
				assert.Equal(t, 0, arr.Len())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := convertJSON(tt.input)
			tt.validate(t, result)
		})
	}
}

func TestUnmarshalExplain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		expectErr bool
		validate  func(t *testing.T, result *types.Document)
	}{
		{
			name:      "empty object",
			input:     `{}`,
			expectErr: false,
			validate: func(t *testing.T, result *types.Document) {
				assert.NotNil(t, result)
				assert.Equal(t, 0, result.Len())
			},
		},
		{
			name: "simple object",
			input: `{
				"query": "SELECT * FROM table",
				"cost": 42.5
			}`,
			expectErr: false,
			validate: func(t *testing.T, result *types.Document) {
				assert.NotNil(t, result)
				assert.Equal(t, 2, result.Len())

				query, err := result.Get("query")
				require.NoError(t, err)
				assert.Equal(t, "SELECT * FROM table", query)

				cost, err := result.Get("cost")
				require.NoError(t, err)
				assert.Equal(t, 42.5, cost)
			},
		},
		{
			name: "nested object",
			input: `{
				"plan": {
					"type": "SeqScan",
					"table": "test_table"
				}
			}`,
			expectErr: false,
			validate: func(t *testing.T, result *types.Document) {
				assert.NotNil(t, result)

				plan, err := result.Get("plan")
				require.NoError(t, err)

				planDoc, ok := plan.(*types.Document)
				require.True(t, ok)

				planType, err := planDoc.Get("type")
				require.NoError(t, err)
				assert.Equal(t, "SeqScan", planType)

				table, err := planDoc.Get("table")
				require.NoError(t, err)
				assert.Equal(t, "test_table", table)
			},
		},
		{
			name: "with array",
			input: `{
				"stages": ["parse", "optimize", "execute"]
			}`,
			expectErr: false,
			validate: func(t *testing.T, result *types.Document) {
				assert.NotNil(t, result)

				stages, err := result.Get("stages")
				require.NoError(t, err)

				stagesArr, ok := stages.(*types.Array)
				require.True(t, ok)
				assert.Equal(t, 3, stagesArr.Len())

				val0, err := stagesArr.Get(0)
				require.NoError(t, err)
				assert.Equal(t, "parse", val0)
			},
		},
		{
			name: "complex nested structure",
			input: `{
				"query_plan": {
					"nodes": [
						{
							"type": "SeqScan",
							"cost": 100.0
						},
						{
							"type": "Filter",
							"cost": 50.5
						}
					],
					"total_cost": 150.5
				}
			}`,
			expectErr: false,
			validate: func(t *testing.T, result *types.Document) {
				assert.NotNil(t, result)

				queryPlan, err := result.Get("query_plan")
				require.NoError(t, err)

				queryPlanDoc, ok := queryPlan.(*types.Document)
				require.True(t, ok)

				nodes, err := queryPlanDoc.Get("nodes")
				require.NoError(t, err)

				nodesArr, ok := nodes.(*types.Array)
				require.True(t, ok)
				assert.Equal(t, 2, nodesArr.Len())

				totalCost, err := queryPlanDoc.Get("total_cost")
				require.NoError(t, err)
				assert.Equal(t, 150.5, totalCost)
			},
		},
		{
			name: "with null value",
			input: `{
				"value": null,
				"name": "test"
			}`,
			expectErr: false,
			validate: func(t *testing.T, result *types.Document) {
				assert.NotNil(t, result)

				value, err := result.Get("value")
				require.NoError(t, err)
				assert.Equal(t, types.Null, value)

				name, err := result.Get("name")
				require.NoError(t, err)
				assert.Equal(t, "test", name)
			},
		},
		{
			name: "with boolean values",
			input: `{
				"enabled": true,
				"disabled": false
			}`,
			expectErr: false,
			validate: func(t *testing.T, result *types.Document) {
				assert.NotNil(t, result)

				enabled, err := result.Get("enabled")
				require.NoError(t, err)
				assert.Equal(t, true, enabled)

				disabled, err := result.Get("disabled")
				require.NoError(t, err)
				assert.Equal(t, false, disabled)
			},
		},
		{
			name:      "invalid json",
			input:     `{invalid json}`,
			expectErr: true,
			validate:  nil,
		},
		{
			name: "empty array in document",
			input: `{
				"items": []
			}`,
			expectErr: false,
			validate: func(t *testing.T, result *types.Document) {
				assert.NotNil(t, result)

				items, err := result.Get("items")
				require.NoError(t, err)

				itemsArr, ok := items.(*types.Array)
				require.True(t, ok)
				assert.Equal(t, 0, itemsArr.Len())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := UnmarshalExplain(tt.input)

			if tt.expectErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tt.validate != nil {
				tt.validate(t, result)
			}
		})
	}
}

func TestConvertJSONWithRealWorldExample(t *testing.T) {
	t.Parallel()

	// Simulate a real YDB explain plan structure
	realWorldJSON := `{
		"Plan": {
			"Node Type": "ResultSet",
			"PlanNodeId": 0,
			"Plans": [
				{
					"Node Type": "Limit",
					"Operators": [
						{
							"Name": "Limit",
							"Limit": "1001"
						}
					],
					"PlanNodeId": 1,
					"Plans": [
						{
							"Node Type": "UnionAll",
							"PlanNodeId": 2
						}
					]
				}
			],
			"Stats": {
				"TotalDuration": 100.5,
				"ExecutionTime": 50.25
			}
		}
	}`

	result, err := UnmarshalExplain(realWorldJSON)
	require.NoError(t, err)
	assert.NotNil(t, result)

	plan, err := result.Get("Plan")
	require.NoError(t, err)

	planDoc, ok := plan.(*types.Document)
	require.True(t, ok)

	nodeType, err := planDoc.Get("Node Type")
	require.NoError(t, err)
	assert.Equal(t, "ResultSet", nodeType)

	stats, err := planDoc.Get("Stats")
	require.NoError(t, err)

	statsDoc, ok := stats.(*types.Document)
	require.True(t, ok)

	totalDuration, err := statsDoc.Get("TotalDuration")
	require.NoError(t, err)
	assert.Equal(t, 100.5, totalDuration)
}

func TestConvertJSONDeepNesting(t *testing.T) {
	t.Parallel()

	// Test deeply nested structure
	input := map[string]any{
		"level1": map[string]any{
			"level2": map[string]any{
				"level3": map[string]any{
					"level4": map[string]any{
						"value": "deep",
					},
				},
			},
		},
	}

	result := convertJSON(input)
	doc, ok := result.(*types.Document)
	require.True(t, ok)

	level1, err := doc.Get("level1")
	require.NoError(t, err)
	level1Doc, ok := level1.(*types.Document)
	require.True(t, ok)

	level2, err := level1Doc.Get("level2")
	require.NoError(t, err)
	level2Doc, ok := level2.(*types.Document)
	require.True(t, ok)

	level3, err := level2Doc.Get("level3")
	require.NoError(t, err)
	level3Doc, ok := level3.(*types.Document)
	require.True(t, ok)

	level4, err := level3Doc.Get("level4")
	require.NoError(t, err)
	level4Doc, ok := level4.(*types.Document)
	require.True(t, ok)

	value, err := level4Doc.Get("value")
	require.NoError(t, err)
	assert.Equal(t, "deep", value)
}

func TestConvertJSONMixedTypes(t *testing.T) {
	t.Parallel()

	input := map[string]any{
		"string": "text",
		"number": 123.456,
		"bool":   true,
		"null":   nil,
		"array":  []any{float64(1), "two", false, nil},
		"nested": map[string]any{"key": "value"},
	}

	result := convertJSON(input)
	doc, ok := result.(*types.Document)
	require.True(t, ok)
	assert.Equal(t, 6, doc.Len())

	str, err := doc.Get("string")
	require.NoError(t, err)
	assert.Equal(t, "text", str)

	num, err := doc.Get("number")
	require.NoError(t, err)
	assert.Equal(t, 123.456, num)

	b, err := doc.Get("bool")
	require.NoError(t, err)
	assert.Equal(t, true, b)

	n, err := doc.Get("null")
	require.NoError(t, err)
	assert.Equal(t, types.Null, n)

	arr, err := doc.Get("array")
	require.NoError(t, err)
	arrTyped, ok := arr.(*types.Array)
	require.True(t, ok)
	assert.Equal(t, 4, arrTyped.Len())

	nested, err := doc.Get("nested")
	require.NoError(t, err)
	nestedTyped, ok := nested.(*types.Document)
	require.True(t, ok)
	assert.Equal(t, 1, nestedTyped.Len())
}

func TestUnmarshalExplainWithSpecialCharacters(t *testing.T) {
	t.Parallel()

	input := `{
		"query": "SELECT * FROM \"table\" WHERE field = 'value'",
		"description": "Test with \"quotes\" and 'apostrophes'"
	}`

	result, err := UnmarshalExplain(input)
	require.NoError(t, err)
	assert.NotNil(t, result)

	query, err := result.Get("query")
	require.NoError(t, err)
	assert.Contains(t, query.(string), "SELECT")
	assert.Contains(t, query.(string), "table")
}

func TestConvertJSONPreservesOrder(t *testing.T) {
	t.Parallel()

	// JSON doesn't guarantee order, but we should handle it correctly
	jsonStr := `{"a": 1, "b": 2, "c": 3}`

	var data map[string]any
	err := json.Unmarshal([]byte(jsonStr), &data)
	require.NoError(t, err)

	result := convertJSON(data)
	doc, ok := result.(*types.Document)
	require.True(t, ok)
	assert.Equal(t, 3, doc.Len())

	// Just verify all keys are present
	_, err = doc.Get("a")
	assert.NoError(t, err)
	_, err = doc.Get("b")
	assert.NoError(t, err)
	_, err = doc.Get("c")
	assert.NoError(t, err)
}

func TestConvertJSONEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("very large number", func(t *testing.T) {
		t.Parallel()
		result := convertJSON(float64(1e308))
		assert.Equal(t, float64(1e308), result)
	})

	t.Run("very small number", func(t *testing.T) {
		t.Parallel()
		result := convertJSON(float64(1e-308))
		assert.Equal(t, float64(1e-308), result)
	})

	t.Run("empty string in map key", func(t *testing.T) {
		t.Parallel()
		input := map[string]any{
			"": "empty key",
		}
		result := convertJSON(input)
		doc, ok := result.(*types.Document)
		require.True(t, ok)

		val, err := doc.Get("")
		require.NoError(t, err)
		assert.Equal(t, "empty key", val)
	})

	t.Run("unicode string", func(t *testing.T) {
		t.Parallel()
		result := convertJSON("Hello 世界 🌍")
		assert.Equal(t, "Hello 世界 🌍", result)
	})

	t.Run("array with mixed nested types", func(t *testing.T) {
		t.Parallel()
		input := []any{
			map[string]any{"key": "value"},
			[]any{float64(1), float64(2)},
			"string",
			float64(42),
			true,
			nil,
		}
		result := convertJSON(input)
		arr, ok := result.(*types.Array)
		require.True(t, ok)
		assert.Equal(t, 6, arr.Len())
	})

	t.Run("deeply nested with mixed types", func(t *testing.T) {
		t.Parallel()
		input := map[string]any{
			"level1": map[string]any{
				"level2": []any{
					map[string]any{
						"level3": "deep value",
					},
				},
			},
		}
		result := convertJSON(input)
		doc, ok := result.(*types.Document)
		require.True(t, ok)
		assert.NotNil(t, doc)
	})
}

func TestUnmarshalExplainEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("very large JSON", func(t *testing.T) {
		t.Parallel()
		// Create a large JSON object
		var builder strings.Builder
		builder.WriteString(`{"fields":[`)
		for i := 0; i < 1000; i++ {
			if i > 0 {
				builder.WriteString(",")
			}
			builder.WriteString(`{"id":`)
			builder.WriteString(string(rune('0' + (i % 10))))
			builder.WriteString(`,"value":"test"}`)
		}
		builder.WriteString(`]}`)

		result, err := UnmarshalExplain(builder.String())
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("JSON with unicode keys", func(t *testing.T) {
		t.Parallel()
		input := `{"имя": "значение", "名前": "値", "🔑": "🌍"}`

		result, err := UnmarshalExplain(input)
		require.NoError(t, err)
		assert.NotNil(t, result)

		val, err := result.Get("имя")
		require.NoError(t, err)
		assert.Equal(t, "значение", val)
	})

	t.Run("JSON with escaped characters", func(t *testing.T) {
		t.Parallel()
		input := `{"key": "value with \"quotes\" and \\backslashes\\ and \nnewline"}`

		result, err := UnmarshalExplain(input)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("JSON with numbers in scientific notation", func(t *testing.T) {
		t.Parallel()
		input := `{"small": 1.23e-10, "large": 4.56e20}`

		result, err := UnmarshalExplain(input)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("empty string", func(t *testing.T) {
		t.Parallel()
		_, err := UnmarshalExplain("")
		assert.Error(t, err)
	})

	t.Run("array at root level", func(t *testing.T) {
		t.Parallel()
		_, err := UnmarshalExplain(`[1, 2, 3]`)
		assert.Error(t, err)
	})

	t.Run("string at root level", func(t *testing.T) {
		t.Parallel()
		_, err := UnmarshalExplain(`"just a string"`)
		assert.Error(t, err)
	})

	t.Run("number at root level", func(t *testing.T) {
		t.Parallel()
		_, err := UnmarshalExplain(`42`)
		assert.Error(t, err)
	})
}

func TestConvertJSONWithLargeArrays(t *testing.T) {
	t.Parallel()

	// Create an array with many elements
	largeArray := make([]any, 10000)
	for i := range largeArray {
		largeArray[i] = float64(i)
	}

	result := convertJSON(largeArray)
	arr, ok := result.(*types.Array)
	require.True(t, ok)
	assert.Equal(t, 10000, arr.Len())

	// Verify first and last elements
	first, err := arr.Get(0)
	require.NoError(t, err)
	assert.Equal(t, float64(0), first)

	last, err := arr.Get(9999)
	require.NoError(t, err)
	assert.Equal(t, float64(9999), last)
}

func TestConvertJSONWithLargeDocuments(t *testing.T) {
	t.Parallel()

	// Create a document with many fields
	largeDoc := make(map[string]any, 1000)
	for i := 0; i < 1000; i++ {
		key := "field_" + string(rune('0'+(i%10))) + string(rune('0'+((i/10)%10))) + string(rune('0'+((i/100)%10)))
		largeDoc[key] = float64(i)
	}

	result := convertJSON(largeDoc)
	doc, ok := result.(*types.Document)
	require.True(t, ok)
	assert.Equal(t, 1000, doc.Len())
}
