package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlaceholderNext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		initial  Placeholder
		expected []string
	}{
		{
			name:     "starts from zero",
			initial:  Placeholder(0),
			expected: []string{"$f1", "$f2", "$f3"},
		},
		{
			name:     "continues from value",
			initial:  Placeholder(5),
			expected: []string{"$f6", "$f7", "$f8"},
		},
		{
			name:     "single call",
			initial:  Placeholder(0),
			expected: []string{"$f1"},
		},
		{
			name:     "many calls",
			initial:  Placeholder(0),
			expected: []string{"$f1", "$f2", "$f3", "$f4", "$f5", "$f6", "$f7", "$f8", "$f9", "$f10"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := tt.initial
			results := make([]string, len(tt.expected))
			for i := range tt.expected {
				results[i] = p.Next()
			}
			assert.Equal(t, tt.expected, results)
		})
	}
}

func TestPlaceholderNamed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple name",
			input:    "id",
			expected: "$f_id",
		},
		{
			name:     "json name",
			input:    "json",
			expected: "$f_json",
		},
		{
			name:     "IDs name",
			input:    "IDs",
			expected: "$f_IDs",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "$f_",
		},
		{
			name:     "with underscore",
			input:    "user_id",
			expected: "$f_user_id",
		},
		{
			name:     "with numbers",
			input:    "field123",
			expected: "$f_field123",
		},
		{
			name:     "special characters",
			input:    "field-name",
			expected: "$f_field-name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := new(Placeholder)
			result := p.Named(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPlaceholderNextDoesNotAffectNamed(t *testing.T) {
	t.Parallel()

	p := new(Placeholder)

	// Call Next several times
	p.Next()
	p.Next()
	p.Next()

	// Named should still work independently
	result := p.Named("test")
	assert.Equal(t, "$f_test", result)

	// And Next should continue its sequence
	next := p.Next()
	assert.Equal(t, "$f4", next)
}

func TestPlaceholderMultipleInstances(t *testing.T) {
	t.Parallel()

	p1 := new(Placeholder)
	p2 := new(Placeholder)

	// Different instances should work independently
	assert.Equal(t, "$f1", p1.Next())
	assert.Equal(t, "$f1", p2.Next())
	assert.Equal(t, "$f2", p1.Next())
	assert.Equal(t, "$f2", p2.Next())
}

func TestPlaceholderLargeNumbers(t *testing.T) {
	t.Parallel()

	p := Placeholder(999)
	result := p.Next()
	assert.Equal(t, "$f1000", result)

	result = p.Next()
	assert.Equal(t, "$f1001", result)
}

func TestPlaceholderZeroValue(t *testing.T) {
	t.Parallel()

	var p Placeholder // zero value
	result := p.Next()
	assert.Equal(t, "$f1", result)
}
