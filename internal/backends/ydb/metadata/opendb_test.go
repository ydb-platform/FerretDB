package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestAuthConstants tests the authentication constants
func TestAuthConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{
			name:     "static credentials constant",
			constant: StaticCredentials,
			expected: "static",
		},
		{
			name:     "service account file constant",
			constant: ServiceAccountFile,
			expected: "sa_file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.constant)
		})
	}
}

// TestAuthConstantsNotEmpty tests that auth constants are non-empty
func TestAuthConstantsNotEmpty(t *testing.T) {
	t.Parallel()

	assert.NotEmpty(t, StaticCredentials, "StaticCredentials should not be empty")
	assert.NotEmpty(t, ServiceAccountFile, "ServiceAccountFile should not be empty")
}

// TestAuthConstantsUnique tests that auth constants are unique
func TestAuthConstantsUnique(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, StaticCredentials, ServiceAccountFile,
		"StaticCredentials and ServiceAccountFile should be different")
}
