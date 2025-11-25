package metadata

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsOperationErrorTableNotFound(t *testing.T) {
	t.Parallel()

	t.Run("nil error", func(t *testing.T) {
		t.Parallel()
		result := IsOperationErrorTableNotFound(nil)
		assert.False(t, result)
	})

	t.Run("regular error", func(t *testing.T) {
		t.Parallel()
		err := errors.New("some error")
		result := IsOperationErrorTableNotFound(err)
		assert.False(t, result)
	})

	t.Run("wrapped error", func(t *testing.T) {
		t.Parallel()
		err := errors.New("table not found")
		wrapped := errors.Join(err, errors.New("another error"))
		result := IsOperationErrorTableNotFound(wrapped)
		assert.False(t, result)
	})
}

func TestIsOperationErrorConflictExistingKey(t *testing.T) {
	t.Parallel()

	t.Run("nil error", func(t *testing.T) {
		t.Parallel()
		result := IsOperationErrorConflictExistingKey(nil)
		assert.False(t, result)
	})

	t.Run("regular error", func(t *testing.T) {
		t.Parallel()
		err := errors.New("some error")
		result := IsOperationErrorConflictExistingKey(err)
		assert.False(t, result)
	})

	t.Run("wrapped error", func(t *testing.T) {
		t.Parallel()
		err := errors.New("key exists")
		wrapped := errors.Join(err, errors.New("another error"))
		result := IsOperationErrorConflictExistingKey(wrapped)
		assert.False(t, result)
	})
}

func TestErrorCodeConstants(t *testing.T) {
	t.Parallel()

	t.Run("tableNotFoundCode", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 2003, tableNotFoundCode)
	})

	t.Run("conflictExistingKeyCode", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 2012, conflictExistingKeyCode)
	})
}
