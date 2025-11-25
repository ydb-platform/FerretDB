package ydb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStatsType tests the stats type structure
func TestStatsType(t *testing.T) {
	t.Parallel()

	s := &stats{
		countDocuments:  100,
		sizeIndexes:     1024,
		sizeTables:      2048,
		sizeFreeStorage: 512,
	}

	assert.Equal(t, int64(100), s.countDocuments)
	assert.Equal(t, int64(1024), s.sizeIndexes)
	assert.Equal(t, int64(2048), s.sizeTables)
	assert.Equal(t, int64(512), s.sizeFreeStorage)
}

// TestStatsZeroValues tests stats with zero values
func TestStatsZeroValues(t *testing.T) {
	t.Parallel()

	s := &stats{}

	assert.Equal(t, int64(0), s.countDocuments)
	assert.Equal(t, int64(0), s.sizeIndexes)
	assert.Equal(t, int64(0), s.sizeTables)
	assert.Equal(t, int64(0), s.sizeFreeStorage)
}

// TestStatsNegativeValues tests stats with negative values (edge case)
func TestStatsNegativeValues(t *testing.T) {
	t.Parallel()

	s := &stats{
		countDocuments:  -1,
		sizeIndexes:     -100,
		sizeTables:      -200,
		sizeFreeStorage: -50,
	}

	assert.Equal(t, int64(-1), s.countDocuments)
	assert.Equal(t, int64(-100), s.sizeIndexes)
	assert.Equal(t, int64(-200), s.sizeTables)
	assert.Equal(t, int64(-50), s.sizeFreeStorage)
}

// TestStatsLargeValues tests stats with large values
func TestStatsLargeValues(t *testing.T) {
	t.Parallel()

	s := &stats{
		countDocuments:  9223372036854775807, // max int64
		sizeIndexes:     9223372036854775807,
		sizeTables:      9223372036854775807,
		sizeFreeStorage: 9223372036854775807,
	}

	assert.Equal(t, int64(9223372036854775807), s.countDocuments)
	assert.Equal(t, int64(9223372036854775807), s.sizeIndexes)
	assert.Equal(t, int64(9223372036854775807), s.sizeTables)
	assert.Equal(t, int64(9223372036854775807), s.sizeFreeStorage)
}

