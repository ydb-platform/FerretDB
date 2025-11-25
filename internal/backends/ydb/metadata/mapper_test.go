package metadata

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/FerretDB/FerretDB/internal/types"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestBsonTypeToYdbType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bsonType BsonType
		expected ydbTypes.Type
	}{
		{
			name:     "string type",
			bsonType: BsonString,
			expected: ydbTypes.TypeString,
		},
		{
			name:     "objectId type",
			bsonType: BsonObjectId,
			expected: ydbTypes.TypeString,
		},
		{
			name:     "int type",
			bsonType: BsonInt,
			expected: ydbTypes.TypeDyNumber,
		},
		{
			name:     "long type",
			bsonType: BsonLong,
			expected: ydbTypes.TypeDyNumber,
		},
		{
			name:     "double type",
			bsonType: BsonDouble,
			expected: ydbTypes.TypeDyNumber,
		},
		{
			name:     "bool type",
			bsonType: BsonBool,
			expected: ydbTypes.TypeBool,
		},
		{
			name:     "date type",
			bsonType: BsonDate,
			expected: ydbTypes.TypeInt64,
		},
		{
			name:     "unknown type",
			bsonType: BsonType("unknown"),
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := BsonTypeToYdbType(tt.bsonType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBsonTypeToColumnStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bsonType BsonType
		expected ColumnAlias
	}{
		{
			name:     "string to string column",
			bsonType: BsonString,
			expected: ColumnString,
		},
		{
			name:     "objectId to objectId column",
			bsonType: BsonObjectId,
			expected: ColumnObjectId,
		},
		{
			name:     "bool to bool column",
			bsonType: BsonBool,
			expected: ColumnBool,
		},
		{
			name:     "date to date column",
			bsonType: BsonDate,
			expected: ColumnDate,
		},
		{
			name:     "int to scalar column",
			bsonType: BsonInt,
			expected: ColumnScalar,
		},
		{
			name:     "long to scalar column",
			bsonType: BsonLong,
			expected: ColumnScalar,
		},
		{
			name:     "double to scalar column",
			bsonType: BsonDouble,
			expected: ColumnScalar,
		},
		{
			name:     "unknown type",
			bsonType: BsonType("unknown"),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := BsonTypeToColumnStore(tt.bsonType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestColumnStoreToYdbType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		column   ColumnAlias
		expected ydbTypes.Type
	}{
		{
			name:     "string column",
			column:   ColumnString,
			expected: ydbTypes.TypeString,
		},
		{
			name:     "objectId column",
			column:   ColumnObjectId,
			expected: ydbTypes.TypeString,
		},
		{
			name:     "bool column",
			column:   ColumnBool,
			expected: ydbTypes.TypeBool,
		},
		{
			name:     "date column",
			column:   ColumnDate,
			expected: ydbTypes.TypeInt64,
		},
		{
			name:     "scalar column",
			column:   ColumnScalar,
			expected: ydbTypes.TypeDyNumber,
		},
		{
			name:     "unknown column",
			column:   ColumnAlias("unknown"),
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ColumnStoreToYdbType(tt.column)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsScalar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		colType  BsonType
		expected bool
	}{
		{
			name:     "int is scalar",
			colType:  BsonInt,
			expected: true,
		},
		{
			name:     "long is scalar",
			colType:  BsonLong,
			expected: true,
		},
		{
			name:     "double is scalar",
			colType:  BsonDouble,
			expected: true,
		},
		{
			name:     "string is not scalar",
			colType:  BsonString,
			expected: false,
		},
		{
			name:     "bool is not scalar",
			colType:  BsonBool,
			expected: false,
		},
		{
			name:     "objectId is not scalar",
			colType:  BsonObjectId,
			expected: false,
		},
		{
			name:     "date is not scalar",
			colType:  BsonDate,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isScalar(tt.colType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBsonValueToYdbValueForJsonQuery(t *testing.T) {
	t.Parallel()

	t.Run("string value", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValueForJsonQuery(BsonString, "test")
		require.NotNil(t, result)
		assert.Equal(t, ydbTypes.UTF8Value("test"), result)
	})

	t.Run("int32 value", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValueForJsonQuery(BsonInt, int32(42))
		require.NotNil(t, result)
		assert.Equal(t, ydbTypes.Int32Value(42), result)
	})

	t.Run("int64 value", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValueForJsonQuery(BsonLong, int64(12345))
		require.NotNil(t, result)
		assert.Equal(t, ydbTypes.Int64Value(12345), result)
	})

	t.Run("float64 value", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValueForJsonQuery(BsonDouble, float64(99.99))
		require.NotNil(t, result)
		assert.Equal(t, ydbTypes.DoubleValue(99.99), result)
	})

	t.Run("bool value", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValueForJsonQuery(BsonBool, true)
		require.NotNil(t, result)
		assert.Equal(t, ydbTypes.BoolValue(true), result)
	})

	t.Run("objectId value", func(t *testing.T) {
		t.Parallel()
		oid := types.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
		result := BsonValueToYdbValueForJsonQuery(BsonObjectId, oid)
		require.NotNil(t, result)
	})

	t.Run("time value", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		result := BsonValueToYdbValueForJsonQuery(BsonDate, now)
		require.NotNil(t, result)
	})

	t.Run("unknown type", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValueForJsonQuery(BsonType("unknown"), "test")
		assert.Nil(t, result)
	})
}

func TestBsonValueToYdbValue(t *testing.T) {
	t.Parallel()

	t.Run("string value", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValue(BsonString, "test")
		require.NotNil(t, result)
	})

	t.Run("int32 value", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValue(BsonInt, int32(42))
		require.NotNil(t, result)
	})

	t.Run("int64 value", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValue(BsonLong, int64(12345))
		require.NotNil(t, result)
	})

	t.Run("float64 value", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValue(BsonDouble, float64(99.99))
		require.NotNil(t, result)
	})

	t.Run("bool value", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValue(BsonBool, true)
		require.NotNil(t, result)
	})

	t.Run("objectId value", func(t *testing.T) {
		t.Parallel()
		oid := types.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
		result := BsonValueToYdbValue(BsonObjectId, oid)
		require.NotNil(t, result)
	})

	t.Run("time value", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		result := BsonValueToYdbValue(BsonDate, now)
		require.NotNil(t, result)
	})

	t.Run("unknown type", func(t *testing.T) {
		t.Parallel()
		result := BsonValueToYdbValue(BsonType("unknown"), "test")
		assert.Nil(t, result)
	})

	t.Run("zero values", func(t *testing.T) {
		t.Parallel()

		// Zero int32
		result := BsonValueToYdbValue(BsonInt, int32(0))
		assert.NotNil(t, result)

		// Zero int64
		result = BsonValueToYdbValue(BsonLong, int64(0))
		assert.NotNil(t, result)

		// Zero float64
		result = BsonValueToYdbValue(BsonDouble, float64(0.0))
		assert.NotNil(t, result)

		// False bool
		result = BsonValueToYdbValue(BsonBool, false)
		assert.NotNil(t, result)

		// Non-empty string
		result = BsonValueToYdbValue(BsonString, "test")
		assert.NotNil(t, result)
	})

	t.Run("negative values", func(t *testing.T) {
		t.Parallel()

		// Negative int32
		result := BsonValueToYdbValue(BsonInt, int32(-42))
		assert.NotNil(t, result)

		// Negative int64
		result = BsonValueToYdbValue(BsonLong, int64(-12345))
		assert.NotNil(t, result)

		// Negative float64
		result = BsonValueToYdbValue(BsonDouble, float64(-99.99))
		assert.NotNil(t, result)
	})
}

func TestFloat64ToOrderedUint64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input float64
	}{
		{
			name:  "positive value",
			input: 123.456,
		},
		{
			name:  "negative value",
			input: -123.456,
		},
		{
			name:  "zero",
			input: 0.0,
		},
		{
			name:  "very small positive",
			input: 0.0000001,
		},
		{
			name:  "very small negative",
			input: -0.0000001,
		},
		{
			name:  "very large positive",
			input: 1e308,
		},
		{
			name:  "very large negative",
			input: -1e308,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := float64ToOrderedUint64(tt.input)
			assert.NotZero(t, result) // Just check it returns something
		})
	}

	t.Run("ordering property", func(t *testing.T) {
		t.Parallel()
		// Smaller floats should produce smaller uint64s
		a := float64ToOrderedUint64(1.0)
		b := float64ToOrderedUint64(2.0)
		assert.Less(t, a, b)

		c := float64ToOrderedUint64(-2.0)
		d := float64ToOrderedUint64(-1.0)
		assert.Less(t, c, d)
	})
}

func TestIndexedBsonTypes(t *testing.T) {
	t.Parallel()

	// Test that all expected types are indexed
	expectedTypes := []BsonType{
		BsonString,
		BsonObjectId,
		BsonBool,
		BsonDate,
		BsonLong,
		BsonDouble,
		BsonInt,
	}

	for _, bsonType := range expectedTypes {
		t.Run(string(bsonType), func(t *testing.T) {
			t.Parallel()
			_, ok := IndexedBsonTypes[bsonType]
			assert.True(t, ok, "Type %s should be indexable", bsonType)
		})
	}
}

func TestColumnOrder(t *testing.T) {
	t.Parallel()

	// Test that ColumnOrder contains expected columns
	assert.Len(t, ColumnOrder, 5)
	assert.Contains(t, ColumnOrder, ColumnString)
	assert.Contains(t, ColumnOrder, ColumnObjectId)
	assert.Contains(t, ColumnOrder, ColumnScalar)
	assert.Contains(t, ColumnOrder, ColumnDate)
	assert.Contains(t, ColumnOrder, ColumnBool)
}
