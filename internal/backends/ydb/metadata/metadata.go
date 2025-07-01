package metadata

import (
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	// YDB table name where FerretDB metadata is stored.
	metadataTableName = backends.ReservedPrefix + "database_metadata"
	// DefaultColumn is a column name for all fields.
	DefaultColumn   = "_jsonb"
	DefaultIdColumn = "id"
	IdHashColumn    = "id_hash"
	IdMongoField    = "_id"
	// RecordIDColumn is a name for RecordID column to store capped collection record id.
	RecordIDColumn = backends.ReservedPrefix + "record_id"
)

// Collection represents collection metadata.
//
// Collection value should be immutable to avoid data races.
// Use [deepCopy] to replace the whole value instead of modifying fields of existing value.
type Collection struct {
	Name      string
	TableName string
	Indexes   Indexes
	Settings  Settings
}

// deepCopy returns a deep copy.
func (c *Collection) deepCopy() *Collection {
	if c == nil {
		return nil
	}

	return &Collection{
		Name:      c.Name,
		TableName: c.TableName,
		Indexes:   c.Indexes.deepCopy(),
		Settings:  *c.Settings.deepCopy(),
	}
}

// Settings represents collection settings.
type Settings struct {
	CappedSize      int64 `json:"cappedSize"`
	CappedDocuments int64 `json:"cappedDocuments"`
}

// deepCopy returns a deep copy.
func (s *Settings) deepCopy() *Settings {
	if s == nil {
		return nil
	}

	return &Settings{
		CappedSize:      s.CappedSize,
		CappedDocuments: s.CappedDocuments,
	}
}

// Capped returns true if collection is capped.
func (c *Collection) Capped() bool {
	return c.Settings.CappedSize > 0
}

type PrimaryKeyColumn struct {
	Name string
	Type ydbTypes.Type
}

func BuildPrimaryKeyColumns() []PrimaryKeyColumn {
	var columns []PrimaryKeyColumn

	columns = []PrimaryKeyColumn{
		{Name: IdHashColumn, Type: ydbTypes.TypeUint64},
	}

	for _, suffix := range ColumnOrder {
		columnName := fmt.Sprintf("%s_%s", IdMongoField, suffix)
		columnType := ydbTypes.Optional(ColumnStoreToYdbType(suffix))

		columns = append(columns, PrimaryKeyColumn{
			Name: columnName,
			Type: columnType,
		})
	}

	return columns
}
