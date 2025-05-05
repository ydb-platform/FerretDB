package metadata

import (
	"github.com/FerretDB/FerretDB/internal/backends"
	"regexp"
)

const (
	// YDB table name where FerretDB metadata is stored.
	metadataTableName = backends.ReservedPrefix + "database_metadata"

	maxObjectNameLength = 255
	// DefaultColumn is a column name for all fields.
	DefaultColumn   = "_jsonb"
	DefaultIDColumn = "id"

	// RecordIDColumn is a name for RecordID column to store capped collection record id.
	RecordIDColumn = backends.ReservedPrefix + "record_id"
)

// specialCharacters are unsupported characters of YDB scheme object name that are replaced with `_`.
var specialCharacters = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

// Collection represents collection metadata.
//
// Collection value should be immutable to avoid data races.
// Use [deepCopy] to replace the whole value instead of modifying fields of existing value.
type Collection struct {
	Name      string
	UUID      string
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
		UUID:      c.UUID,
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
func (c Collection) Capped() bool {
	return c.Settings.CappedSize > 0
}
