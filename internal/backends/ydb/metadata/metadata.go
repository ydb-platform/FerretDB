package metadata

import (
	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
)

const (
	// DefaultColumn is a column name for all fields.
	DefaultColumn = "_jsonb"

	// RecordIDColumn is a name for RecordID column to store capped collection record id.
	RecordIDColumn = backends.ReservedPrefix + "record_id"
)

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

// marshal returns [*types.Document] for that collection.
func (c *Collection) marshal() *types.Document {
	return must.NotFail(types.NewDocument(
		"_id", c.Name,
		"uuid", c.UUID,
		"table", c.TableName,
		"indexes", c.Indexes.marshal(),
		"cappedSize", c.Settings.CappedSize,
		"cappedDocs", c.Settings.CappedDocuments,
	))
}

// unmarshal sets collection metadata from [*types.Document].
func (c *Collection) unmarshal(doc *types.Document) error {
	v, _ := doc.Get("_id")
	c.Name, _ = v.(string)

	if c.Name == "" {
		return lazyerrors.New("collection name is empty")
	}

	v, _ = doc.Get("table")
	c.TableName, _ = v.(string)

	if c.TableName == "" {
		return lazyerrors.New("table name is empty")
	}

	v, _ = doc.Get("indexes")
	i, _ := v.(*types.Array)

	if i == nil {
		return lazyerrors.New("indexes are empty")
	}

	if err := c.Indexes.unmarshal(i); err != nil {
		return lazyerrors.Error(err)
	}

	// those fields do not exist in older versions of FerretDB

	if v, _ := doc.Get("uuid"); v != nil {
		c.UUID = v.(string)
	}

	if v, _ := doc.Get("cappedSize"); v != nil {
		c.Settings.CappedSize = v.(int64)
	}

	if v, _ := doc.Get("cappedDocs"); v != nil {
		c.Settings.CappedDocuments = v.(int64)
	}

	return nil
}
