package metadata

// CollectionCreateParams contains parameters for CollectionCreate.
type CollectionCreateParams struct {
	DBName          string
	Name            string
	Indexes         []IndexInfo
	CappedSize      int64
	CappedDocuments int64
	_               struct{} // prevent unkeyed literals
}

type SelectParams struct {
	Schema  string
	Table   string
	Comment string

	Capped        bool
	OnlyRecordIDs bool
}
