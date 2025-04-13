package metadata

// CollectionCreateParams contains parameters for CollectionCreate.
type CollectionCreateParams struct {
	DBName          string
	Name            string
	CappedSize      int64
	CappedDocuments int64
	_               struct{} // prevent unkeyed literals
}
