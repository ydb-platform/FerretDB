package metadata

import "regexp"

const (
	maxObjectNameLength = 255
)

// objectNameCharacters are unsupported characters of YDB scheme object name that are replaced with `_`.
var objectNameCharacters = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

// columnNameCharacters are unsupported characters of YDB column name that are replaced with `_`.
var columnNameCharacters = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
