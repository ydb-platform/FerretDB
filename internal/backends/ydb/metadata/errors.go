package metadata

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

const tableNotFoundCode = 2003
const conflictExistingKeyCode = 2012

func IsOperationErrorTableNotFound(err error) (isNotFound bool) {
	if ydb.IsOperationError(err, Ydb.StatusIds_SCHEME_ERROR) {
		ydb.IterateByIssues(err, func(_ string, code Ydb.StatusIds_StatusCode, severity uint32) {
			isNotFound = isNotFound || (code == tableNotFoundCode)
		})
		isNotFound = true
	}
	return isNotFound
}

func IsOperationErrorConflictExistingKey(err error) (keyExists bool) {
	if ydb.IsOperationError(err, Ydb.StatusIds_SCHEME_ERROR) {
		ydb.IterateByIssues(err, func(_ string, code Ydb.StatusIds_StatusCode, severity uint32) {
			keyExists = keyExists || (code == conflictExistingKeyCode)
		})
		keyExists = true
	}
	return keyExists
}
