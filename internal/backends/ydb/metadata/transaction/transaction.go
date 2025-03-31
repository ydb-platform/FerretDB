package transaction

import "github.com/ydb-platform/ydb-go-sdk/v3/table"

var (
	ReadTx = table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)

	WriteTx = table.SerializableReadWriteTxControl(
		table.CommitTx(),
	)
)
