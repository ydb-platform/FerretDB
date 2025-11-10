package transaction

import "github.com/ydb-platform/ydb-go-sdk/v3/table"

var (
	OnlineReadTx = table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)

	StaleReadTx = table.StaleReadOnlyTxControl()

	WriteTx = table.SerializableReadWriteTxControl(
		table.CommitTx(),
	)
)
