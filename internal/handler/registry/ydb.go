package registry

import (
	"github.com/FerretDB/FerretDB/internal/backends/ydb"
	"github.com/FerretDB/FerretDB/internal/handler"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/logging"
)

// init registers "ydb" handler.
func init() {
	registry["ydb"] = func(opts *NewHandlerOpts) (*handler.Handler, CloseBackendFunc, error) {
		b, err := ydb.NewBackend(&ydb.NewBackendParams{
			URI:       opts.YdbURL,
			L:         logging.WithName(opts.Logger, "ydb"),
			P:         opts.StateProvider,
			BatchSize: opts.BatchSize,
		})
		if err != nil {
			return nil, nil, lazyerrors.Error(err)
		}

		handlerOpts := &handler.NewOpts{
			Backend:     b,
			TCPHost:     opts.TCPHost,
			ReplSetName: opts.ReplSetName,

			SetupDatabase: opts.SetupDatabase,
			SetupUsername: opts.SetupUsername,
			SetupPassword: opts.SetupPassword,
			SetupTimeout:  opts.SetupTimeout,

			L:             logging.WithName(opts.Logger, "ydb"),
			ConnMetrics:   opts.ConnMetrics,
			StateProvider: opts.StateProvider,

			DisablePushdown:         opts.DisablePushdown,
			EnableNestedPushdown:    opts.EnableNestedPushdown,
			CappedCleanupPercentage: opts.CappedCleanupPercentage,
			CappedCleanupInterval:   opts.CappedCleanupInterval,
			EnableNewAuth:           opts.EnableNewAuth,
			BatchSize:               opts.BatchSize,
			MaxBsonObjectSizeBytes:  opts.MaxBsonObjectSizeBytes,
		}

		h, err := handler.New(handlerOpts)
		if err != nil {
			return nil, b.Close, lazyerrors.Error(err)
		}

		return h, b.Close, nil
	}
}
