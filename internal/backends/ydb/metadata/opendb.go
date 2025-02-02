package metadata

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/FerretDB/FerretDB/internal/util/state"
)

func openDB(dsn string, _ *slog.Logger, _ *state.Provider) (*ydb.Driver, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	driver, err := ydb.Open(ctx, dsn)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}

	return driver, nil
}

// checkConnection checks that connection works and YDB settings are what we expect.
func checkConnection(ctx context.Context, driver *ydb.Driver) error {
	panic("implement me")
}
