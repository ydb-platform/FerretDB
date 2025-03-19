package metadata

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/FerretDB/FerretDB/internal/util/state"
)

func openDB(dsn *url.URL, l *slog.Logger, _ *state.Provider) (*ydb.Driver, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	l.Info(dsn.String())

	// user := dsn.User
	// username := user.Username()
	// password, _ := user.Password()
	dsn.User = nil

	l.Info(dsn.String())
	driver, err := ydb.Open(ctx, dsn.String())
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}

	return driver, nil
}

// checkConnection checks that connection works and YDB settings are what we expect.
func checkConnection(ctx context.Context, driver *ydb.Driver, l *slog.Logger) error {
	_, err := driver.Discovery().WhoAmI(ctx)
	if err != nil {
		return err
	}

	return nil
}
