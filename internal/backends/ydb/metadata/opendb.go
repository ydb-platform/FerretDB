package metadata

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/FerretDB/FerretDB/internal/util/state"
	ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
)

func openDB(dsn *url.URL, l *slog.Logger, _ *state.Provider) (*ydb.Driver, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log := zerolog.New(os.Stdout).With().Timestamp().Logger()

	driver, err := ydb.Open(ctx,
		dsn.String(),
		ydbZerolog.WithTraces(
			&log,
			trace.DetailsAll,
		),
	)
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
