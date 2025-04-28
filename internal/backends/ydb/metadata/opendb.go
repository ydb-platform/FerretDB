package metadata

import (
	"context"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
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

func openDB(dsn *url.URL, _ *slog.Logger, _ *state.Provider) (*ydb.Driver, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log := zerolog.New(os.Stdout).With().Timestamp().Logger()

	opts := []ydb.Option{
		ydbZerolog.WithTraces(
			&log,
			trace.DetailsAll,
		),
	}

	if dsn.Scheme == "grpcs" {
		if dsn.User != nil {
			username := dsn.User.Username()
			password, _ := dsn.User.Password()

			if username != "" && password != "" {
				opts = append(opts, ydb.WithStaticCredentials(username, password))
			}
		}
	}

	driver, err := ydb.Open(ctx, dsn.String(), opts...)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if dsn.Scheme == "grpcs" {
		whoAmI, err := driver.Discovery().WhoAmI(ctx)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		log.Print(whoAmI)
	}

	return driver, nil
}
