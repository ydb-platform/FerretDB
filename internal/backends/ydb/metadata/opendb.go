package metadata

import (
	"context"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/rs/zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	yc "github.com/ydb-platform/ydb-go-yc"
	"log/slog"
	"net/url"
	"os"

	"github.com/FerretDB/FerretDB/internal/util/state"
	ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
)

func openDB(dsn *url.URL, auth, file string, _ *slog.Logger, _ *state.Provider) (*ydb.Driver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var opts []ydb.Option

	log := zerolog.New(os.Stdout).With().Timestamp().Logger()
	opts = []ydb.Option{
		ydbZerolog.WithTraces(
			&log,
			trace.DetailsAll,
		),
	}

	useTLS := dsn.Scheme == "grpcs"

	if useTLS {
		switch auth {
		case "static":
			var username, password string

			if dsn.User != nil {
				username = dsn.User.Username()
				password, _ = dsn.User.Password()
			}

			if username != "" && password != "" {
				opts = append(opts, ydb.WithStaticCredentials(username, password))
			}

			if file != "" {
				caData, err := os.ReadFile(file)
				if err != nil {
					return nil, lazyerrors.Error(err)
				}
				opts = append(opts, ydb.WithCertificatesFromPem(caData))
			}
		case "sa_file":
			if file != "" {
				opts = append(opts,
					yc.WithInternalCA(),
					yc.WithServiceAccountKeyFileCredentials(file))
			}
		}
	}

	driver, err := ydb.Open(ctx, dsn.String(), opts...)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	if useTLS {
		whoAmI, err := driver.Discovery().WhoAmI(ctx)
		if err != nil {
			return nil, lazyerrors.Error(err)
		}

		log.Print(whoAmI)
	}

	return driver, nil
}
