package metadata

import (
	"context"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/logging"
	"github.com/rs/zerolog"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	yc "github.com/ydb-platform/ydb-go-yc"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/FerretDB/FerretDB/internal/util/state"
	ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
)

const (
	StaticCredentials  = "static"
	ServiceAccountFile = "sa_file"
)

func openDB(dsn *url.URL, auth, file string, l *slog.Logger, sp *state.Provider) (*ydb.Driver, error) {
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
		case StaticCredentials:
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
		case ServiceAccountFile:
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

	{
		ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		row, err := driver.Query().QueryRow(ctxTimeout,
			`SELECT Version() AS version`,
			query.WithIdempotent(),
		)
		if err != nil {
			l.ErrorContext(ctxTimeout, "openDB: failed to get YDB version", logging.Error(err))
		} else {
			var version string

			if err := row.Scan(&version); err != nil {
				l.ErrorContext(ctxTimeout, "openDB: failed to scan version", logging.Error(err))
			} else {
				if sp.Get().BackendVersion != version {
					if err := sp.Update(func(s *state.State) {
						s.BackendName = "YDB"
						s.BackendVersion = version
					}); err != nil {
						l.ErrorContext(ctxTimeout, "openDB: failed to update state", logging.Error(err))
					}
				}
			}
		}
	}

	return driver, nil
}
