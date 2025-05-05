package metadata

import (
	"context"
	"log/slog"
	"net/url"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/resource"
	"github.com/FerretDB/FerretDB/internal/util/state"
)

type DB struct {
	baseURI url.URL
	l       *slog.Logger
	sp      *state.Provider
	Driver  *ydb.Driver
	token   *resource.Token
}

func New(dsn, auth, ca string, l *slog.Logger, sp *state.Provider) (*DB, error) {
	baseURI, err := url.Parse(dsn)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	values := baseURI.Query()
	baseURI.RawQuery = values.Encode()
	driver, err := openDB(baseURI, auth, ca, l, sp)
	if err != nil {
		return nil, err
	}

	p := &DB{
		baseURI: *baseURI,
		l:       l,
		sp:      sp,
		Driver:  driver,
		token:   resource.NewToken(),
	}

	resource.Track(p, p.token)

	return p, nil
}

func (db *DB) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db.Driver.Close(ctx)

	resource.Untrack(db, db.token)
}
