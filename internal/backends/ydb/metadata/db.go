package metadata

import (
	"log/slog"
	"net/url"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/resource"
	"github.com/FerretDB/FerretDB/internal/util/state"
)

type DB struct {
	baseURI url.URL
	l       *slog.Logger
	sp      *state.Provider
	driver  *ydb.Driver
	token   *resource.Token
}

func New(dsn string, l *slog.Logger, sp *state.Provider) (*DB, error) {
	baseURI, err := url.Parse(dsn)
	if err != nil {
		return nil, lazyerrors.Error(err)
	}

	values := baseURI.Query()
	baseURI.RawQuery = values.Encode()
	driver, err := openDB(baseURI, l, sp)
	if err != nil {
		return nil, err
	}

	p := &DB{
		baseURI: *baseURI,
		l:       l,
		sp:      sp,
		driver:  driver,
		token:   resource.NewToken(),
	}

	resource.Track(p, p.token)

	return p, nil
}
