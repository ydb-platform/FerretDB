package metadata

import (
	"context"
	"log/slog"
	"time"

	"github.com/FerretDB/FerretDB/internal/util/state"
)

type Registry struct {
	d *DB
	l *slog.Logger
}

// NewRegistry creates a registry for YDB databases with a given base URI.
func NewRegistry(dsn string, l *slog.Logger, sp *state.Provider) (*Registry, error) {
	// TODO: implement me
	db, err := New(dsn, l, sp)
	if err != nil {
		return nil, err
	}

	r := &Registry{
		d: db,
		l: l,
	}

	return r, nil
}

// Close closes the registry.
func (r *Registry) Close() {
	// TODO: implement me
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.d.driver.Close(ctx)
}

func (r *Registry) initDBs(ctx context.Context) ([]string, error) {
	panic("implement me")
}
