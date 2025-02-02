package ydb

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata"
	"github.com/FerretDB/FerretDB/internal/util/state"
)

// backend implements backends.Backend interface.
type backend struct {
	r *metadata.Registry
}

// NewBackendParams represents the parameters of NewBackend function.
//
//nolint:vet // for readability
type NewBackendParams struct {
	URI string
	L   *slog.Logger
	P   *state.Provider
	_   struct{} // prevent unkeyed literals
}

// NewBackend creates a new Backend.
func NewBackend(params *NewBackendParams) (backends.Backend, error) {
	// TODO: implement me
	r, err := metadata.NewRegistry(params.URI, params.L, params.P)
	if err != nil {
		return nil, err
	}

	return backends.BackendContract(&backend{
		r: r,
	}), nil
}

// Close implements backends.Backend interface.
func (b *backend) Close() {
	b.r.Close()
}

// Status implements backends.Backend interface.
func (b *backend) Status(ctx context.Context, params *backends.StatusParams) (*backends.StatusResult, error) {
	panic("implement me")
}

// Database implements backends.Backend interface.
func (b *backend) Database(name string) (backends.Database, error) {
	panic("implement me")
}

// ListDatabases implements backends.Backend interface.
//
//nolint:lll // for readability
func (b *backend) ListDatabases(ctx context.Context, params *backends.ListDatabasesParams) (*backends.ListDatabasesResult, error) {
	panic("implement me")
}

// DropDatabase implements backends.Backend interface.
func (b *backend) DropDatabase(ctx context.Context, params *backends.DropDatabaseParams) error {
	panic("implement me")
}

// Describe implements prometheus.Collector.
func (b *backend) Describe(ch chan<- *prometheus.Desc) {
	panic("implement me")
}

// Collect implements prometheus.Collector.
func (b *backend) Collect(ch chan<- prometheus.Metric) {
	panic("implement me")
}

// check interfaces
var (
	_ backends.Backend = (*backend)(nil)
)
