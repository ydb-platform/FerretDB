package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata/transaction"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"hash/fnv"
	"log/slog"
)

// Parts of Prometheus metric names.
const (
	namespace = "ferretdb"
	subsystem = "ydb_metadata"
)

const (
	defaultBatchSize = 1000
	localDBName      = "local"
)

func shouldSkipDatabase(dbName string) bool {
	return dbName == localDBName || dbName == ""
}

func (r *Registry) storeCollectionMetadata(ctx context.Context, params *CollectionCreateParams, ydbPath, tableName string) (*Collection, error) {
	collection := &Collection{
		Name:      params.Name,
		TableName: tableName,
		Indexes: []IndexInfo{
			{
				Name:          backends.DefaultIndexName,
				SanitizedName: backends.DefaultIndexName,
				Ready:         true,
				Key:           []IndexKeyPair{{Field: IdMongoField}},
				Unique:        true,
			},
		},
		Settings: Settings{
			CappedSize:      params.CappedSize,
			CappedDocuments: params.CappedDocuments,
		},
	}

	collection.Indexes = append(collection.Indexes, params.Indexes...)

	jsonData, err := json.Marshal(collection)
	if err != nil {
		return nil, err
	}

	q, _ := Render(UpdateMedataTmpl, ReplaceIntoMetadataConfig{
		TablePathPrefix: ydbPath,
		TableName:       metadataTableName,
	})

	var p *Placeholder

	err = r.D.Driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, res, err := s.Execute(ctx, transaction.WriteTx, q, table.NewQueryParameters(
			table.ValueParam(p.Named("id"), ydbTypes.BytesValueFromString(collection.Name)),
			table.ValueParam(p.Named("json"), ydbTypes.JSONValueFromBytes(jsonData)),
		))
		if err != nil {
			return err
		}

		err = res.Err()
		if err != nil {
			return err
		}

		return res.Close()
	},
		table.WithIdempotent(),
	)

	return collection, err
}

func (r *Registry) collectionExists(dbName, collectionName string) bool {
	colls := r.colls[dbName]
	return colls != nil && colls[collectionName] != nil
}

// Capped returns true if capped collection creation is requested.
func (ccp *CollectionCreateParams) Capped() bool {
	return ccp.CappedSize > 0
}

func (r *Registry) initDirectory(ctx context.Context, ydbPath string) error {
	exists, err := sugar.IsDirectoryExists(ctx, r.D.Driver.Scheme(), ydbPath)
	if err != nil {
		return lazyerrors.Error(err)
	}

	if !exists {
		err = r.D.Driver.Scheme().MakeDirectory(ctx, ydbPath)
		if err != nil {
			return lazyerrors.Error(err)
		}
	}

	return nil
}

// loadMetadataPage loads a single page of collection metadata from YDB
// Returns empty=true when no more data is available
func (r *Registry) loadMetadataPage(ctx context.Context, ydbPath, dbName string, limit int, lastKey *string) (empty bool, err error) {
	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			var (
				res      result.Result
				jsonData string
			)

			q, _ := Render(SelectMetadataTmpl, TemplateConfig{
				TablePathPrefix: ydbPath,
				TableName:       metadataTableName,
				ColumnName:      DefaultColumn,
			})

			_, res, err = s.Execute(ctx, transaction.StaleReadTx, q,
				table.NewQueryParameters(
					table.ValueParam("$limit", ydbTypes.Uint64Value(uint64(limit))),
					table.ValueParam("$lastKey", ydbTypes.BytesValueFromString(*lastKey)),
				),
			)

			if err != nil {
				return err
			}

			defer func() {
				_ = res.Close()
			}()

			if !res.NextResultSet(ctx) || !res.HasNextRow() {
				empty = true

				return res.Err()
			}

			colls := map[string]*Collection{}

			for res.NextRow() {
				err = res.ScanNamed(
					named.Required(DefaultIdColumn, lastKey),
					named.OptionalWithDefault(DefaultColumn, &jsonData),
				)
				if err != nil {
					return err
				}

				var c Collection
				if err = json.Unmarshal([]byte(jsonData), &c); err != nil {
					return err
				}
				colls[c.Name] = &c
			}

			r.colls[dbName] = colls

			return res.Err()
		})

	if err != nil {
		if IsOperationErrorTableNotFound(err) {
			r.D.l.Error("table not found for db", slog.String("db", dbName))
			return true, nil
		}
		return empty, err
	}

	return empty, err
}

func (r *Registry) fetchCollectionMetadata(ctx context.Context, ydbPath, dbName, collectionName string) (err error) {
	err = r.D.Driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			var (
				res      result.Result
				jsonData string
			)

			q, err := Render(SelectCollectionMetadataTmpl, TemplateConfig{
				TablePathPrefix: ydbPath,
				TableName:       metadataTableName,
				ColumnName:      DefaultColumn,
			})

			var p Placeholder

			_, res, err = s.Execute(ctx, transaction.StaleReadTx, q,
				table.NewQueryParameters(
					table.ValueParam(p.Named("id"), ydbTypes.BytesValueFromString(collectionName)),
				),
			)

			if err != nil {
				return err
			}

			defer func() {
				_ = res.Close()
			}()

			err = res.NextResultSetErr(ctx)
			if err != nil {
				return err
			}

			if !res.NextRow() {
				if m := r.colls[dbName]; m != nil {
					delete(m, collectionName)
				}
				return nil
			}

			err = res.ScanNamed(
				named.OptionalWithDefault(DefaultColumn, &jsonData),
			)
			if err != nil {
				return err
			}

			var c Collection
			if err = json.Unmarshal([]byte(jsonData), &c); err != nil {
				return err
			}

			if r.colls == nil {
				r.colls = make(map[string]map[string]*Collection)
			}
			if r.colls[dbName] == nil {
				r.colls[dbName] = make(map[string]*Collection)
			}
			r.colls[dbName][collectionName] = &c

			return res.Err()
		})

	if err != nil {
		if IsOperationErrorTableNotFound(err) {
			r.D.l.Error("table not found for db", slog.String("db", dbName))
			return nil
		}
		return err
	}

	return nil
}

func fnv32Hash(s string) uint32 {
	h := fnv.New32a()
	must.NotFail(h.Write([]byte(s)))
	return h.Sum32()
}

func generateIndexName(originalName string) string {
	sanitized := objectNameCharacters.ReplaceAllString(originalName, "_")

	hash := fnv32Hash(originalName)
	hashSuffix := fmt.Sprintf("_%08x_idx", hash)

	maxBaseLength := maxObjectNameLength/2 - len(hashSuffix)
	if len(sanitized) > maxBaseLength {
		sanitized = sanitized[:maxBaseLength]
	}

	return fmt.Sprintf("%s%s", sanitized, hashSuffix)
}
