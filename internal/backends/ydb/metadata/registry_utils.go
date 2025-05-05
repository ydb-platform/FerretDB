package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata/transaction"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"path"
)

func (r *Registry) storeCollectionMetadata(ctx context.Context, params *CollectionCreateParams, ydbPath, tableName string) (*Collection, error) {
	collection := &Collection{
		Name:      params.Name,
		UUID:      uuid.New().String(),
		TableName: tableName,
		Indexes: []IndexInfo{
			{
				Name:   fmt.Sprintf("_%s_", DefaultIDColumn),
				Ready:  true,
				Key:    []IndexKeyPair{{Field: "_id"}},
				Unique: true,
			},
		},
		Settings: Settings{
			CappedSize:      params.CappedSize,
			CappedDocuments: params.CappedDocuments,
		},
	}

	jsonData, err := json.Marshal(collection)
	if err != nil {
		return nil, err
	}

	q, _ := Render(UpdateMedataTmpl, ReplaceIntoMetadataConfig{
		TablePathPrefix: ydbPath,
		TableName:       metadataTableName,
	})

	err = r.D.Driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, res, err := s.Execute(ctx, transaction.WriteTx, q, table.NewQueryParameters(
			table.ValueParam("$meta_id", ydbTypes.UuidValue(uuid.MustParse(collection.UUID))),
			table.ValueParam("$json", ydbTypes.JSONValueFromBytes(jsonData)),
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

func (r *Registry) createTable(ctx context.Context, ydbPath, tableName string, isCapped bool) error {
	columns := []options.CreateTableOption{
		options.WithColumn(DefaultColumn, ydbTypes.TypeJSON),
		options.WithColumn("id_hash", ydbTypes.TypeString),
		options.WithColumn(DefaultIDColumn, ydbTypes.TypeString),
		//options.WithColumn("id_type", ydbTypes.TypeString),
	}

	if isCapped {
		columns = append(columns, options.WithColumn(RecordIDColumn, ydbTypes.TypeInt64))
	}

	return r.D.Driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, path.Join(ydbPath, tableName),
			append(columns,
				options.WithPrimaryKeyColumn("id_hash"),
				options.WithPrimaryKeyColumn(DefaultIDColumn))...,
		)
	})
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
func (r *Registry) loadMetadataPage(ctx context.Context, ydbPath, dbName string, limit int, lastKey *uuid.UUID) (empty bool, err error) {
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

			_, res, err = s.Execute(ctx, transaction.ReadTx, q,
				table.NewQueryParameters(
					table.ValueParam("$limit", ydbTypes.Uint64Value(uint64(limit))),
					table.ValueParam("$lastKey", ydbTypes.UuidValue(*lastKey)),
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
			var lastProcessedID uuid.UUID

			for res.NextRow() {
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

				lastProcessedID = uuid.MustParse(c.UUID)
				colls[c.Name] = &c
			}

			r.colls[dbName] = colls
			*lastKey = lastProcessedID

			return res.Err()
		})

	return empty, err
}
