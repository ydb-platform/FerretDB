package metadata

import (
	"context"
	"errors"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata/transaction"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/query"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"log"
	"log/slog"
	"path"
	"slices"
	"text/template"

	"github.com/FerretDB/FerretDB/internal/types"
	"github.com/FerretDB/FerretDB/internal/util/iterator"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	"github.com/FerretDB/FerretDB/internal/util/must"
)

// Indexes represents information about all indexes in a collection.
type Indexes []IndexInfo

// IndexInfo represents information about a single index.
type IndexInfo struct {
	Name    string
	YDBType YDBIndexType
	Key     []IndexKeyPair
	Unique  bool
}

// IndexKeyPair consists of a field name and a sort order that are part of the index.
type IndexKeyPair struct {
	Field      string
	Descending bool
}

type YDBIndexType string

const (
	GlobalIndex YDBIndexType = "GLOBAL"
)

// deepCopy returns a deep copy.
func (indexes Indexes) deepCopy() Indexes {
	res := make(Indexes, len(indexes))

	for i, index := range indexes {
		res[i] = IndexInfo{
			Name:    index.Name,
			YDBType: index.YDBType,
			Key:     slices.Clone(index.Key),
			Unique:  index.Unique,
		}
	}

	return res
}

// marshal returns [*types.Array] for indexes.
func (indexes Indexes) marshal() *types.Array {
	res := types.MakeArray(len(indexes))

	for _, index := range indexes {
		key := types.MakeDocument(len(index.Key))

		for _, pair := range index.Key {
			order := int32(1)
			if pair.Descending {
				slog.Warn("YDB не поддерживает Descending-индексы, поле `%s` будет ASC.", "field", pair.Field)
			}
			key.Set(pair.Field, order)
		}

		res.Append(must.NotFail(types.NewDocument(
			"ydbindex", string(index.YDBType),
			"name", index.Name,
			"key", key,
			"unique", index.Unique,
		)))
	}

	return res
}

// unmarshal sets indexes from [*types.Array].
func (s *Indexes) unmarshal(a *types.Array) error {
	res := make(Indexes, a.Len())

	iter := a.Iterator()
	defer iter.Close()

	for {
		i, v, err := iter.Next()
		if errors.Is(err, iterator.ErrIteratorDone) {
			break
		}

		if err != nil {
			return lazyerrors.Error(err)
		}

		index := v.(*types.Document)

		keyDoc := must.NotFail(index.Get("key")).(*types.Document)
		fields := keyDoc.Keys()
		key := make([]IndexKeyPair, keyDoc.Len())

		for j, f := range fields {
			key[j] = IndexKeyPair{
				Field:      f,
				Descending: false,
			}
		}

		ydbType, _ := index.Get("ydbindex")

		v, _ = index.Get("unique")
		unique, _ := v.(bool)

		res[i] = IndexInfo{
			Name:    must.NotFail(index.Get("name")).(string),
			YDBType: YDBIndexType(ydbType.(string)),
			Key:     key,
			Unique:  unique,
		}
	}

	*s = res

	return nil
}

func (r *Registry) cdcFeed(ctx context.Context, prefix, tableName string) (string, string) {
	topicPath := path.Join(prefix, tableName, "feed")
	consumerName := "test-consumer"

	err := addCdcToTable(
		ctx,
		r.D.Driver.Table(),
		prefix, tableName,
	)
	if err != nil {
		panic(fmt.Errorf("create table error: %w", err))
	}
	log.Println("Adding cdc feed table done")

	log.Println("Create consumer")
	err = r.D.Driver.Topic().Alter(ctx, topicPath, topicoptions.AlterWithAddConsumers(topictypes.Consumer{
		Name: consumerName,
	}))

	if err != nil {
		panic(fmt.Errorf("failed to create feed consumer: %w", err))
	}

	return topicPath, consumerName
}

func addCdcToTable(ctx context.Context, c table.Client, prefix, tableName string) (err error) {
	err = c.Do(ctx, func(ctx context.Context, s table.Session) error {
		q := fmt.Sprintf(`
					PRAGMA TablePathPrefix("%v");
					
					ALTER TABLE
						%v
					ADD CHANGEFEED
						feed
					WITH (
						FORMAT = 'JSON',
						MODE = 'NEW_AND_OLD_IMAGES'
					)
					`,
			prefix, tableName)

		return s.ExecuteSchemeQuery(ctx, q)
	})
	if err != nil {
		return fmt.Errorf("failed to add changefeed to test table: %w", err)
	}

	return nil
}

func cdcRead(ctx context.Context, db *ydb.Driver, consumerName, topicPath string) {
	log.Println("Start cdc read")
	reader, err := db.Topic().StartReader(consumerName, []topicoptions.ReadSelector{{Path: topicPath}})
	if err != nil {
		log.Fatal("failed to start read feed", err)
	}

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			panic(fmt.Errorf("failed to read message: %w", err))
		}

		var event interface{}
		err = topicsugar.JSONUnmarshal(msg, &event)
		if err != nil {
			panic(fmt.Errorf("failed to unmarshal json cdc: %w", err))
		}
		log.Println("new cdc event:", event)
		err = reader.Commit(ctx, msg)
		if err != nil {
			panic(fmt.Errorf("failed to commit message: %w", err))
		}
	}
}

func selectJsonField(ctx context.Context, c table.Client, prefix, tableName, column string) (ydbTypes.Type, error) {
	q := query.Render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			
			SELECT
				_jsonb
			FROM
				{{ .TableName }}
			WHERE
				JSON_EXISTS(_jsonb, "$.{{ .ColumnName }}")
			LIMIT 1;
		`)),
		query.TemplateConfig{
			TablePathPrefix: prefix,
			TableName:       tableName,
			ColumnName:      column,
		},
	)

	var ydbType ydbTypes.Type
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			_, res, err := s.Execute(ctx, transaction.ReadTx, q,
				table.NewQueryParameters(),
			)
			if err != nil {
				return err
			}

			defer func() {
				_ = res.Close()
			}()

			var (
				jsonData string
			)
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					err = res.ScanNamed(
						named.OptionalWithDefault(DefaultColumn, &jsonData),
					)
					if err != nil {
						return err
					}

					ydbType, err = MapToYdbType([]byte(jsonData), column)
				}
			}

			return res.Err()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve: %w", err)
	}

	return ydbType, nil
}

func addFieldColumn(ctx context.Context, c table.Client, prefix string, tableName string, column string, ydbType ydbTypes.Type) error {
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.AlterTable(ctx, path.Join(prefix, tableName),
				options.WithAddColumn(column, ydbTypes.Optional(ydbType)),
			)
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return fmt.Errorf("unexpected error: %v", err)
	}

	return nil
}

func updateColumnWithJsonValues(ctx context.Context, c table.Client, prefix, tableName, column string, collType ydbTypes.Type) error {
	q := query.Render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			UPSERT INTO {{ .TableName }} (id, {{ .ColumnName }})
			SELECT
				id,
				CAST(JSON_VALUE(_jsonb, "$.{{ .ColumnName }}") AS {{ .ColumnType }})
			FROM {{ .TableName }}
			WHERE JSON_EXISTS(_jsonb, "$.{{ .ColumnName }}");
		`)),
		query.TemplateConfig{
			TablePathPrefix: prefix,
			TableName:       tableName,
			ColumnName:      column,
			ColumnType:      collType.String(),
		},
	)

	err := c.DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) (err error) {
			res, err := tx.Execute(ctx, q, table.NewQueryParameters())
			if err != nil {
				return err
			}
			if err = res.Err(); err != nil {
				return err
			}
			return err
		})

	if err != nil {
		return fmt.Errorf("failed to update column %s from JSON: %w", column, err)
	}

	return nil
}

func addIndex(ctx context.Context, c table.Client, prefix string, tableName string, column string) error {
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.AlterTable(ctx, path.Join(prefix, tableName),
				options.WithAddIndex(fmt.Sprintf("idx_%s", column),
					options.WithIndexColumns(column),
					options.WithIndexType(options.GlobalIndex()),
				))
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return fmt.Errorf("unexpected error: %v", err)
	}

	return nil
}
