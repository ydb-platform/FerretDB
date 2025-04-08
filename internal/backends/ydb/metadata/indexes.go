package metadata

import (
	"context"
	"errors"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends/ydb/metadata/transaction"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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
	Name   string
	Key    []IndexKeyPair
	Unique bool
}

// IndexKeyPair consists of a field name and a sort order that are part of the index.
type IndexKeyPair struct {
	Field      string
	YdbType    any
	Descending bool
}

type IndexColumn struct {
	ColumnName  string
	ColumnType  any
	ColumnValue any
}

// deepCopy returns a deep copy.
func (indexes Indexes) deepCopy() Indexes {
	res := make(Indexes, len(indexes))

	for i, index := range indexes {
		res[i] = IndexInfo{
			Name:   index.Name,
			Key:    slices.Clone(index.Key),
			Unique: index.Unique,
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

		v, _ = index.Get("unique")
		unique, _ := v.(bool)

		res[i] = IndexInfo{
			Name:   must.NotFail(index.Get("name")).(string),
			Key:    key,
			Unique: unique,
		}
	}

	*s = res

	return nil
}

const fetchJsonTemplate = `
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			
			SELECT
				_jsonb
			FROM
				{{ .TableName }}
			WHERE
				JSON_EXISTS(_jsonb, "$.{{ .ColumnName }}")
			LIMIT 1;
`

func SelectJsonField(ctx context.Context, c table.Client, prefix, tableName, column string) (string, error) {
	q := buildSelectJsonQuery(prefix, tableName, column)

	var jsonData string

	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			_, res, err := s.Execute(ctx, transaction.ReadTx, q, table.NewQueryParameters())
			if err != nil {
				return err
			}

			defer res.Close()

			if err = res.NextResultSetErr(ctx); err != nil {
				return err
			}

			for res.NextRow() {
				err = res.ScanNamed(named.OptionalWithDefault(DefaultColumn, &jsonData))
				if err != nil {
					return err
				}
			}

			return res.Err()
		},
	)
	if err != nil {
		return jsonData, lazyerrors.Error(err)
	}

	return jsonData, nil
}

func buildSelectJsonQuery(prefix string, tableName string, column string) string {
	q := render(template.Must(template.New("").Parse(fetchJsonTemplate)),
		templateConfig{
			TablePathPrefix: prefix,
			TableName:       tableName,
			ColumnName:      column,
		},
	)

	return q
}

func detectJsonFieldType(jsonStr string, field string) (ydbTypes.Type, error) {
	if jsonStr == "" {
		return nil, errors.New("empty JSON string")
	}

	ydbType, err := MapToYdbType([]byte(jsonStr), field)
	if err != nil {
		return nil, lazyerrors.Error(err)
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

func dropFieldColumn(ctx context.Context, c table.Client, prefix string, tableName string, column string) error {
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.AlterTable(ctx, path.Join(prefix, tableName),
				options.WithDropColumn(column),
			)
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return fmt.Errorf("unexpected error: %v", err)
	}

	return nil
}

func updateColumnWithExistingValues(ctx context.Context, c table.Client, prefix, tableName, column string, collType ydbTypes.Type) error {
	q := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			UPSERT INTO {{ .TableName }} (id, {{ .ColumnName }})
			SELECT
				id,
				CAST(JSON_VALUE(_jsonb, "$.{{ .ColumnName }}") AS {{ .ColumnType }})
			FROM {{ .TableName }}
			WHERE JSON_EXISTS(_jsonb, "$.{{ .ColumnName }}");
		`)),
		templateConfig{
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
		return lazyerrors.Error(err)
	}

	return nil
}

func dropIndex(ctx context.Context, c table.Client, prefix string, tableName string, column string) error {
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.AlterTable(ctx, path.Join(prefix, tableName),
				options.WithDropIndex(fmt.Sprintf("idx_%s", column)))
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return lazyerrors.Error(err)
	}

	return nil
}
