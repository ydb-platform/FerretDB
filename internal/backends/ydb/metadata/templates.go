package metadata

import (
	"bytes"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends"
	"github.com/FerretDB/FerretDB/internal/util/lazyerrors"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"text/template"
)

var (
	UpsertTmpl             *template.Template
	DeleteTmpl             *template.Template
	InsertTmpl             *template.Template
	UpdateMedataTmpl       *template.Template
	SelectMetadataTmpl     *template.Template
	DeleteFromMetadataTmpl *template.Template
)

func init() {
	UpsertTmpl = template.Must(template.New("upsert").
		Funcs(template.FuncMap{"escapeName": escapeName}).
		Parse(UpsertTemplate))

	DeleteTmpl = template.Must(template.New("delete").
		Funcs(template.FuncMap{"escapeName": escapeName}).
		Parse(DeleteTemplate))

	InsertTmpl = template.Must(template.New("insert").
		Funcs(template.FuncMap{"escapeName": escapeName}).
		Parse(InsertTemplate))

	UpdateMedataTmpl = template.Must(template.New("update_metadata").
		Funcs(template.FuncMap{"escapeName": escapeName}).
		Parse(UpdateMetadataTemplate))

	SelectMetadataTmpl = template.Must(template.New("select_metadata").
		Funcs(template.FuncMap{"escapeName": escapeName}).
		Parse(SelectMetadataTemplate))

	DeleteFromMetadataTmpl = template.Must(template.New("delete_metadata").
		Funcs(template.FuncMap{"escapeName": escapeName}).
		Parse(DeleteFromMetadataTemplate))
}

const (
	DeleteTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
					DECLARE $IDs AS List<{{ .IDType }}>;
					
					$to_delete = (
						SELECT {{ .ColumnName }}
						FROM {{ .TableName | escapeName}}
						WHERE {{ .ColumnName }} IN $IDs
					);
					
					$count = (
						SELECT COUNT(*) AS deleted_count
						FROM $to_delete
					);
					
					DELETE FROM {{ .TableName | escapeName}} ON
					SELECT * FROM $to_delete;
					
					SELECT deleted_count FROM $count;
	`

	DeleteFromMetadataTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
					DECLARE $meta_id AS Uuid;
			
					DELETE FROM {{ .TableName | escapeName}} WHERE id=$meta_id
	`

	UpdateMetadataTemplate = `
				PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

				DECLARE $meta_id AS Uuid;
				DECLARE $json AS Json;

				REPLACE INTO {{ .TableName | escapeName}} (id, _jsonb) 
				VALUES ($meta_id, $json);
	`

	SelectMetadataTemplate = `
				PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
				SELECT {{ .ColumnName }} FROM {{ .TableName | escapeName}}
	`

	UpsertTemplate = `
				PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
				
				DECLARE $insertData AS List<Struct<
				{{ .FieldDecls }}
				>>;
				
				UPSERT INTO {{ escapeName .TableName }}
				SELECT
				{{ .SelectFields }}
				FROM AS_TABLE($insertData);
	`

	InsertTemplate = `
				PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
				
				DECLARE $insertData AS List<Struct<
				{{ .FieldDecls }}
				>>;
				
				INSERT INTO {{ .TableName | escapeName}}
				SELECT
				{{ .SelectFields }}
				FROM AS_TABLE($insertData);
	`
)

func Render(t *template.Template, data interface{}) (string, error) {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", lazyerrors.Error(err)
	}

	return buf.String(), nil
}

func escapeName(name string) string {
	return fmt.Sprintf("`%s`", name)
}

func NewDeleteConfig(ydbPath, tableName string, params *backends.DeleteAllParams) DeleteTemplateConfig {
	config := DeleteTemplateConfig{
		TablePathPrefix: ydbPath,
		TableName:       tableName,
	}

	if params.RecordIDs != nil {
		config.ColumnName = RecordIDColumn
		config.IDType = ydbTypes.TypeInt64.String()
	} else {
		config.ColumnName = DefaultIDColumn
		config.IDType = ydbTypes.TypeString.String()
	}

	return config
}

type DeleteTemplateConfig struct {
	TablePathPrefix string
	TableName       string
	ColumnName      string
	IDType          string
}

type TemplateConfig struct {
	TablePathPrefix string
	TableName       string
	ColumnName      string
}

type ReplaceIntoMetadataConfig struct {
	TablePathPrefix string
	TableName       string
}

type UpsertTemplateConfig struct {
	TablePathPrefix string
	TableName       string
	FieldDecls      string
	SelectFields    string
}

type InsertTemplateConfig struct {
	TablePathPrefix string
	TableName       string
	FieldDecls      string
	SelectFields    string
}
