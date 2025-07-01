package metadata

import (
	"bytes"
	"fmt"
	"github.com/FerretDB/FerretDB/internal/backends"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"strings"
	"text/template"
)

var (
	UpsertTmpl                   *template.Template
	DeleteTmpl                   *template.Template
	InsertTmpl                   *template.Template
	UpdateMedataTmpl             *template.Template
	SelectMetadataTmpl           *template.Template
	SelectCollectionMetadataTmpl *template.Template
	DeleteFromMetadataTmpl       *template.Template
	CreateTableTmpl              *template.Template
)

func init() {
	UpsertTmpl = template.Must(template.New("upsert").
		Funcs(template.FuncMap{"escapeName": escapeName}).
		Parse(UpsertTemplate))

	DeleteTmpl = template.Must(template.New("delete").
		Funcs(template.FuncMap{
			"escapeName": escapeName,
			"join":       strings.Join,
		}).
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

	SelectCollectionMetadataTmpl = template.Must(template.New("select_collection_data").
		Funcs(template.FuncMap{"escapeName": escapeName}).
		Parse(ReadCollectionMetadataTemplate))

	DeleteFromMetadataTmpl = template.Must(template.New("delete_metadata").
		Funcs(template.FuncMap{"escapeName": escapeName}).
		Parse(DeleteFromMetadataTemplate))

	CreateTableTmpl = template.Must(template.New("create_table").
		Funcs(template.FuncMap{
			"escapeName": escapeName,
			"join":       strings.Join,
			"sub":        sub,
		}).
		Parse(CreateTableTemplate))

}

const (
	DeleteTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
					DECLARE $f_IDs AS List<{{ .IDType }}>;
					
					$to_delete = (
						SELECT {{ join .PrimaryKeyColumns ", " }}
						FROM {{ .TableName | escapeName}}
						WHERE {{ .ColumnName }} IN $f_IDs
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
					DECLARE $f_id AS String;
			
					DELETE FROM {{ .TableName | escapeName}} WHERE id=$f_id
	`

	UpdateMetadataTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
	
					DECLARE $f_id AS String;
					DECLARE $f_json AS Json;
	
					REPLACE INTO {{ .TableName | escapeName}} (id, _jsonb) 
					VALUES ($f_id, $f_json);
	`

	SelectMetadataTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					DECLARE $limit AS Uint64;
					DECLARE $lastKey AS String;
					
					SELECT id, {{ .ColumnName }} FROM {{ .TableName | escapeName}}
					WHERE id > $lastKey
		
					ORDER BY id
					LIMIT $limit
	`

	ReadCollectionMetadataTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					DECLARE $f_id AS String;
					
					SELECT id, {{ .ColumnName }} FROM {{ .TableName | escapeName}}
					WHERE id == $f_id
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

	CreateTableTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
				
					CREATE TABLE {{ .TableName | escapeName}} (
						_jsonb Json,
						{{ .ColumnDefs }},
						PRIMARY KEY ({{ join .PrimaryKeyColumns ", " }}),

						{{- $len := len .Indexes -}}
						{{- range $i, $index := .Indexes }}
						INDEX {{ $index.Name | escapeName }} GLOBAL {{ if $index.Unique }} UNIQUE {{ end }} ON ({{ join $index.Columns ", " }})
						{{ if ne $i (sub $len 1) }},{{ end }}
						{{- end }}
					)
					WITH (
						AUTO_PARTITIONING_BY_SIZE = ENABLED,
						AUTO_PARTITIONING_BY_LOAD = ENABLED
					);
	`
)

func Render(t *template.Template, data interface{}) (string, error) {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func escapeName(name string) string {
	return fmt.Sprintf("`%s`", name)
}

func NewDeleteConfig(ydbPath, tableName string, pkColumnNames []string, params *backends.DeleteAllParams) DeleteTemplateConfig {
	config := DeleteTemplateConfig{
		TablePathPrefix:   ydbPath,
		TableName:         tableName,
		PrimaryKeyColumns: pkColumnNames,
	}

	if params.RecordIDs != nil {
		config.ColumnName = RecordIDColumn
		config.IDType = ydbTypes.TypeInt64.String()
		config.PrimaryKeyColumns = append(config.PrimaryKeyColumns, RecordIDColumn)
	} else {
		config.ColumnName = IdHashColumn
		config.IDType = ydbTypes.TypeUint64.String()
	}

	return config
}

type DeleteTemplateConfig struct {
	TablePathPrefix   string
	TableName         string
	PrimaryKeyColumns []string
	ColumnName        string
	IDType            string
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

type CreateTableTemplateConfig struct {
	TablePathPrefix   string
	TableName         string
	ColumnDefs        string
	PrimaryKeyColumns []string
	Indexes           []SecondaryIndexDef
}

func sub(a, b int) int {
	return a - b
}
