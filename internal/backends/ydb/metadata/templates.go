package metadata

import (
	"bytes"
	"text/template"
)

const (
	DeleteTemplate = `
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
					DECLARE $IDs AS List<String>;

					$to_delete = (
							SELECT id
							FROM {{ .TableName }}
							WHERE id IN $IDs
					);

					$count = (
							SELECT COUNT(*) AS deleted_count
							FROM $to_delete
						);


					DELETE FROM {{ .TableName }} ON
					SELECT * FROM $to_delete;

					SELECT deleted_count FROM $count;
			`

	FetchJsonTemplate = `
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			
			SELECT
				_jsonb
			FROM
				{{ .TableName }}
			WHERE
				JSON_EXISTS(_jsonb, "$.{{ .ColumnName }}")
			LIMIT 1;
	`

	DeleteFromMetadataTemplate = `
					PRAGMA TablePathPrefix("%v");

					DECLARE $meta_id AS Uuid;
			
					DELETE FROM %s WHERE meta_id=$meta_id
	`

	ReplaceIntoMetadataTemplate = `
				PRAGMA TablePathPrefix("%v");

				DECLARE $meta_id AS Uuid;
				DECLARE $json AS JsonDocument;

				REPLACE INTO %s (meta_id, _jsonb) VALUES ($meta_id, $json);
	`

	SelectMetadataTemplate = `
				PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
				SELECT {{ .ColumnName }} FROM {{ .TableName }}
			`
)

type TemplateConfig struct {
	TablePathPrefix string
	TableName       string
	ColumnName      string
	ColumnType      string
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		panic(err)
	}

	return buf.String()
}
