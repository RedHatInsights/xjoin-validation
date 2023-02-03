package database

import (
	"bytes"
	"fmt"
	"text/template"
	"time"
)

func (d *DBClient) GetIDsByModifiedOn(start time.Time, end time.Time) (ids []string, err error) {
	//TODO: parse name of id and modified_on fields from avro schema
	query := fmt.Sprintf(
		`SELECT id FROM %s WHERE modified_on > '%s' AND modified_on < '%s' ORDER BY id `,
		d.Config.Table, start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano))

	return d.queryIds(query)
}

func (d *DBClient) GetIDsByIDList(ids []string) (responseIds []string, err error) {
	idsString, err := formatIdsList(ids)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`SELECT id FROM %s WHERE id in (%s)`, d.Config.Table, idsString)
	return d.queryIds(query)
}

func (d *DBClient) queryIds(query string) ([]string, error) {
	rows, err := d.runQuery(query)
	defer d.closeRows(rows)

	var ids []string

	if err != nil {
		return ids, err
	}

	for rows.Next() {
		var id string
		err = rows.Scan(&id)

		if err != nil {
			return ids, err
		}

		ids = append(ids, id)
	}

	return ids, nil
}

func formatIdsList(ids []string) (string, error) {
	idsMap := make(map[string]interface{})
	idsMap["IDs"] = ids

	tmpl, err := template.New("ids").Parse(`{{range $idx, $id := .IDs}}'{{$id}}',{{end}}`)
	if err != nil {
		return "", err
	}

	var idsTemplateBuffer bytes.Buffer
	err = tmpl.Execute(&idsTemplateBuffer, idsMap)
	if err != nil {
		return "", err
	}
	idsTemplateParsed := idsTemplateBuffer.String()
	idsTemplateParsed = idsTemplateParsed[0 : len(idsTemplateParsed)-1]
	return idsTemplateParsed, nil
}
