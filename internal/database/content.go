package database

import (
	"fmt"
	. "github.com/RedHatInsights/xjoin-validation/internal/record"
	"github.com/go-errors/errors"
	"strings"
)

func (d *DBClient) GetRowsByIDs(ids []string) (records []map[string]interface{}, err error) {
	cols := strings.Join(d.Config.ParsedAvroSchema.DatabaseColumns, ",")

	idsString, err := formatIdsList(ids)
	if err != nil {
		return records, errors.Wrap(err, 0)
	}

	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE ID IN (%s) ORDER BY id",
		cols, d.Config.Table, idsString)

	rows, err := d.connection.Queryx(query)
	defer d.closeRows(rows)

	if err != nil {
		return records, errors.Wrap(fmt.Errorf("error executing query %s, %w", query, err), 0)
	}

	for rows.Next() {
		record := map[string]interface{}{}
		for _, col := range d.Config.ParsedAvroSchema.DatabaseColumns {
			record[col] = nil
		}

		err = rows.MapScan(record)

		if err != nil {
			return records, err
		}

		record = map[string]interface{}{d.Config.ParsedAvroSchema.RootNode: record}

		recordParser := RecordParser{
			Record:           record,
			ParsedAvroSchema: d.Config.ParsedAvroSchema,
		}
		record, err = recordParser.Parse()
		if err != nil {
			return records, errors.Wrap(err, 0)
		}

		records = append(records, record)
	}

	return
}
