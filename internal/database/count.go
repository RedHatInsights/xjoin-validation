package database

import (
	"fmt"
	"github.com/go-errors/errors"
)

func (d *DBClient) CountTable() (count int, err error) {
	rows, err := d.runQuery(fmt.Sprintf("SELECT count(*) from %s", d.Config.Table))
	defer d.closeRows(rows)

	if err != nil {
		return count, errors.Wrap(err, 0)
	}

	for rows.Next() {
		var row int
		err = rows.Scan(&row)
		if err != nil {
			return -1, err
		}
		count = row
	}

	return
}
