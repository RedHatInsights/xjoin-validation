package internal

import "time"

type DBClient struct {
}

func (d *DBClient) CountTable() (count int, err error) {
	return
}

func (d *DBClient) GetIDsByModifiedOn(startTime time.Time, endTime time.Time) (ids []string, err error) {
	return
}

func (d *DBClient) GetIDsByIDList(ids []string) (responseIds []string, err error) {
	return
}
