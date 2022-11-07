package internal

import "time"

type ESClient struct {
}

func (e *ESClient) CountIndex() (count int, err error) {
	return -1, nil
}

func (e *ESClient) GetIDsByModifiedOn(startTime time.Time, endTime time.Time) (ids []string, err error) {
	return
}

func (e *ESClient) GetIDsByIDList(ids []string) (responseIds []string, err error) {
	return
}
