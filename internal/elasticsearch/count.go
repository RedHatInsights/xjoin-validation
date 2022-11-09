package elasticsearch

import (
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-operator/controllers/utils"
	"io/ioutil"
)

type CountIDsResponse struct {
	Count int `json:"count"`
}

func (e *ESClient) CountIndex() (count int, err error) {
	req := esapi.CountRequest{
		Index: []string{e.index},
	}

	ctx, cancel := utils.DefaultContext()
	defer cancel()
	res, err := req.Do(ctx, e.client)
	if err != nil {
		return count, errors.Wrap(err, 0)
	}

	var countIDsResponse CountIDsResponse
	byteValue, _ := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(byteValue, &countIDsResponse)
	if err != nil {
		return count, errors.Wrap(err, 0)
	}

	return countIDsResponse.Count, nil
}
