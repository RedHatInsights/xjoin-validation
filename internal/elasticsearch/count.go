package elasticsearch

import (
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
	"io/ioutil"
)

type CountIDsResponse struct {
	Count int `json:"count"`
}

func (e *ESClient) CountIndex() (count int, err error) {
	req := esapi.CountRequest{
		Index: []string{e.index},
	}

	e.log.Debug("Elasticsearch count request", "request", req)

	ctx, cancel := utils.DefaultContext()
	defer cancel()
	res, err := req.Do(ctx, e.client)
	if err != nil {
		return count, errors.Wrap(err, 0)
	}

	if res.StatusCode >= 300 {
		return count, errors.Wrap(fmt.Errorf(
			"invalid response code when counting index: %v", res.StatusCode), 0)
	}

	var countIDsResponse CountIDsResponse
	byteValue, _ := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(byteValue, &countIDsResponse)
	if err != nil {
		return count, errors.Wrap(err, 0)
	}

	e.log.Debug("Elasticsearch count response", "response", res, "body", countIDsResponse)

	return countIDsResponse.Count, nil
}
