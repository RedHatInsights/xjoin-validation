package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	. "github.com/RedHatInsights/xjoin-validation/internal/record"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
	"io/ioutil"
)

func (e *ESClient) GetDocumentsByIDs(ids []string) (records []map[string]interface{}, err error) {
	ctx, cancel := utils.DefaultContext()
	defer cancel()

	var query QueryIDsList
	query.Query.Bool.Filter.IDs.Values = ids
	reqJSON, err := json.Marshal(query)
	requestSize := len(ids)

	searchReq := esapi.SearchRequest{
		Index: []string{e.index},
		Size:  &requestSize,
		Sort:  []string{"_id"},
		Body:  bytes.NewReader(reqJSON),
	}

	searchRes, err := searchReq.Do(ctx, e.client)
	if err != nil {
		return records, errors.Wrap(err, 0)
	}
	if searchRes.StatusCode >= 400 {
		bodyBytes, _ := ioutil.ReadAll(searchRes.Body)

		return nil, errors.Wrap(errors.New(fmt.Sprintf(
			"invalid response code when getting elasticsearch records by id. StatusCode: %v, Body: %s",
			searchRes.StatusCode, bodyBytes)), 0)
	}

	records, err = e.parseSearchResponse(searchRes)
	if err != nil {
		return records, errors.Wrap(err, 0)
	}

	return
}

func (e *ESClient) parseSearchResponse(res *esapi.Response) (records []map[string]interface{}, err error) {
	var searchResponse SearchResponse
	byteValue, _ := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(byteValue, &searchResponse)
	if err != nil {
		return records, errors.Wrap(err, 0)
	}

	for _, hit := range searchResponse.Hits.Hits {
		recordParser := RecordParser{
			Record:           hit.Source,
			ParsedAvroSchema: e.parsedAvroSchema,
		}
		record, err := recordParser.Parse()
		if err != nil {
			return records, errors.Wrap(err, 0)
		}

		records = append(records, record)
	}

	return
}

type SearchResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}
