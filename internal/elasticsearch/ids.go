package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-operator/controllers/utils"
	"io/ioutil"
	"math"
	"time"
)

func (e *ESClient) GetIDsByModifiedOn(start time.Time, end time.Time) (ids []string, err error) {
	var query QueryIDsRange
	query.Query.Range.ModifiedOn.Lt = end.Format(utils.TimeFormat())
	query.Query.Range.ModifiedOn.Gt = start.Format(utils.TimeFormat())
	reqJSON, err := json.Marshal(query)
	if err != nil {
		return ids, errors.Wrap(err, 0)
	}

	return e.getIDsQuery(e.index, reqJSON)
}

func (e *ESClient) GetIDsByIDList(ids []string) (responseIds []string, err error) {
	chunkSize := float64(10000)
	length := float64(len(ids))
	numChunks := int(math.Ceil(length / chunkSize))

	for i := 0; i < numChunks; i++ {
		var query QueryIDsList

		start := i * int(chunkSize)
		end := ((i + 1) * int(chunkSize)) - 1
		if len(ids) < end {
			end = len(ids)
		}

		query.Query.Bool.Filter.IDs.Values = ids[start:end]
		reqJSON, err := json.Marshal(query)
		if err != nil {
			return responseIds, errors.Wrap(err, 0)
		}

		idsChunk, err := e.getIDsQuery(e.index, reqJSON)
		if err != nil {
			return responseIds, errors.Wrap(err, 0)
		}
		responseIds = append(responseIds, idsChunk...)
	}

	return
}

func (e *ESClient) getIDsQuery(index string, reqJSON []byte) (responseIds []string, err error) {
	size := new(int)
	*size = 5000

	searchReq := esapi.SearchRequest{
		Index:  []string{index},
		Scroll: time.Duration(1) * time.Minute,
		Body:   bytes.NewReader(reqJSON),
		Source: []string{"host.id"},
		Size:   size,
		Sort:   []string{"_doc"},
	}

	ctx, cancel := utils.DefaultContext()
	defer cancel()
	searchRes, err := searchReq.Do(ctx, e.client)
	if err != nil {
		return responseIds, errors.Wrap(err, 0)
	}

	if searchRes.StatusCode >= 400 {
		bodyBytes, _ := ioutil.ReadAll(searchRes.Body)

		return responseIds, errors.Wrap(errors.New(fmt.Sprintf(
			"invalid response code when getting hosts ids. StatusCode: %v, Body: %s",
			searchRes.StatusCode, bodyBytes)), 0)
	}

	ids, searchJSON, err := parseSearchIdsResponse(searchRes)
	if err != nil {
		return responseIds, errors.Wrap(err, 0)
	}

	if searchJSON.Hits.Total.Value == 0 {
		return ids, nil
	}

	moreHits := true
	scrollID := searchJSON.ScrollID

	for moreHits == true {
		scrollReq := esapi.ScrollRequest{
			Scroll:   time.Duration(1) * time.Minute,
			ScrollID: scrollID,
		}

		ctx, cancel := utils.DefaultContext()
		defer cancel()
		scrollRes, err := scrollReq.Do(ctx, e.client)
		if err != nil {
			return responseIds, errors.Wrap(err, 0)
		}

		moreIds, scrollJSON, err := parseSearchIdsResponse(scrollRes)
		if err != nil {
			return responseIds, errors.Wrap(err, 0)
		}
		ids = append(ids, moreIds...)
		scrollID = scrollJSON.ScrollID

		if len(scrollJSON.Hits.Hits) == 0 {
			moreHits = false
		}
	}

	return ids, nil
}

func parseSearchIdsResponse(scrollRes *esapi.Response) ([]string, SearchIDsResponse, error) {
	var ids []string
	var searchJSON SearchIDsResponse
	byteValue, _ := ioutil.ReadAll(scrollRes.Body)
	err := json.Unmarshal(byteValue, &searchJSON)
	if err != nil {
		return nil, searchJSON, errors.Wrap(err, 0)
	}

	for _, hit := range searchJSON.Hits.Hits {
		ids = append(ids, hit.ID)
	}

	return ids, searchJSON, nil
}

type SearchIDsResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			ID string `json:"_id"`
		} `json:"hits"`
	} `json:"hits"`
	ScrollID string `json:"_scroll_id"`
}

type QueryIDsRange struct {
	Query struct {
		Range struct {
			ModifiedOn struct {
				Lt string `json:"lt"`
				Gt string `json:"gt"`
			} `json:"modified_on"`
		} `json:"range"`
	} `json:"query"`
}

type QueryIDsList struct {
	Query struct {
		Bool struct {
			Filter struct {
				IDs struct {
					Values []string `json:"values"`
				} `json:"ids"`
			} `json:"filter"`
		} `json:"bool"`
	} `json:"query"`
}
