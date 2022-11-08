package internal

import (
	"crypto/tls"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/go-errors/errors"
	"net/http"
	"time"
)

type ESClient struct {
	client *elasticsearch.Client
}

type ESParams struct {
	Url      string
	Username string
	Password string
	Index    string
}

func NewESClient(params ESParams) (*ESClient, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{params.Url},
		Username:  params.Username,
		Password:  params.Password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: false,
			},
		},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	esClient := ESClient{
		client: client,
	}

	return &esClient, nil
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

func (e *ESClient) GetDocumentsByIDs(ids []string) (documents []interface{}, err error) {
	return
}
