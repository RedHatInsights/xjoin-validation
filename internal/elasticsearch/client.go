package elasticsearch

import (
	"crypto/tls"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/go-errors/errors"
	"net/http"
)

type ESClient struct {
	client *elasticsearch.Client
	index  string
}

type ESParams struct {
	Url      string
	Username string
	Password string
	Index    string
}

func NewESClient(params ESParams) (*ESClient, error) {
	cfg := elasticsearch.Config{
		//Addresses: []string{params.Url}, //TODO change ELASTICSEARCH_URL env var name
		Username: params.Username,
		Password: params.Password,
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
		index:  params.Index,
	}

	return &esClient, nil
}
