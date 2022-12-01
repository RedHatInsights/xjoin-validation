package elasticsearch

import (
	"github.com/RedHatInsights/xjoin-validation/internal/avro"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/go-errors/errors"
)

type ESClient struct {
	client           *elasticsearch.Client
	index            string
	rootNode         string
	parsedAvroSchema avro.ParsedAvroSchema
}

type ESParams struct {
	Url              string
	Username         string
	Password         string
	Index            string
	RootNode         string
	ParsedAvroSchema avro.ParsedAvroSchema
}

func NewESClient(params ESParams) (*ESClient, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{params.Url},
		Username:  params.Username,
		Password:  params.Password,
		//Transport: &http.Transport{
		//	TLSClientConfig: &tls.Config{
		//		InsecureSkipVerify: false,
		//	},
		//},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	esClient := ESClient{
		client:           client,
		index:            params.Index,
		rootNode:         params.RootNode,
		parsedAvroSchema: params.ParsedAvroSchema,
	}

	return &esClient, nil
}
