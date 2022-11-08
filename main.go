package main

import (
	"encoding/json"
	"fmt"
	. "github.com/RedHatInsights/xjoin-validation/internal"
	. "github.com/RedHatInsights/xjoin-validation/pkg"
	"github.com/redhatinsights/xjoin-operator/controllers/avro"
	"os"
	"strings"
)

//currently assumes a single reference
func main() {
	fmt.Println("Starting validation...")

	//unmarshal avro schema
	var fullAvroSchema avro.Schema
	err := json.Unmarshal([]byte(os.Getenv("FULL_AVRO_SCHEMA")), &fullAvroSchema)

	if err != nil {
		fmt.Println("error parsing full avro schema")
		fmt.Println(err)
		os.Exit(1)
	}

	var indexAvroSchema avro.Schema
	err = json.Unmarshal([]byte(os.Getenv("INDEX_AVRO_SCHEMA")), &indexAvroSchema)
	if err != nil {
		fmt.Println("error parsing index avro schema")
		fmt.Println(err)
		os.Exit(1)
	}

	//connect to database
	datasourceName := strings.Split(fullAvroSchema.Namespace, ".")[0]
	dbClient, err := NewDBClient(DBParams{
		User:     os.Getenv(datasourceName + "_DB_USERNAME"),
		Password: os.Getenv(datasourceName + "_DB_PASSWORD"),
		Host:     os.Getenv(datasourceName + "_DB_HOSTNAME"),
		Name:     os.Getenv(datasourceName + "_DB_NAME"),
		Port:     os.Getenv(datasourceName + "_DB_PORT"),
		SSLMode:  "disable",
	})
	if err != nil {
		fmt.Println("error connecting to database")
		fmt.Println(err)
		os.Exit(1)
	}

	//connect to Elasticsearch
	esClient, err := NewESClient(ESParams{
		Url:      os.Getenv("ELASTICSEARCH_URL"),
		Username: os.Getenv("ELASTICSEARCH_USERNAME"),
		Password: os.Getenv("ELASTICSEARCH_PASSWORD"),
		Index:    os.Getenv("ELASTICSEARCH_INDEX"),
	})
	if err != nil {
		fmt.Println("error connecting to elasticsearch")
		fmt.Println(err)
		os.Exit(1)
	}

	//run validation
	validator := Validator{
		DBClient: *dbClient,
		ESClient: *esClient,
	}
	validator.Validate()

	//TODO: retry n times, auto retry if sync is progressing (e.g. previous mismatch count < new mismatch count)

	response := Response{
		Result:  "valid",
		Reason:  "count mismatch",
		Message: "20 rows missing from Elasticsearch",
		Details: ResponseDetails{
			TotalMismatch:                    20,
			IdsMissingFromElasticsearch:      []string{"1", "2", "3"},
			IdsMissingFromElasticsearchCount: 20,
			IdsOnlyInElasticsearch:           nil,
			IdsOnlyInElasticsearchCount:      0,
			IdsWithMismatchContent:           nil,
			MismatchContentDetails:           nil,
		},
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		fmt.Println("Unable to marshal response to JSON")
		os.Exit(-1)
	}

	//time.Sleep(30 * time.Second)

	fmt.Println(string(jsonResponse))
	os.Exit(0)
}
