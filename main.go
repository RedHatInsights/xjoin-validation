package main

import (
	"encoding/json"
	"fmt"
	. "github.com/RedHatInsights/xjoin-validation/pkg"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/redhatinsights/xjoin-operator/controllers/avro"
	"github.com/redhatinsights/xjoin-operator/controllers/database"
	xjoinElasticsearch "github.com/redhatinsights/xjoin-operator/controllers/elasticsearch"
	"github.com/redhatinsights/xjoin-operator/controllers/utils"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

//currently assumes a single reference
func main() {
	fmt.Println("Starting validation...")

	//parse avro schema
	//for db.table in avroSchema.fields
	var fullAvroSchema avro.Schema
	err := json.Unmarshal([]byte(os.Getenv("FULL_AVRO_SCHEMA")), &fullAvroSchema)

	if err != nil {
		fmt.Println("error parsing schema")
		fmt.Println(err)
	}

	var indexAvroSchema avro.Schema
	err = json.Unmarshal([]byte(os.Getenv("INDEX_AVRO_SCHEMA")), &indexAvroSchema)
	if err != nil {
		fmt.Println("error parsing index schema")
		fmt.Println(err)
	}

	//TODO: connect to DB
	datasourceName := strings.Split(fullAvroSchema.Namespace, ".")[0]

	db := database.NewDatabase(database.DBParams{
		User:     os.Getenv(datasourceName + "_DB_USERNAME"),
		Password: os.Getenv(datasourceName + "_DB_PASSWORD"),
		Host:     os.Getenv(datasourceName + "_DB_HOSTNAME"),
		Name:     os.Getenv(datasourceName + "_DB_NAME"),
		Port:     os.Getenv(datasourceName + "_DB_PORT"),
		SSLMode:  "disable",
	})

	err = db.Connect()
	if err != nil {
		fmt.Println("error connecting to db")
	}

	rows, err := db.CountHosts()
	if err != nil {
		fmt.Println("error running query")
	}

	fmt.Println(rows)

	//TODO: connect to ES
	cfg := elasticsearch.Config{
		//Addresses: []string{os.Getenv("ELASTICSEARCH_URL")},
		Username: os.Getenv("ELASTICSEARCH_USERNAME"),
		Password: os.Getenv("ELASTICSEARCH_PASSWORD"),
		//Transport: &http.Transport{
		//	TLSClientConfig: &tls.Config{
		//		InsecureSkipVerify: false,
		//	},
		//},
	}

	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		fmt.Println("error creating ES client")
		fmt.Println(err)
	}

	req := esapi.CountRequest{
		Index: []string{os.Getenv("ELASTICSEARCH_INDEX")},
	}

	ctx, cancel := utils.DefaultContext()
	defer cancel()
	res, err := req.Do(ctx, esClient)
	if err != nil {
		fmt.Println("error running es count query")
		fmt.Println(err)
	}

	var countIDsResponse xjoinElasticsearch.CountIDsResponse
	byteValue, _ := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(byteValue, &countIDsResponse)
	if err != nil {
		fmt.Println("error parsing countids response")
	}

	//TODO: compare count
	fmt.Println("es count: " + strconv.Itoa(countIDsResponse.Count))

	//TODO: compare IDs

	//TODO: compare full content

	//TODO: retry n times, auto retry if sync is progressing (e.g. previous mismatch count < new mismatch count)

	//TODO: build response

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
