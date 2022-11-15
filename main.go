package main

import (
	"encoding/json"
	"fmt"
	"github.com/RedHatInsights/xjoin-validation/internal/avro"
	. "github.com/RedHatInsights/xjoin-validation/internal/database"
	. "github.com/RedHatInsights/xjoin-validation/internal/elasticsearch"
	. "github.com/RedHatInsights/xjoin-validation/internal/validator"
	"os"
	"strings"
)

//currently assumes a single reference
func main() {
	fmt.Println("Starting validation...")

	//parse avro schema
	schemaParser := avro.SchemaParser{
		FullSchemaString:  os.Getenv("FULL_AVRO_SCHEMA"),
		IndexSchemaString: os.Getenv("INDEX_AVRO_SCHEMA"),
	}
	parsedSchema, err := schemaParser.Parse()
	if err != nil {
		fmt.Println("error parsing avro schemas")
		fmt.Println(err)
		os.Exit(1)
	}

	//connect to database
	datasourceName := strings.Split(parsedSchema.FullAvroSchema.Namespace, ".")[0]
	dbClient, err := NewDBClient(DBParams{
		User:             os.Getenv(datasourceName + "_DB_USERNAME"),
		Password:         os.Getenv(datasourceName + "_DB_PASSWORD"),
		Host:             os.Getenv(datasourceName + "_DB_HOSTNAME"),
		Name:             os.Getenv(datasourceName + "_DB_NAME"),
		Port:             os.Getenv(datasourceName + "_DB_PORT"),
		Table:            os.Getenv(datasourceName + "_DB_TABLE"),
		SSLMode:          "disable",
		ParsedAvroSchema: parsedSchema,
	})
	if err != nil {
		fmt.Println("error connecting to database")
		fmt.Println(err)
		os.Exit(1)
	}

	//connect to Elasticsearch
	esClient, err := NewESClient(ESParams{
		Url:              os.Getenv("ELASTICSEARCH_URL"),
		Username:         os.Getenv("ELASTICSEARCH_USERNAME"),
		Password:         os.Getenv("ELASTICSEARCH_PASSWORD"),
		Index:            os.Getenv("ELASTICSEARCH_INDEX"),
		RootNode:         parsedSchema.RootNode,
		ParsedAvroSchema: parsedSchema,
	})
	if err != nil {
		fmt.Println("error connecting to elasticsearch")
		fmt.Println(err)
		os.Exit(1)
	}

	//run validation
	validator := Validator{
		DBClient:          *dbClient,
		ESClient:          *esClient,
		ValidationPeriod:  60,
		ValidationLagComp: 0,
	}
	response, err := validator.Validate()
	if err != nil {
		fmt.Println("error during validation")
		fmt.Println(err)
		os.Exit(1)
	}

	//TODO: retry n times, auto retry if sync is progressing (i.e. new mismatch count < previous mismatch count)

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		fmt.Println("Unable to marshal response to JSON")
		os.Exit(-1)
	}

	fmt.Println(string(jsonResponse))
	os.Exit(0)
}
