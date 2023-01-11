package main

import (
	"encoding/json"
	"fmt"
	"github.com/JeremyLoy/config"
	"github.com/RedHatInsights/xjoin-validation/internal/avro"
	. "github.com/RedHatInsights/xjoin-validation/internal/database"
	. "github.com/RedHatInsights/xjoin-validation/internal/elasticsearch"
	. "github.com/RedHatInsights/xjoin-validation/internal/validator"
	"os"
	"strings"
	"time"
)

type DatabaseConnectionInfo struct {
	Name     string `json:"name"`
	Hostname string `json:"hostname"`
	Username string `json:"username"`
	Password string `json:"password"`
	Port     string `json:"port"`
	Table    string `json:"table"`
	SSLMode  string `json:"sslMode"`
}

type Config struct {
	ElasticsearchHostUrl  string `config:"ELASTICSEARCH_HOST_URL"`
	ElasticsearchIndex    string `config:"ELASTICSEARCH_INDEX"`
	ElasticsearchPassword string `config:"ELASTICSEARCH_PASSWORD"`
	ElasticsearchUsername string `config:"ELASTICSEARCH_USERNAME"`
	DatabaseConnections   string `config:"DATABASE_CONNECTIONS"`
	FullAvroSchema        string `config:"FULL_AVRO_SCHEMA"`
}

//currently assumes a single reference
func main() {
	fmt.Println("Starting validation...")

	//load config
	var c Config
	err := config.From("config/dev.config").FromEnv().To(&c)
	if err != nil {
		fmt.Println("error parsing config")
		fmt.Println(err)
		os.Exit(1)
	}

	var dbConnectionInfo map[string]DatabaseConnectionInfo
	err = json.Unmarshal([]byte(c.DatabaseConnections), &dbConnectionInfo)
	if err != nil {
		fmt.Println("error parsing DatabaseConnections json")
		fmt.Println(err)
		os.Exit(1)
	}

	//parse avro schema
	schemaParser := avro.SchemaParser{
		FullSchemaString: c.FullAvroSchema,
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
		User:             dbConnectionInfo[datasourceName].Username,
		Password:         dbConnectionInfo[datasourceName].Password,
		Host:             dbConnectionInfo[datasourceName].Hostname,
		Name:             dbConnectionInfo[datasourceName].Name,
		Port:             dbConnectionInfo[datasourceName].Port,
		Table:            dbConnectionInfo[datasourceName].Table,
		SSLMode:          dbConnectionInfo[datasourceName].SSLMode,
		ParsedAvroSchema: parsedSchema,
	})
	if err != nil {
		fmt.Println("error connecting to database")
		fmt.Println(err)
		os.Exit(1)
	}

	//connect to Elasticsearch
	esClient, err := NewESClient(ESParams{
		Url:              c.ElasticsearchHostUrl,
		Username:         c.ElasticsearchUsername,
		Password:         c.ElasticsearchPassword,
		Index:            c.ElasticsearchIndex,
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
		Now:               time.Now().UTC(),
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
