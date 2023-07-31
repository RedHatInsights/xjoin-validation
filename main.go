package main

import (
	"encoding/json"
	"fmt"
	"github.com/JeremyLoy/config"
	"github.com/RedHatInsights/xjoin-validation/internal/avro"
	. "github.com/RedHatInsights/xjoin-validation/internal/database"
	. "github.com/RedHatInsights/xjoin-validation/internal/elasticsearch"
	logger "github.com/RedHatInsights/xjoin-validation/internal/log"
	. "github.com/RedHatInsights/xjoin-validation/internal/validator"
	"github.com/go-errors/errors"
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
	ElasticsearchHostUrl       string `config:"ELASTICSEARCH_HOST_URL"`
	ElasticsearchIndex         string `config:"ELASTICSEARCH_INDEX"`
	ElasticsearchPassword      string `config:"ELASTICSEARCH_PASSWORD"`
	ElasticsearchUsername      string `config:"ELASTICSEARCH_USERNAME"`
	DatabaseConnections        string `config:"DATABASE_CONNECTIONS"`
	FullAvroSchema             string `config:"FULL_AVRO_SCHEMA"`
	NumAttempts                int    `config:"NUM_ATTEMPTS"`
	Interval                   int    `config:"INTERVAL"`
	LagCompSec                 int    `config:"LAG_COMP_SEC"`
	PeriodMin                  int    `config:"PERIOD_MIN"`
	InvalidThresholdPercentage int    `config:"INVALID_THRESHOLD_PERCENTAGE"`
}

func parseDatabaseConnectionFromEnv(datasourceName string) (dbConnectionInfo DatabaseConnectionInfo, err error) {
	dbConnectionInfo.Hostname = os.Getenv(datasourceName + "_DB_HOSTNAME")
	dbConnectionInfo.Username = os.Getenv(datasourceName + "_DB_USERNAME")
	dbConnectionInfo.Password = os.Getenv(datasourceName + "_DB_PASSWORD")
	dbConnectionInfo.Name = os.Getenv(datasourceName + "_DB_NAME")
	dbConnectionInfo.Port = os.Getenv(datasourceName + "_DB_PORT")
	dbConnectionInfo.Table = os.Getenv(datasourceName + "_DB_TABLE")
	dbConnectionInfo.SSLMode = os.Getenv(datasourceName + "_DB_SSL_MODE")

	if dbConnectionInfo.Hostname == "" {
		return dbConnectionInfo, errors.Wrap(errors.New(
			"missing database hostname environment variable for datasource: "+datasourceName), 0)
	} else if dbConnectionInfo.Username == "" {
		return dbConnectionInfo, errors.Wrap(errors.New(
			"missing database username environment variable for datasource: "+datasourceName), 0)
	} else if dbConnectionInfo.Password == "" {
		return dbConnectionInfo, errors.Wrap(errors.New(
			"missing database password environment variable for datasource: "+datasourceName), 0)
	} else if dbConnectionInfo.Name == "" {
		return dbConnectionInfo, errors.Wrap(errors.New(
			"missing database name environment variable for datasource: "+datasourceName), 0)
	} else if dbConnectionInfo.Port == "" {
		return dbConnectionInfo, errors.Wrap(errors.New(
			"missing database port environment variable for datasource: "+datasourceName), 0)
	} else if dbConnectionInfo.Table == "" {
		return dbConnectionInfo, errors.Wrap(errors.New(
			"missing database table environment variable for datasource: "+datasourceName), 0)
	} else if dbConnectionInfo.SSLMode == "" {
		dbConnectionInfo.SSLMode = "disable"
	}

	return
}

//currently assumes a single reference
func main() {
	log, err := logger.NewLogger()
	if err != nil {
		fmt.Println("Unable to initialize logger")
		os.Exit(1)
	}

	log.Info("Starting validation...")

	//load config
	var c Config
	if strings.ToLower(os.Getenv("ENV")) == "development" {
		err = config.From("config/dev.config").FromEnv().To(&c)
		if err != nil {
			log.Error(errors.Wrap(err, 0), "error parsing config")
			os.Exit(1)
		}
	} else {
		err = config.From("config/prod.config").FromEnv().To(&c)
		if err != nil {
			log.Error(errors.Wrap(err, 0), "error parsing config")
			os.Exit(1)
		}
	}

	//parse avro schema
	schemaParser := avro.SchemaParser{
		FullSchemaString: c.FullAvroSchema,
	}
	parsedSchema, err := schemaParser.Parse()
	if err != nil {
		log.Error(errors.Wrap(err, 0), "error parsing avro schemas")
		os.Exit(1)
	}

	//connect to database
	if len(strings.Split(parsedSchema.FullAvroSchema.Namespace, ".")) < 2 {
		log.Error(errors.Wrap(errors.New(
			"Invalid FullAvroSchema.Namespace value. Expected '.' delimited string, e.g. xjoinindexpipeline.hosts.1"), 0),
			"Invalid FullAvroSchema")
		os.Exit(1)
	}

	datasourceName := strings.Split(parsedSchema.FullAvroSchema.Namespace, ".")[1]
	dbConnectionInfo, err := parseDatabaseConnectionFromEnv(datasourceName)
	if err != nil {
		log.Error(errors.Wrap(err, 0), "error parsing database connection from environment variables")
		os.Exit(1)
	}

	dbClient, err := NewDBClient(DBParams{
		User:             dbConnectionInfo.Username,
		Password:         dbConnectionInfo.Password,
		Host:             dbConnectionInfo.Hostname,
		Name:             dbConnectionInfo.Name,
		Port:             dbConnectionInfo.Port,
		Table:            dbConnectionInfo.Table,
		SSLMode:          dbConnectionInfo.SSLMode,
		ParsedAvroSchema: parsedSchema,
		Log:              log,
	})
	if err != nil {
		log.Error(errors.Wrap(err, 0), "error connecting to database")
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
		Log:              log,
	})
	if err != nil {
		log.Error(errors.Wrap(err, 0), "error connecting to elasticsearch")
		os.Exit(1)
	}

	//run validation
	//TODO: auto retry if sync is progressing (i.e. new mismatch count < previous mismatch count)
	i := 0
	for i < c.NumAttempts {
		log.Info("Validation attempt", "number", i)
		validator := Validator{
			DBClient:                   *dbClient,
			ESClient:                   *esClient,
			PeriodMin:                  c.PeriodMin,
			LagCompSec:                 c.LagCompSec,
			Now:                        time.Now().UTC(),
			Log:                        log,
			InvalidThresholdPercentage: c.InvalidThresholdPercentage,
		}
		response, err := validator.Validate()
		if err != nil {
			log.Error(errors.Wrap(err, 0), "error during validation")
			os.Exit(1)
		}

		jsonResponse, err := json.Marshal(response)
		if err != nil {
			log.Error(errors.Wrap(err, 0), "unable to marshal response to JSON")
			os.Exit(1)
		}
		fmt.Println(string(jsonResponse))

		if response.Result == "valid" {
			break
		} else {
			time.Sleep(time.Duration(c.Interval) * time.Second)
			i += 1
		}
	}
	os.Exit(0)
}
