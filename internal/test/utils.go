package test

import (
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/RedHatInsights/xjoin-validation/internal/avro"
	"github.com/RedHatInsights/xjoin-validation/internal/database"
	"github.com/RedHatInsights/xjoin-validation/internal/elasticsearch"
	. "github.com/RedHatInsights/xjoin-validation/internal/validator"
	"github.com/jarcoal/httpmock"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"path"
	"path/filepath"
	"runtime"
	"time"
)

type TestEnv struct {
	Validator Validator
	DBMock    sqlmock.Sqlmock
	MockDB    *sql.DB
}

func GetRootDir() string {
	_, b, _, _ := runtime.Caller(0)
	d := path.Join(path.Dir(b))
	return filepath.Dir(d)
}

func LoadTestDataFile(fileName string) string {
	dataString, err := ioutil.ReadFile(GetRootDir() + "/test/" + fileName + ".json")
	Expect(err).ToNot(HaveOccurred())
	return string(dataString)
}

func BeforeEach() TestEnv {
	httpmock.Activate()
	httpmock.RegisterNoResponder(httpmock.InitialTransport.RoundTrip) //disable mocks for unregistered http requests

	//parse avro schema
	schemaParser := avro.SchemaParser{
		FullSchemaString:  LoadTestDataFile("avro/full"),
		IndexSchemaString: LoadTestDataFile("avro/index"),
	}
	parsedSchema, err := schemaParser.Parse()
	Expect(err).ToNot(HaveOccurred())

	//connect to database
	mockDB, dbMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	sqlxMockDB := sqlx.NewDb(mockDB, "sqlmock")
	dbClient := database.NewTestDBClient(sqlxMockDB, database.DBParams{
		Table:            "hosts",
		ParsedAvroSchema: parsedSchema,
	})
	Expect(err).ToNot(HaveOccurred())

	//connect to Elasticsearch
	esClient, err := elasticsearch.NewESClient(elasticsearch.ESParams{
		Url:              "http://mock-es:9200",
		Username:         "mock",
		Password:         "mockpassword",
		Index:            "mockindex",
		RootNode:         parsedSchema.RootNode,
		ParsedAvroSchema: parsedSchema,
	})
	Expect(err).ToNot(HaveOccurred())

	validator := Validator{
		DBClient:          *dbClient,
		ESClient:          *esClient,
		ValidationPeriod:  100,
		ValidationLagComp: 0,
		State:             "new",
		Now:               time.Now(),
	}

	return TestEnv{
		Validator: validator,
		MockDB:    mockDB,
		DBMock:    dbMock,
	}
}
