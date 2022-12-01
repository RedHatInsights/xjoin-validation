package validator_test

import (
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/RedHatInsights/xjoin-validation/internal/avro"
	"github.com/RedHatInsights/xjoin-validation/internal/database"
	"github.com/RedHatInsights/xjoin-validation/internal/elasticsearch"
	. "github.com/RedHatInsights/xjoin-validation/internal/validator"
	"github.com/jarcoal/httpmock"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"path"
	"path/filepath"
	"runtime"
)

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

var _ = Describe("Count", func() {
	var validator Validator
	var dbMock sqlmock.Sqlmock
	var mockDB *sql.DB

	BeforeEach(func() {
		httpmock.Activate()
		//httpmock.RegisterNoResponder(httpmock.InitialTransport.RoundTrip) //disable mocks for unregistered http requests

		//parse avro schema
		schemaParser := avro.SchemaParser{
			FullSchemaString:  LoadTestDataFile("avro/full"),
			IndexSchemaString: LoadTestDataFile("avro/index"),
		}
		parsedSchema, err := schemaParser.Parse()
		Expect(err).ToNot(HaveOccurred())

		//connect to database
		mockDB, dbMock, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
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

		validator = Validator{
			DBClient:          *dbClient,
			ESClient:          *esClient,
			ValidationPeriod:  100,
			ValidationLagComp: 0,
			State:             "new",
		}
	})

	AfterEach(func() {
		httpmock.DeactivateAndReset()
	})

	Describe("validation", func() {
		Context("should be valid", func() {
			It("when database and elasticsearch count is the same", func() {
				dbMock.ExpectQuery("SELECT count(*) from hosts").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("1"))

				httpmock.RegisterResponder(
					"POST",
					"http://mock-es:9200/mockindex/_count",
					httpmock.NewStringResponder(200, `{"count": 1}`))

				result, err := validator.ValidateCount()
				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(Equal(ValidateCountResult{
					CountIsValid:  true,
					ESCount:       1,
					DBCount:       1,
					MismatchCount: 0,
					MismatchRatio: 0,
				}))

				info := httpmock.GetCallCountInfo()
				count := info["POST http://mock-es:9200/mockindex/_count"]
				Expect(count).To(Equal(1))
			})
		})

		Context("should be invalid", func() {
			It("when database has more records than elasticsearch", func() {
				dbMock.ExpectQuery("SELECT count(*) from hosts").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("10"))

				httpmock.RegisterResponder(
					"POST",
					"http://mock-es:9200/mockindex/_count",
					httpmock.NewStringResponder(200, `{"count": 0}`))

				result, err := validator.ValidateCount()
				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(Equal(ValidateCountResult{
					CountIsValid:  false,
					ESCount:       0,
					DBCount:       10,
					MismatchCount: 10,
					MismatchRatio: 1,
				}))

				info := httpmock.GetCallCountInfo()
				count := info["POST http://mock-es:9200/mockindex/_count"]
				Expect(count).To(Equal(1))

			})

			It("when elasticsearch has more records than the database", func() {
				dbMock.ExpectQuery("SELECT count(*) from hosts").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("0"))

				httpmock.RegisterResponder(
					"POST",
					"http://mock-es:9200/mockindex/_count",
					httpmock.NewStringResponder(200, `{"count": 10}`))

				result, err := validator.ValidateCount()
				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(Equal(ValidateCountResult{
					CountIsValid:  false,
					ESCount:       10,
					DBCount:       0,
					MismatchCount: 10,
					MismatchRatio: 1,
				}))

				info := httpmock.GetCallCountInfo()
				count := info["POST http://mock-es:9200/mockindex/_count"]
				Expect(count).To(Equal(1))

			})

			It("and correctly calculate complex mismatch ratio when ES has more than DB", func() {
				dbMock.ExpectQuery("SELECT count(*) from hosts").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("3"))

				httpmock.RegisterResponder(
					"POST",
					"http://mock-es:9200/mockindex/_count",
					httpmock.NewStringResponder(200, `{"count": 9}`))

				result, err := validator.ValidateCount()
				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(Equal(ValidateCountResult{
					CountIsValid:  false,
					ESCount:       9,
					DBCount:       3,
					MismatchCount: 6,
					MismatchRatio: 0.67,
				}))

				info := httpmock.GetCallCountInfo()
				count := info["POST http://mock-es:9200/mockindex/_count"]
				Expect(count).To(Equal(1))

			})

			It("and correctly calculate complex mismatch ratio when DB has more than ES", func() {
				dbMock.ExpectQuery("SELECT count(*) from hosts").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("12"))

				httpmock.RegisterResponder(
					"POST",
					"http://mock-es:9200/mockindex/_count",
					httpmock.NewStringResponder(200, `{"count": 2}`))

				result, err := validator.ValidateCount()
				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(Equal(ValidateCountResult{
					CountIsValid:  false,
					ESCount:       2,
					DBCount:       12,
					MismatchCount: 10,
					MismatchRatio: 0.83,
				}))

				info := httpmock.GetCallCountInfo()
				count := info["POST http://mock-es:9200/mockindex/_count"]
				Expect(count).To(Equal(1))

			})
		})
	})
})
