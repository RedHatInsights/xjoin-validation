package validator_test

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/RedHatInsights/xjoin-validation/internal/test"
	. "github.com/RedHatInsights/xjoin-validation/internal/validator"
	"github.com/jarcoal/httpmock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Count", func() {
	var validator Validator
	var dbMock sqlmock.Sqlmock

	BeforeEach(func() {
		testEnv := test.BeforeEach()
		validator = testEnv.Validator
		dbMock = testEnv.DBMock
	})

	AfterEach(func() {
		httpmock.DeactivateAndReset()
	})

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
