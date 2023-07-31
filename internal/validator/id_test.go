package validator_test

import (
	"fmt"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/RedHatInsights/xjoin-validation/internal/test"
	. "github.com/RedHatInsights/xjoin-validation/internal/validator"
	"github.com/jarcoal/httpmock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ID validation", func() {
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
		It("when database and elasticsearch IDs are the same", func() {
			startTime := validator.Now.Add(-time.Duration(validator.PeriodMin) * time.Minute)
			endTime := validator.Now.Add(-time.Duration(validator.LagCompSec) * time.Second)

			dbMock.ExpectQuery(fmt.Sprintf(
				`SELECT id FROM hosts WHERE modified_on > '%s' AND modified_on < '%s' ORDER BY id`,
				startTime.Format(time.RFC3339Nano), endTime.Format(time.RFC3339Nano))).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("1234"))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/id/one.hit.response")))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/_search/scroll?scroll=60000ms&scroll_id=test-scroll-id-1",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/id/empty.scroll.response")))

			result, err := validator.ValidateIDs()
			Expect(err).ToNot(HaveOccurred())

			Expect(result).To(Equal(ValidateIDsResult{
				InDBOnly:                []string{},
				InESOnly:                []string{},
				TotalDBRecordsRetrieved: 1,
				TotalESRecordsRetrieved: 1,
				MismatchCount:           0,
				MismatchRatio:           0,
				IDsAreValid:             true,
			}))

			info := httpmock.GetCallCountInfo()
			count := info["GET http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc"]
			Expect(count).To(Equal(1))
			count = info["GET http://mock-es:9200/_search/scroll?scroll=60000ms&scroll_id=test-scroll-id-1"]
			Expect(count).To(Equal(1))
		})

		It("when there are no hosts in time range", func() {
			startTime := validator.Now.Add(-time.Duration(validator.PeriodMin) * time.Minute)
			endTime := validator.Now.Add(-time.Duration(validator.LagCompSec) * time.Second)

			dbMock.ExpectQuery(fmt.Sprintf(
				`SELECT id FROM hosts WHERE modified_on > '%s' AND modified_on < '%s' ORDER BY id`,
				startTime.Format(time.RFC3339Nano), endTime.Format(time.RFC3339Nano))).
				WillReturnRows(sqlmock.NewRows([]string{"id"}))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/id/zero.hit.response")))

			result, err := validator.ValidateIDs()
			Expect(err).ToNot(HaveOccurred())

			Expect(result).To(Equal(ValidateIDsResult{
				InDBOnly:                []string{},
				InESOnly:                []string{},
				TotalDBRecordsRetrieved: 0,
				TotalESRecordsRetrieved: 0,
				MismatchCount:           0,
				MismatchRatio:           0,
				IDsAreValid:             true,
			}))

			info := httpmock.GetCallCountInfo()
			count := info["GET http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc"]
			Expect(count).To(Equal(1))
		})
	})

	Context("should be invalid", func() {
		It("when the DB has more IDs than ES", func() {
			startTime := validator.Now.Add(-time.Duration(validator.PeriodMin) * time.Minute)
			endTime := validator.Now.Add(-time.Duration(validator.LagCompSec) * time.Second)

			dbMock.ExpectQuery(fmt.Sprintf(
				`SELECT id FROM hosts WHERE modified_on > '%s' AND modified_on < '%s' ORDER BY id`,
				startTime.Format(time.RFC3339Nano), endTime.Format(time.RFC3339Nano))).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("1234"))

			dbMock.ExpectQuery(`SELECT id FROM hosts WHERE id in ('1234')`).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("1234"))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/id/zero.hit.response")))

			result, err := validator.ValidateIDs()
			Expect(err).ToNot(HaveOccurred())

			Expect(result).To(Equal(ValidateIDsResult{
				InDBOnly:                []string{"1234"},
				InESOnly:                []string{},
				TotalDBRecordsRetrieved: 1,
				TotalESRecordsRetrieved: 0,
				MismatchCount:           1,
				MismatchRatio:           1,
				IDsAreValid:             false,
			}))

			info := httpmock.GetCallCountInfo()
			count := info["GET http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc"]
			Expect(count).To(Equal(2))
		})

		It("when ES has more IDs than the DB", func() {
			startTime := validator.Now.Add(-time.Duration(validator.PeriodMin) * time.Minute)
			endTime := validator.Now.Add(-time.Duration(validator.LagCompSec) * time.Second)

			dbMock.ExpectQuery(fmt.Sprintf(
				`SELECT id FROM hosts WHERE modified_on > '%s' AND modified_on < '%s' ORDER BY id`,
				startTime.Format(time.RFC3339Nano), endTime.Format(time.RFC3339Nano))).
				WillReturnRows(sqlmock.NewRows([]string{"id"}))

			dbMock.ExpectQuery(`SELECT id FROM hosts WHERE id in ('1234')`).
				WillReturnRows(sqlmock.NewRows([]string{"id"}))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/id/one.hit.response")))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/_search/scroll?scroll=60000ms&scroll_id=test-scroll-id-1",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/id/empty.scroll.response")))

			result, err := validator.ValidateIDs()
			Expect(err).ToNot(HaveOccurred())

			Expect(result).To(Equal(ValidateIDsResult{
				InDBOnly:                []string{},
				InESOnly:                []string{"1234"},
				TotalDBRecordsRetrieved: 0,
				TotalESRecordsRetrieved: 1,
				MismatchCount:           1,
				MismatchRatio:           1,
				IDsAreValid:             false,
			}))

			info := httpmock.GetCallCountInfo()
			count := info["GET http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc"]
			Expect(count).To(Equal(2))
			count = info["GET http://mock-es:9200/_search/scroll?scroll=60000ms&scroll_id=test-scroll-id-1"]
			Expect(count).To(Equal(2))
		})

		It("when ES and DB have mismatched IDs", func() {
			startTime := validator.Now.Add(-time.Duration(validator.PeriodMin) * time.Minute)
			endTime := validator.Now.Add(-time.Duration(validator.LagCompSec) * time.Second)

			dbMock.ExpectQuery(fmt.Sprintf(
				`SELECT id FROM hosts WHERE modified_on > '%s' AND modified_on < '%s' ORDER BY id`,
				startTime.Format(time.RFC3339Nano), endTime.Format(time.RFC3339Nano))).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("5678"))

			dbMock.ExpectQuery(`SELECT id FROM hosts WHERE id in ('5678','1234')`).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("5678"))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/id/one.hit.response")))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/_search/scroll?scroll=60000ms&scroll_id=test-scroll-id-1",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/id/empty.scroll.response")))

			validator.SetDBCount(1)
			result, err := validator.ValidateIDs()
			Expect(err).ToNot(HaveOccurred())

			Expect(result).To(Equal(ValidateIDsResult{
				InDBOnly:                []string{"5678"},
				InESOnly:                []string{"1234"},
				TotalDBRecordsRetrieved: 1,
				TotalESRecordsRetrieved: 1,
				MismatchCount:           2,
				MismatchRatio:           1,
				IDsAreValid:             false,
			}))

			info := httpmock.GetCallCountInfo()
			count := info["GET http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc"]
			Expect(count).To(Equal(2))
			count = info["GET http://mock-es:9200/_search/scroll?scroll=60000ms&scroll_id=test-scroll-id-1"]
			Expect(count).To(Equal(2))
		})

		It("when ES and DB have complex mismatched IDs", func() {
			startTime := validator.Now.Add(-time.Duration(validator.PeriodMin) * time.Minute)
			endTime := validator.Now.Add(-time.Duration(validator.LagCompSec) * time.Second)

			dbMock.ExpectQuery(fmt.Sprintf(
				`SELECT id FROM hosts WHERE modified_on > '%s' AND modified_on < '%s' ORDER BY id`,
				startTime.Format(time.RFC3339Nano), endTime.Format(time.RFC3339Nano))).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("in.both").AddRow("db.only.1").AddRow("db.only.2"))

			dbMock.ExpectQuery(`SELECT id FROM hosts WHERE id in ('db.only.1','db.only.2','es.only.1','es.only.2')`).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("in.both").AddRow("db.only.1").AddRow("db.only.2"))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/id/multi.hit.response")))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/_search/scroll?scroll=60000ms&scroll_id=test-scroll-id-1",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/id/empty.scroll.response")))

			validator.SetDBCount(3)
			result, err := validator.ValidateIDs()
			Expect(err).ToNot(HaveOccurred())

			Expect(result).To(Equal(ValidateIDsResult{
				InDBOnly:                []string{"db.only.1", "db.only.2"},
				InESOnly:                []string{"es.only.1", "es.only.2"},
				TotalDBRecordsRetrieved: 3,
				TotalESRecordsRetrieved: 3,
				MismatchCount:           4,
				MismatchRatio:           0.8,
				IDsAreValid:             false,
			}))

			info := httpmock.GetCallCountInfo()
			count := info["GET http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc"]
			Expect(count).To(Equal(2))
			count = info["GET http://mock-es:9200/_search/scroll?scroll=60000ms&scroll_id=test-scroll-id-1"]
			Expect(count).To(Equal(2))
		})
	})
})
