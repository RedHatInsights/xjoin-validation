package validator_test

import (
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/RedHatInsights/xjoin-validation/internal/test"
	. "github.com/RedHatInsights/xjoin-validation/internal/validator"
	"github.com/jarcoal/httpmock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
	"time"
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
			startTime := validator.Now.Add(-time.Duration(validator.ValidationPeriod) * time.Minute)
			endTime := validator.Now.Add(-time.Duration(validator.ValidationLagComp) * time.Second)

			dbMock.ExpectQuery(fmt.Sprintf(
				`SELECT id FROM hosts WHERE modified_on > '%s' AND modified_on < '%s' ORDER BY id`,
				startTime.Format(utils.TimeFormat()), endTime.Format(utils.TimeFormat()))).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("1234"))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/mockindex/_search?_source=host.id&scroll=60000ms&size=5000&sort=_doc",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/scroll.search.id.response")))

			httpmock.RegisterResponder(
				"GET",
				"http://mock-es:9200/_search/scroll?scroll=60000ms&scroll_id=test-scroll-id-1",
				httpmock.NewStringResponder(200, test.LoadTestDataFile("elasticsearch/empty.scroll.search.id.response")))

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
	})
})
