package internal

import (
	"fmt"
	. "github.com/RedHatInsights/xjoin-validation/internal/database"
	. "github.com/RedHatInsights/xjoin-validation/internal/elasticsearch"
	"github.com/go-errors/errors"
	"github.com/go-test/deep"
	"github.com/redhatinsights/xjoin-operator/api/v1alpha1"
	"github.com/redhatinsights/xjoin-operator/controllers/utils"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Validator struct {
	DBClient
	ESClient
	ValidationPeriod  int
	ValidationLagComp int
	State             string
	dbIds             []string
}

func (v *Validator) Validate() (response v1alpha1.ValidationResponse, err error) {
	countResponse, err := v.ValidateCount()
	if err != nil {
		return response, errors.Wrap(err, 0)
	}

	if !countResponse.isValid {
		message := fmt.Sprintf(
			"%v discrepancies while counting. %v documents in elasticsearch. %v rows in database.",
			countResponse.mismatchCount, countResponse.esCount, countResponse.dbCount)

		response = v1alpha1.ValidationResponse{
			Result:  "invalid",
			Reason:  "count mismatch",
			Message: message,
			Details: v1alpha1.ResponseDetails{
				TotalMismatch: countResponse.mismatchCount,
			},
		}

		return
	} else {
		fmt.Println("count is valid")
	}

	idsResponse, err := v.ValidateIDs()
	if err != nil {
		return response, errors.Wrap(err, 0)
	}

	if !idsResponse.isValid {
		message := fmt.Sprintf(
			"%v ids did not match.",
			idsResponse.mismatchCount)

		response = v1alpha1.ValidationResponse{
			Result:  "invalid",
			Reason:  "id mismatch",
			Message: message,
			Details: v1alpha1.ResponseDetails{
				TotalMismatch:                    idsResponse.mismatchCount,
				IdsMissingFromElasticsearch:      idsResponse.inDBOnly[:utils.Min(50, len(idsResponse.inDBOnly))],
				IdsMissingFromElasticsearchCount: len(idsResponse.inDBOnly),
				IdsOnlyInElasticsearch:           idsResponse.inESOnly[:utils.Min(50, len(idsResponse.inESOnly))],
				IdsOnlyInElasticsearchCount:      len(idsResponse.inESOnly),
			},
		}

		return
	} else {
		fmt.Println("ids are valid")
	}

	contentResponse, err := v.ValidateContent()
	if err != nil {
		return response, errors.Wrap(err, 0)
	}

	if !contentResponse.isValid {
		message := fmt.Sprintf(
			"%v record's contents did not match.",
			contentResponse.mismatchCount)

		response = v1alpha1.ValidationResponse{
			Result:  "invalid",
			Reason:  "content mismatch",
			Message: message,
			Details: v1alpha1.ResponseDetails{
				TotalMismatch:          contentResponse.mismatchCount,
				IdsWithMismatchContent: []string{},
				MismatchContentDetails: []v1alpha1.MismatchContentDetails{},
			},
		}

		return
	} else {
		fmt.Println("content is valid")
	}

	return v1alpha1.ValidationResponse{
		Result: "valid",
	}, nil
}

type ValidateCountResponse struct {
	isValid       bool
	esCount       int
	dbCount       int
	mismatchCount int
	mismatchRatio float64
}

func (v *Validator) ValidateCount() (response ValidateCountResponse, err error) {
	dbCount, err := v.DBClient.CountTable()
	if err != nil {
		return response, errors.Wrap(err, 0)
	}
	response.dbCount = dbCount

	esCount, err := v.ESClient.CountIndex()
	if err != nil {
		return response, errors.Wrap(err, 0)
	}
	response.esCount = esCount

	if dbCount != esCount {
		response.isValid = false
	} else {
		response.isValid = true
	}

	response.mismatchCount = utils.Abs(dbCount - esCount)
	response.mismatchRatio = float64(response.mismatchCount) / math.Max(float64(dbCount), 1)

	return
}

type ValidateIDsResponse struct {
	inDBOnly              []string
	inESOnly              []string
	totalDBHostsRetrieved int
	totalESHostsRetrieved int
	mismatchCount         int
	mismatchRatio         float64
	isValid               bool
}

func (v *Validator) validateIdChunk(dbIds []string, esIds []string) (mismatchCount int, inDBOnly []string, inESOnly []string) {
	inDBOnly = utils.Difference(dbIds, esIds)
	inESOnly = utils.Difference(esIds, dbIds)
	mismatchCount = len(inDBOnly) + len(inESOnly)
	return mismatchCount, inDBOnly, inESOnly
}

func (v *Validator) ValidateIDs() (response ValidateIDsResponse, err error) {
	now := time.Now().UTC()

	var startTime time.Time
	if v.State == "INITIAL_SYNC" {
		startTime = time.Unix(86400, 0) //24 hours since epoch
	} else {
		startTime = now.Add(-time.Duration(v.ValidationPeriod) * time.Minute)
	}
	endTime := now.Add(-time.Duration(v.ValidationLagComp) * time.Second)

	//validate chunk between startTime and endTime //TODO: can this rely on the presence of a modified_on field?
	dbIds, err := v.DBClient.GetIDsByModifiedOn(startTime, endTime)
	if err != nil {
		return response, errors.Wrap(err, 0)
	}

	esIds, err := v.ESClient.GetIDsByModifiedOn(startTime, endTime)
	if err != nil {
		return response, errors.Wrap(err, 0)
	}

	mismatchCount, inDBOnly, inESOnly := v.validateIdChunk(dbIds, esIds)

	//re-validate any mismatched hosts to check if they were invalid due to lag
	//this can happen when the modified_on filter excludes hosts updated between retrieving hosts from the DB/ES
	if response.mismatchCount > 0 {
		mismatchedIds := append(dbIds, esIds...)
		dbIds, err = v.DBClient.GetIDsByIDList(mismatchedIds)
		if err != nil {
			return response, errors.Wrap(err, 0)
		}

		esIds, err = v.ESClient.GetIDsByIDList(mismatchedIds)
		if err != nil {
			return response, errors.Wrap(err, 0)
		}

		mismatchCount, inDBOnly, inESOnly = v.validateIdChunk(dbIds, esIds)
	}

	response.inDBOnly = inDBOnly
	response.inESOnly = inESOnly
	response.mismatchCount = mismatchCount
	response.mismatchRatio = float64(mismatchCount) / math.Max(float64(len(dbIds)), 1)
	response.totalDBHostsRetrieved = len(dbIds)
	response.totalESHostsRetrieved = len(esIds)

	if mismatchCount > 0 {
		response.isValid = false
	} else {
		response.isValid = true
	}

	//isValid = (mismatchRatio * 100) <= float64(validationThresholdPercent)

	return
}

type idDiff struct {
	id   string
	diff string
}

func (v *Validator) validateFullChunkSync(chunk []string) (allIdDiffs []idDiff, err error) {
	//retrieve hosts from db and es
	esDocuments, err := v.ESClient.GetDocumentsByIDs(chunk)
	if err != nil {
		return allIdDiffs, errors.Wrap(err, 0)
	}
	if esDocuments == nil {
		esDocuments = make([]interface{}, 0)
	}

	dbHosts, err := v.DBClient.GetRowsByIDs(chunk)
	if err != nil {
		return allIdDiffs, errors.Wrap(err, 0)
	}
	if dbHosts == nil {
		dbHosts = make([]interface{}, 0)
	}

	deep.MaxDiff = len(chunk) * 100
	diffs := deep.Equal(dbHosts, esDocuments)

	//build the change object for logging
	for _, diff := range diffs {
		idxStr := diff[strings.Index(diff, "[")+1 : strings.Index(diff, "]")]

		var idx int64
		idx, err = strconv.ParseInt(idxStr, 10, 64)
		if err != nil {
			return
		}
		id := chunk[idx]
		allIdDiffs = append(allIdDiffs, idDiff{id: id, diff: diff})
	}

	return
}

func (v *Validator) validateFullChunkAsync(chunk []string, allIdDiffs chan idDiff, errorsChan chan error, wg *sync.WaitGroup) {
	defer wg.Done()

	diffs, err := v.validateFullChunkSync(chunk)
	if err != nil {
		errorsChan <- err
		return
	}

	for _, diff := range diffs {
		allIdDiffs <- diff
	}

	return
}

type ValidateContentResponse struct {
	mismatchCount     int
	mismatchRatio     float64
	isValid           bool
	mismatchedRecords map[string][]string
}

func (v *Validator) ValidateContent() (response ValidateContentResponse, err error) {
	allIdDiffs := make(chan idDiff, len(v.dbIds)*100)
	errorsChan := make(chan error, len(v.dbIds))
	numThreads := 0
	wg := new(sync.WaitGroup)

	//chunkSize := i.Parameters.FullValidationChunkSize.Int() //TODO parameterize
	chunkSize := 100
	var numChunks = int(math.Ceil(float64(len(v.dbIds)) / float64(chunkSize)))

	for j := 0; j < numChunks; j++ {
		//determine which chunk of systems to validate
		start := j * chunkSize
		var end int
		if j == numChunks-1 && len(v.dbIds)%chunkSize > 0 {
			end = start + (len(v.dbIds) % chunkSize)
		} else {
			end = start + chunkSize
		}
		chunk := v.dbIds[start:end]

		//validate chunks in parallel
		wg.Add(1)
		numThreads += 1
		go v.validateFullChunkAsync(chunk, allIdDiffs, errorsChan, wg)

		maxThreads := 10 //TODO: parameterize
		if numThreads == maxThreads || j == numChunks-1 {
			wg.Wait()
			numThreads = 0
		}
	}

	close(allIdDiffs)
	close(errorsChan)

	if len(errorsChan) > 0 {
		for e := range errorsChan {
			fmt.Println("Error during full validation")
			fmt.Println(e)
		}

		return response, errors.Wrap(errors.New("Error during full validation"), 0)
	}

	//double check mismatched hosts to account for lag
	var mismatchedIds []string
	for d := range allIdDiffs {
		mismatchedIds = append(mismatchedIds, d.id)
	}

	diffsById := make(map[string][]string)
	if len(mismatchedIds) > 0 {
		var diffs []idDiff
		diffs, err = v.validateFullChunkSync(mismatchedIds)
		if err != nil {
			return
		}

		//group diffs by id for counting mismatched systems
		for _, d := range diffs {
			diffsById[d.id] = append(diffsById[d.id], d.diff)
		}
	}

	//determine if the data is valid within the threshold
	response.mismatchCount = len(diffsById)
	response.mismatchRatio = float64(response.mismatchCount) / math.Max(float64(len(v.dbIds)), 1)
	//response.isValid = (response.mismatchRatio * 100) <= float64(i.GetValidationPercentageThreshold())
	response.isValid = response.mismatchCount == 0

	//log at most 50 invalid systems
	mismatchedRecords := make(map[string][]string)
	idx := 0
	for key, val := range diffsById {
		if idx > 50 {
			break
		}
		mismatchedRecords[key] = val
		idx++
	}
	response.mismatchedRecords = mismatchedRecords

	return
}
