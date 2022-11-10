package validator

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/go-test/deep"
	"math"
	"strconv"
	"strings"
	"sync"
)

type ValidateContentResult struct {
	MismatchCount     int                 `json:"mismatchCount,omitempty"`
	MismatchRatio     float64             `json:"mismatchRatio,omitempty"`
	ContentIsValid    bool                `json:"contentIsValid,omitempty"`
	MismatchedRecords map[string][]string `json:"mismatchedRecords,omitempty"`
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

func (v *Validator) ValidateContent() (result ValidateContentResult, err error) {
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

		return result, errors.Wrap(errors.New("Error during full validation"), 0)
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
	result.MismatchCount = len(diffsById)
	result.MismatchRatio = float64(result.MismatchCount) / math.Max(float64(len(v.dbIds)), 1)
	//result.ContentIsValid = (result.MismatchRatio * 100) <= float64(i.GetValidationPercentageThreshold())
	result.ContentIsValid = result.MismatchCount == 0

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
	result.MismatchedRecords = mismatchedRecords

	return
}
