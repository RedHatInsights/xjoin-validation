package validator

import (
	"encoding/json"
	"fmt"
	goErrors "github.com/go-errors/errors"
	"github.com/go-test/deep"
	validation "github.com/redhatinsights/xjoin-go-lib/pkg/validation"
	"math"
	"strconv"
	"strings"
	"sync"
)

type ValidateContentResult struct {
	MismatchCount         int                          `json:"mismatchCount,omitempty"`
	MismatchRatio         float64                      `json:"mismatchRatio,omitempty"`
	ContentIsValid        bool                         `json:"contentIsValid,omitempty"`
	MismatchedRecords     validation.MismatchedRecords `json:"mismatchedRecords,omitempty"`
	MismatchedIDs         []string                     `json:"mismatchedIDs,omitempty"`
	TotalRecordsValidated int                          `json:"totalRecordsValidated,omitempty"`
}

func (v *Validator) getDBRecord(id string, dbRecords []map[string]interface{}) (string, error) {
	for _, record := range dbRecords {
		if record[v.RootNode] != nil {
			rootNodeMap, ok := record[v.RootNode].(map[string]interface{})
			if !ok {
				continue
			}

			if rootNodeMap["id"] == id { //TODO: parse ID field from schema
				response, err := json.Marshal(record)
				if err != nil {
					return "", goErrors.Wrap(err, 0)
				}
				return string(response), nil
			}
		}
	}
	return "", nil
}

func (v *Validator) getESDocument(id string, esDocuments []map[string]interface{}) (string, error) {
	for _, document := range esDocuments {
		if document[v.RootNode] != nil {
			rootNodeMap, ok := document[v.RootNode].(map[string]interface{})
			if !ok {
				continue
			}

			if rootNodeMap["id"] == id { //TODO: parse ID field from schema
				response, err := json.Marshal(document)
				if err != nil {
					return "", goErrors.Wrap(err, 0)
				}
				return string(response), nil
			}
		}
	}
	return "", nil
}

func (v *Validator) validateFullChunkSync(chunk []string) (allIdDiffs validation.MismatchedRecords, err error) {
	allIdDiffs = make(validation.MismatchedRecords)
	//retrieve records from db and es
	esDocuments, err := v.ESClient.GetDocumentsByIDs(chunk)
	if err != nil {
		return allIdDiffs, goErrors.Wrap(err, 0)
	}
	if esDocuments == nil {
		esDocuments = make([]map[string]interface{}, 0)
	}

	dbRecords, err := v.DBClient.GetRowsByIDs(chunk)
	if err != nil {
		return allIdDiffs, goErrors.Wrap(err, 0)
	}
	if dbRecords == nil {
		dbRecords = make([]map[string]interface{}, 0)
	}

	deep.MaxDiff = len(chunk) * 100
	diffs := deep.Equal(dbRecords, esDocuments)

	//build the change object for logging
	for _, diff := range diffs {
		//extract the index from the diff to get the id from the chunk
		idxStr := diff[strings.Index(diff, "[")+1 : strings.Index(diff, "]")]

		var idx int64
		idx, err = strconv.ParseInt(idxStr, 10, 64)
		if err != nil {
			return
		}
		id := chunk[idx]

		//add the diff to the map of id/diffs
		_, hasKey := allIdDiffs[id]
		if hasKey {
			allIdDiffs[id].AddDiff(diff)
		} else {
			esDocumentString, err := v.getESDocument(id, esDocuments)
			if err != nil {
				return allIdDiffs, goErrors.Wrap(err, 0)
			}
			dbRecordString, err := v.getDBRecord(id, dbRecords)
			if err != nil {
				return allIdDiffs, goErrors.Wrap(err, 0)
			}

			allIdDiffs[id] = &validation.ContentDiff{
				ESDocument: esDocumentString,
				DBRecord:   dbRecordString,
			}
			allIdDiffs[id].AddDiff(diff)
		}
	}

	return
}

func (v *Validator) validateFullChunkAsync(chunk []string, allIdDiffs chan validation.MismatchedRecords, errorsChan chan error, wg *sync.WaitGroup) {
	defer wg.Done()

	diffs, err := v.validateFullChunkSync(chunk)
	if err != nil {
		errorsChan <- err
		return
	}

	allIdDiffs <- diffs
}

func (v *Validator) ValidateContent() (result ValidateContentResult, err error) {
	v.Log.Debug("starting content validation", "num ids", len(v.dbIds))

	//chunkSize := i.Parameters.FullValidationChunkSize.Int() //TODO parameterize
	chunkSize := 100
	var numChunks = int(math.Ceil(float64(len(v.dbIds)) / float64(chunkSize)))

	allIdDiffs := make(chan validation.MismatchedRecords, numChunks)
	errorsChan := make(chan error, len(v.dbIds))
	numThreads := 0
	wg := new(sync.WaitGroup)

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
		v.Log.Debug("starting content validation thread", "thread number", numThreads, "chunk start", start, "chunk end", end)
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
		var allErrors error
		for e := range errorsChan {
			fmt.Println("Error during full validation")
			fmt.Println(e)
			//allErrors = errors.Join(err, e)
			allErrors = e //TODO: temporary until ubi9 releases a go 1.20 image
		}

		return result, goErrors.Wrap(allErrors, 0)
	}

	//double check mismatched records to account for lag
	mismatchedIds := make([]string, 0, len(allIdDiffs))
	for currentDiff := range allIdDiffs {
		for id := range currentDiff {
			mismatchedIds = append(mismatchedIds, id)
		}
	}

	var doubleCheckedDiffs validation.MismatchedRecords
	if len(mismatchedIds) > 0 {
		doubleCheckedDiffs, err = v.validateFullChunkSync(mismatchedIds)
		if err != nil {
			return
		}

		mismatchedIds = []string{}
		for id := range doubleCheckedDiffs {
			mismatchedIds = append(mismatchedIds, id)
		}
	}

	//determine if the data is valid within the threshold
	result.MismatchCount = len(doubleCheckedDiffs)
	result.MismatchRatio = float64(result.MismatchCount) / math.Max(float64(len(v.dbIds)), 1)
	result.ContentIsValid = (result.MismatchRatio * 100) <= float64(v.InvalidThresholdPercentage)
	result.MismatchedIDs = mismatchedIds
	result.TotalRecordsValidated = len(v.dbIds)

	//log at most 50 invalid systems
	if result.MismatchCount > 50 {
		counter := 1
		for id, diff := range doubleCheckedDiffs {
			if counter > 50 {
				break
			}

			result.MismatchedRecords[id] = diff
			counter += 1
		}
	} else {
		result.MismatchedRecords = doubleCheckedDiffs
	}

	return
}
