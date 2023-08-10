package validator

import (
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
	"math"
	"time"
)

type ValidateIDsResult struct {
	InDBOnly                []string `json:"inDBOnly,omitempty"`
	InESOnly                []string `json:"inESOnly,omitempty"`
	TotalDBRecordsRetrieved int      `json:"totalDBRecordsRetrieved,omitempty"`
	TotalESRecordsRetrieved int      `json:"totalESRecordsRetrieved,omitempty"`
	MismatchCount           int      `json:"mismatchCount,omitempty"`
	MismatchRatio           float64  `json:"mismatchRatio,omitempty"`
	IDsAreValid             bool     `json:"idsAreValid,omitempty"`
}

func (v *Validator) validateIdChunk(dbIds []string, esIds []string) (mismatchCount int, inDBOnly []string, inESOnly []string) {
	inDBOnly = utils.Difference(dbIds, esIds)
	inESOnly = utils.Difference(esIds, dbIds)
	mismatchCount = len(inDBOnly) + len(inESOnly)
	return mismatchCount, inDBOnly, inESOnly
}

func (v *Validator) ValidateIDs() (result ValidateIDsResult, err error) {
	var startTime time.Time
	if v.ValidateEverything == true {
		startTime = time.Unix(86400, 0) //24 hours since epoch
	} else {
		startTime = v.Now.Add(-time.Duration(v.PeriodMin) * time.Minute)
	}
	endTime := v.Now.Add(-time.Duration(v.LagCompSec) * time.Second)

	//validate chunk between startTime and endTime //TODO: can this rely on the presence of a modified_on field?
	var dbIds []string
	dbIds, err = v.DBClient.GetIDsByModifiedOn(startTime, endTime)
	if err != nil {
		return result, errors.Wrap(err, 0)
	}
	v.dbIds = dbIds

	var esIds []string
	esIds, err = v.ESClient.GetIDsByModifiedOn(startTime, endTime)
	if err != nil {
		return result, errors.Wrap(err, 0)
	}

	mismatchCount, inDBOnly, inESOnly := v.validateIdChunk(dbIds, esIds)

	//re-validate any mismatched records to check if they were invalid due to lag
	//this can happen when the modified_on filter excludes records updated between retrieving records from the DB/ES
	if mismatchCount > 0 {
		v.Log.Debug("Double checking mismatched IDs", "inDBOnly", inDBOnly, "inESOnly", inESOnly)
		mismatchedIds := append(inDBOnly, inESOnly...)
		mismatchedDBIds, err := v.DBClient.GetIDsByIDList(mismatchedIds)
		if err != nil {
			return result, errors.Wrap(err, 0)
		}

		mismatchedESIDs, err := v.ESClient.GetIDsByIDList(mismatchedIds)
		if err != nil {
			return result, errors.Wrap(err, 0)
		}

		mismatchCount, inDBOnly, inESOnly = v.validateIdChunk(mismatchedDBIds, mismatchedESIDs)
	}

	result.InDBOnly = inDBOnly
	result.InESOnly = inESOnly
	result.MismatchCount = mismatchCount
	result.MismatchRatio = float64(mismatchCount) / math.Max(float64(v.dbCount)+float64(len(result.InESOnly)), 1)
	result.TotalDBRecordsRetrieved = len(dbIds)
	result.TotalESRecordsRetrieved = len(esIds)

	if mismatchCount > 0 { //TODO: threshold
		result.IDsAreValid = false
	} else {
		result.IDsAreValid = true
	}

	return
}
