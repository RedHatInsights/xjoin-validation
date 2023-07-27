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
	if v.State == "INITIAL_SYNC" {
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
	if result.MismatchCount > 0 {
		mismatchedIds := append(inDBOnly, inESOnly...)
		dbIds, err = v.DBClient.GetIDsByIDList(mismatchedIds)
		if err != nil {
			return result, errors.Wrap(err, 0)
		}

		esIds, err = v.ESClient.GetIDsByIDList(mismatchedIds)
		if err != nil {
			return result, errors.Wrap(err, 0)
		}

		mismatchCount, inDBOnly, inESOnly = v.validateIdChunk(dbIds, esIds)
	}

	result.InDBOnly = inDBOnly
	result.InESOnly = inESOnly
	result.MismatchCount = mismatchCount
	result.MismatchRatio = float64(mismatchCount) / math.Max(float64(len(dbIds))+float64(len(result.InESOnly)), 1)
	result.TotalDBRecordsRetrieved = len(dbIds)
	result.TotalESRecordsRetrieved = len(esIds)

	if mismatchCount > 0 {
		result.IDsAreValid = false
	} else {
		result.IDsAreValid = true
	}

	//ContentIsValid = (MismatchRatio * 100) <= float64(validationThresholdPercent)

	return
}
