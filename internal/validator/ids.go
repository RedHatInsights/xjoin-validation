package validator

import (
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-operator/controllers/utils"
	"math"
	"time"
)

type ValidateIDsResult struct {
	InDBOnly              []string `json:"inDBOnly,omitempty"`
	InESOnly              []string `json:"inESOnly,omitempty"`
	TotalDBHostsRetrieved int      `json:"totalDBHostsRetrieved,omitempty"`
	TotalESHostsRetrieved int      `json:"totalESHostsRetrieved,omitempty"`
	MismatchCount         int      `json:"mismatchCount,omitempty"`
	MismatchRatio         float64  `json:"mismatchRatio,omitempty"`
	IDsAreValid           bool     `json:"idsAreValid,omitempty"`
}

func (v *Validator) validateIdChunk(dbIds []string, esIds []string) (mismatchCount int, inDBOnly []string, inESOnly []string) {
	inDBOnly = utils.Difference(dbIds, esIds)
	inESOnly = utils.Difference(esIds, dbIds)
	mismatchCount = len(inDBOnly) + len(inESOnly)
	return mismatchCount, inDBOnly, inESOnly
}

func (v *Validator) ValidateIDs() (result ValidateIDsResult, err error) {
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
		return result, errors.Wrap(err, 0)
	}

	esIds, err := v.ESClient.GetIDsByModifiedOn(startTime, endTime)
	if err != nil {
		return result, errors.Wrap(err, 0)
	}

	mismatchCount, inDBOnly, inESOnly := v.validateIdChunk(dbIds, esIds)

	//re-validate any mismatched hosts to check if they were invalid due to lag
	//this can happen when the modified_on filter excludes hosts updated between retrieving hosts from the DB/ES
	if result.MismatchCount > 0 {
		mismatchedIds := append(dbIds, esIds...)
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
	result.MismatchRatio = float64(mismatchCount) / math.Max(float64(len(dbIds)), 1)
	result.TotalDBHostsRetrieved = len(dbIds)
	result.TotalESHostsRetrieved = len(esIds)

	if mismatchCount > 0 {
		result.IDsAreValid = false
	} else {
		result.IDsAreValid = true
	}

	//ContentIsValid = (MismatchRatio * 100) <= float64(validationThresholdPercent)

	return
}
