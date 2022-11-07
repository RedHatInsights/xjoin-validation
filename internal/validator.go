package internal

import (
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-operator/controllers/utils"
	"math"
	"time"
)

type Validator struct {
	DBClient
	ESClient
	validationPeriod  int
	validationLagComp int
	state             string
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
	if v.state == "INITIAL_SYNC" {
		startTime = time.Unix(86400, 0) //24 hours since epoch
	} else {
		startTime = now.Add(-time.Duration(v.validationPeriod) * time.Minute)
	}
	endTime := now.Add(-time.Duration(v.validationLagComp) * time.Second)

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

func (v *Validator) ValidateContent() {

}
