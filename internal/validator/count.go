package validator

import (
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
	"math"
)

type ValidateCountResult struct {
	CountIsValid  bool    `json:"countIsValid,omitempty"`
	ESCount       int     `json:"esCount,omitempty"`
	DBCount       int     `json:"dbCount,omitempty"`
	MismatchCount int     `json:"MismatchCount,omitempty"`
	MismatchRatio float64 `json:"MismatchRatio,omitempty"`
}

func (v *Validator) ValidateCount() (result ValidateCountResult, err error) {
	dbCount, err := v.DBClient.CountTable()
	if err != nil {
		return result, errors.Wrap(err, 0)
	}
	result.DBCount = dbCount

	esCount, err := v.ESClient.CountIndex()
	if err != nil {
		return result, errors.Wrap(err, 0)
	}
	result.ESCount = esCount

	if dbCount != esCount {
		result.CountIsValid = false
	} else {
		result.CountIsValid = true
	}

	result.MismatchCount = utils.Abs(dbCount - esCount)
	result.MismatchRatio = float64(result.MismatchCount) / math.Max(float64(dbCount), 1)

	return
}
