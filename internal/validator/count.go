package validator

import (
	"math"

	"github.com/go-errors/errors"
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

	diff := math.Abs(float64(dbCount - esCount))
	result.MismatchCount = int(diff)
	result.MismatchRatio = math.Round(diff/math.Max(math.Max(float64(dbCount), float64(esCount)), 1)*100) / 100

	return
}
