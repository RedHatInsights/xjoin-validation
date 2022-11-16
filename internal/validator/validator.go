package validator

import (
	"encoding/json"
	"fmt"
	. "github.com/RedHatInsights/xjoin-validation/internal/database"
	. "github.com/RedHatInsights/xjoin-validation/internal/elasticsearch"
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
	validation "github.com/redhatinsights/xjoin-go-lib/pkg/validation"
)

type Validator struct {
	DBClient
	ESClient
	ValidationPeriod  int
	ValidationLagComp int
	State             string
	dbIds             []string
}

func (v *Validator) Validate() (response validation.ValidationResponse, err error) {
	countResponse, err := v.ValidateCount()
	if err != nil {
		return response, errors.Wrap(err, 0)
	}

	if !countResponse.CountIsValid {
		message := fmt.Sprintf(
			"%v discrepancies while counting. %v documents in elasticsearch. %v rows in database.",
			countResponse.MismatchCount, countResponse.ESCount, countResponse.DBCount)

		response = validation.ValidationResponse{
			Result:  "invalid",
			Reason:  "count mismatch",
			Message: message,
			Details: validation.ResponseDetails{
				TotalMismatch: countResponse.MismatchCount,
			},
		}

		return
	} else {
		countResponseString, err := json.Marshal(countResponse)
		if err != nil {
			return response, errors.Wrap(err, 0)
		}
		fmt.Println(string(countResponseString))
	}

	idsResponse, err := v.ValidateIDs()
	if err != nil {
		return response, errors.Wrap(err, 0)
	}

	if !idsResponse.IDsAreValid {
		message := fmt.Sprintf(
			"%v ids did not match.",
			idsResponse.MismatchCount)

		response = validation.ValidationResponse{
			Result:  "invalid",
			Reason:  "id mismatch",
			Message: message,
			Details: validation.ResponseDetails{
				TotalMismatch:                    idsResponse.MismatchCount,
				IdsMissingFromElasticsearch:      idsResponse.InDBOnly[:utils.Min(50, len(idsResponse.InDBOnly))],
				IdsMissingFromElasticsearchCount: len(idsResponse.InDBOnly),
				IdsOnlyInElasticsearch:           idsResponse.InESOnly[:utils.Min(50, len(idsResponse.InESOnly))],
				IdsOnlyInElasticsearchCount:      len(idsResponse.InESOnly),
			},
		}

		return
	} else {
		idsResponseString, err := json.Marshal(idsResponse)
		if err != nil {
			return response, errors.Wrap(err, 0)
		}
		fmt.Println(string(idsResponseString))
	}

	contentResponse, err := v.ValidateContent()
	if err != nil {
		return response, errors.Wrap(err, 0)
	}

	if !contentResponse.ContentIsValid {
		message := fmt.Sprintf(
			"%v record's contents did not match.",
			contentResponse.MismatchCount)

		response = validation.ValidationResponse{
			Result:  "invalid",
			Reason:  "content mismatch",
			Message: message,
			Details: validation.ResponseDetails{
				TotalMismatch:          contentResponse.MismatchCount,
				IdsWithMismatchContent: []string{},
				MismatchContentDetails: []validation.MismatchContentDetails{},
			},
		}

		return
	} else {
		contentResponseString, err := json.Marshal(contentResponse)
		if err != nil {
			return response, errors.Wrap(err, 0)
		}
		fmt.Println(string(contentResponseString))
	}

	return validation.ValidationResponse{
		Result: "valid",
	}, nil
}
