package validator

import (
	"encoding/json"
	"fmt"
	. "github.com/RedHatInsights/xjoin-validation/internal/database"
	. "github.com/RedHatInsights/xjoin-validation/internal/elasticsearch"
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-operator/api/v1alpha1"
	"github.com/redhatinsights/xjoin-operator/controllers/utils"
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

	if !countResponse.CountIsValid {
		message := fmt.Sprintf(
			"%v discrepancies while counting. %v documents in elasticsearch. %v rows in database.",
			countResponse.MismatchCount, countResponse.ESCount, countResponse.DBCount)

		response = v1alpha1.ValidationResponse{
			Result:  "invalid",
			Reason:  "count mismatch",
			Message: message,
			Details: v1alpha1.ResponseDetails{
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

		response = v1alpha1.ValidationResponse{
			Result:  "invalid",
			Reason:  "id mismatch",
			Message: message,
			Details: v1alpha1.ResponseDetails{
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

		response = v1alpha1.ValidationResponse{
			Result:  "invalid",
			Reason:  "content mismatch",
			Message: message,
			Details: v1alpha1.ResponseDetails{
				TotalMismatch:          contentResponse.MismatchCount,
				IdsWithMismatchContent: []string{},
				MismatchContentDetails: []v1alpha1.MismatchContentDetails{},
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

	return v1alpha1.ValidationResponse{
		Result: "valid",
	}, nil
}
