package main

import (
	"encoding/json"
	"fmt"
	. "github.com/RedHatInsights/xjoin-validation/pkg"
	"os"
)

func main() {
	fmt.Println("Starting validation...")

	response := Response{
		Result:  "invalid",
		Reason:  "count mismatch",
		Message: "20 rows missing from Elasticsearch",
		Details: ResponseDetails{
			TotalMismatch:                    20,
			IdsMissingFromElasticsearch:      []string{"1", "2", "3"},
			IdsMissingFromElasticsearchCount: 20,
			IdsOnlyInElasticsearch:           nil,
			IdsOnlyInElasticsearchCount:      0,
			IdsWithMismatchContent:           nil,
			MismatchContentDetails:           nil,
		},
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		fmt.Println("Unable to marshal response to JSON")
		os.Exit(-1)
	}

	fmt.Println(string(jsonResponse))
	os.Exit(0)
}
