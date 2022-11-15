package record

import (
	"encoding/json"
	"github.com/RedHatInsights/xjoin-validation/internal/avro"
	"github.com/go-errors/errors"
	"golang.org/x/exp/slices"
	"time"
)

type RecordParser struct {
	Record           map[string]interface{}
	ParsedAvroSchema avro.ParsedAvroSchema
}

func (r *RecordParser) Parse() (parsedRecord map[string]interface{}, err error) {
	parsedRecord = make(map[string]interface{})
	record := r.Record[r.ParsedAvroSchema.RootNode].(map[string]interface{})
	for _, field := range r.ParsedAvroSchema.FullAvroSchema.Fields[0].Type[0].Fields {
		if slices.Contains(r.ParsedAvroSchema.TransformedFields, r.ParsedAvroSchema.RootNode+"."+field.Name) {
			continue //TODO: validate transformed fields
		}

		var xjoinType string
		if len(field.Type) == 1 {
			xjoinType = field.Type[0].XJoinType
		} else {
			xjoinType = field.Type[1].XJoinType
		}

		switch xjoinType {
		case "string":
			if record[field.Name] == nil {
				parsedRecord[field.Name] = ""
			} else {
				switch record[field.Name].(type) {
				case string:
					parsedRecord[field.Name] = record[field.Name]
				case []uint8:
					parsedRecord[field.Name] = string(record[field.Name].([]uint8)[:])
				case interface{}:
					if record[field.Name] == nil {
						parsedRecord[field.Name] = ""
					} else {
						parsedRecord[field.Name] = record[field.Name].(string)
					}
				}
			}
		case "date_nanos":
			switch record[field.Name].(type) {
			case time.Time:
				parsedRecord[field.Name] = record[field.Name].(time.Time)
			case string:
				parsedField, err := time.Parse(time.RFC3339Nano, record[field.Name].(string))
				if err != nil {
					return parsedRecord, errors.Wrap(err, 0)
				}
				parsedRecord[field.Name] = parsedField
			}
		case "json":
			//convert json fields to map[string]interface{}
			var parsedField map[string]interface{}

			if record[field.Name] == nil {
				parsedRecord[field.Name] = parsedField
			} else {
				switch record[field.Name].(type) {
				case string:
					err := json.Unmarshal([]byte(record[field.Name].(string)), &parsedField)
					if err != nil {
						return parsedRecord, errors.Wrap(err, 0)
					}
					parsedRecord[field.Name] = parsedField
				case []uint8:
					err := json.Unmarshal(record[field.Name].([]uint8)[:], &parsedField)
					if err != nil {
						return parsedRecord, errors.Wrap(err, 0)
					}
					parsedRecord[field.Name] = parsedField
				case map[string]interface{}:
					parsedRecord[field.Name] = record[field.Name]
				}
			}
		case "boolean":
			parsedRecord[field.Name] = record[field.Name].(bool)
		case "byte":
			parsedRecord[field.Name] = record[field.Name].(byte)
		case "array":
			parsedRecord[field.Name] = record[field.Name].(string)
		default:
			parsedRecord[field.Name] = record[field.Name]
		}
	}

	parsedRecord = map[string]interface{}{r.ParsedAvroSchema.RootNode: parsedRecord}

	return
}
