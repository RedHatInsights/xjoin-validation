package avro

import (
	"encoding/json"
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-go-lib/pkg/avro"
	"golang.org/x/exp/slices"
)

type SchemaParser struct {
	FullSchemaString string
}

type ParsedAvroSchema struct {
	DatabaseColumns   []string
	FullAvroSchema    avro.Schema
	RootNode          string
	TransformedFields []string
}

func (s *SchemaParser) Parse() (parsedSchema ParsedAvroSchema, err error) {
	//unmarshal full avro schema
	var fullAvroSchema avro.Schema
	err = json.Unmarshal([]byte(s.FullSchemaString), &fullAvroSchema)

	if err != nil {
		return parsedSchema, errors.Wrap(err, 0)
	}
	parsedSchema.FullAvroSchema = fullAvroSchema

	//parse root node
	if len(parsedSchema.FullAvroSchema.Fields) == 0 {
		return parsedSchema, errors.Wrap(errors.New("root field missing from FullAvroSchema"), 0)
	}
	parsedSchema.RootNode = parsedSchema.FullAvroSchema.Fields[0].Name

	//parse transformed field names
	for _, transformation := range fullAvroSchema.Transformations {
		parsedSchema.TransformedFields = append(parsedSchema.TransformedFields, transformation.OutputField)
	}

	//parse database columns
	parsedSchema.DatabaseColumns = s.parseDatabaseColumns(fullAvroSchema, parsedSchema.TransformedFields)

	return
}

func (s *SchemaParser) parseDatabaseColumns(fullAvroSchema avro.Schema, transformedFields []string) (dbColumns []string) {
	root := fullAvroSchema.Fields[0].Name

	for _, field := range fullAvroSchema.Fields[0].Type[0].Fields {
		if !slices.Contains(transformedFields, root+"."+field.Name) {
			dbColumns = append(dbColumns, field.Name)
		}
	}

	return
}
