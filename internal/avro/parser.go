package avro

import (
	"encoding/json"
	"github.com/go-errors/errors"
	. "github.com/redhatinsights/xjoin-operator/controllers/avro"
	"golang.org/x/exp/slices"
)

type SchemaParser struct {
	FullSchemaString  string
	IndexSchemaString string
}

type ParsedAvroSchema struct {
	DatabaseColumns   []string
	FullAvroSchema    Schema
	IndexAvroSchema   Schema
	RootNode          string
	TransformedFields []string
}

func (s *SchemaParser) Parse() (parsedSchema ParsedAvroSchema, err error) {
	//unmarshal full avro schema
	var fullAvroSchema Schema
	err = json.Unmarshal([]byte(s.FullSchemaString), &fullAvroSchema)

	if err != nil {
		return parsedSchema, errors.Wrap(err, 0)
	}
	parsedSchema.FullAvroSchema = fullAvroSchema

	//unmarshal index avro schema
	var indexAvroSchema Schema
	err = json.Unmarshal([]byte(s.IndexSchemaString), &indexAvroSchema)
	if err != nil {
		return parsedSchema, errors.Wrap(err, 0)
	}
	parsedSchema.IndexAvroSchema = indexAvroSchema

	//parse root node
	parsedSchema.RootNode = parsedSchema.IndexAvroSchema.Fields[0].Name

	//parse transformed field names
	for _, transformation := range fullAvroSchema.Transformations {
		parsedSchema.TransformedFields = append(parsedSchema.TransformedFields, transformation.OutputField)
	}

	//parse database columns
	parsedSchema.DatabaseColumns = s.parseDatabaseColumns(fullAvroSchema, parsedSchema.TransformedFields)

	return
}

func (s *SchemaParser) parseDatabaseColumns(fullAvroSchema Schema, transformedFields []string) (dbColumns []string) {
	root := fullAvroSchema.Fields[0].Name

	for _, field := range fullAvroSchema.Fields[0].Type[0].Fields {
		if !slices.Contains(transformedFields, root+"."+field.Name) {
			dbColumns = append(dbColumns, field.Name)
		}
	}

	return
}
