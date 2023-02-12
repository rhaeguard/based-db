package main

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"
)

const (
	MainDirectory = "C:\\Programming\\based-db-test-playground"
)

func main() {
	dbo := &DatabaseOperations{}

	dbo.createTable(&TableDefinition{
		Name: "people",
		Fields: []TableField{
			{
				Name:     "id",
				DataType: TableFieldType(INT),
			},
			{
				Name:     "name",
				DataType: TableFieldType(STRING),
			},
			{
				Name:     "date_of_birth",
				DataType: TableFieldType(DATE),
			},
		},
		Indices: []string{
			"id",
		},
	})

	dbo.add(&TableInsertionOperation{
		Table: "people",
		Fields: map[string]string{
			"id":            "1",
			"name":          "John Doe",
			"date_of_birth": "2000-01-01",
		},
	})
}

type DatabaseOperations struct{}

func (dbo *DatabaseOperations) createTable(table *TableDefinition) {
	// create the necessary directories
	mainTableDir := filepath.Join(MainDirectory, "tables", table.Name)
	var path = filepath.Join(mainTableDir, "data")
	var err = os.MkdirAll(path, 0777)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error occurred: %s\n", err.Error())
		return
	}

	path = filepath.Join(mainTableDir, "indices")
	err = os.MkdirAll(path, 0777)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error occurred: %s\n", err.Error())
		return
	}

	bytes, err := json.Marshal(table)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error occurred: %s\n", err.Error())
		return
	}

	path = filepath.Join(mainTableDir, "definition.json")
	err = os.WriteFile(path, bytes, 0644)

	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error occurred: %s\n", err.Error())
		return
	}

	for _, tableIndex := range table.Indices {
		filename := fmt.Sprintf("%s.json", tableIndex)
		path = filepath.Join(mainTableDir, "indices", filename)
		err = os.WriteFile(path, []byte{}, 0644)

		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "error occurred while creating the tableIndex [%s]: %s\n", tableIndex, err.Error())
			return
		}
	}
}

func (dbo *DatabaseOperations) add(data *TableInsertionOperation) {
	// get the table definition
	tableDefinition := filepath.Join(MainDirectory, "tables", data.Table, "definition.json")
	contents, err := os.ReadFile(tableDefinition)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error occurred while reading the table definition: %s", err.Error())
		return
	}
	table := TableDefinition{}
	if err = json.Unmarshal(contents, &table); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error occurred while unmarshalling the table definition: %s", err.Error())
		return
	}
	// create a document for this entry
	documentPath := filepath.Join(MainDirectory, "tables", table.Name, "data", fmt.Sprintf("%s.json", uuid.New().String()))
	contents, err = json.Marshal(data.Fields)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error occurred while marshalling the fields map: %s", err.Error())
		return
	}
	err = os.WriteFile(documentPath, contents, 0777)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error occurred while writing the document to a file: %s", err.Error())
		return
	}
	// grab the index value
	for _, index := range table.Indices {
		indexValue, isPresent := data.Fields[index]
		if isPresent {
			indexFile := filepath.Join(MainDirectory, "tables", table.Name, "indices", fmt.Sprintf("%s.json", index))
			file, err := os.OpenFile(indexFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "error occurred while opening the index file: %s", err.Error())
				return
			}

			_, err = file.WriteString(fmt.Sprintf("%s:%s\n", indexValue, documentPath))
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "error occurred while writing to the index file: %s", err.Error())
				return
			}

			if err := file.Close(); err != nil {
				panic("could not close the file")
			}
		}
	}
}

type TableDefinition struct {
	Name    string
	Fields  []TableField
	Indices []string
}

type TableField struct {
	Name     string
	DataType TableFieldType
}

type TableInsertionOperation struct {
	Table  string
	Fields map[string]string
}

type TableFieldType string

const (
	INT     string = "INT"
	STRING  string = "STRING"
	DATE    string = "DATE"
	DOUBLE  string = "DOUBLE"
	BOOLEAN string = "BOOLEAN"
)
