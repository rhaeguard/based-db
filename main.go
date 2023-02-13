package main

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"strings"
)

const (
	MainDirectory = "C:\\Programming\\based-db-test-playground"
)

func main() {
	dbo := &DatabaseOperations{}

	dbo.CreateTable(&TableDefinition{
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

	dbo.Add(&TableInsertionOperation{
		Table: "people",
		Fields: map[string]string{
			"id":            "1",
			"name":          "John Doe",
			"date_of_birth": "2000-01-01",
		},
	})

	dbo.Add(&TableInsertionOperation{
		Table: "people",
		Fields: map[string]string{
			"id":            "2",
			"name":          "Jane Doe",
			"date_of_birth": "2001-01-01",
		},
	})

	dbo.Add(&TableInsertionOperation{
		Table: "people",
		Fields: map[string]string{
			"id":            "3",
			"name":          "Glenn Doe",
			"date_of_birth": "2002-01-01",
		},
	})

	allResults := dbo.Retrieve(&TableRetrieveOperation{
		Table:  "people",
		Filter: RetrievalFilter{},
	})

	for _, result := range allResults {
		fmt.Println(result)
	}
}

type DatabaseOperations struct{}

func (dbo *DatabaseOperations) getTableDefinition(tableName string) *TableDefinition {
	// get the table definition
	tableDefinition := filepath.Join(MainDirectory, "tables", tableName, "definition.json")
	contents, err := os.ReadFile(tableDefinition)
	if err != nil {
		panic(fmt.Sprintf("error occurred while reading the table definition: %s", err.Error()))
	}
	table := TableDefinition{}
	if err = json.Unmarshal(contents, &table); err != nil {
		panic(fmt.Sprintf("error occurred while unmarshalling the table definition: %s", err.Error()))
	}
	return &table
}

func (dbo *DatabaseOperations) CreateTable(table *TableDefinition) {
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

func (dbo *DatabaseOperations) Add(data *TableInsertionOperation) {
	// get the table definition
	table := *dbo.getTableDefinition(data.Table)
	// create a document for this entry
	documentPath := filepath.Join(MainDirectory, "tables", table.Name, "data", fmt.Sprintf("%s.json", uuid.New().String()))
	contents, err := json.Marshal(data.Fields)
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

// type Result map[string]interface{}
//type ResultSet []map[string]interface{}

func (dbo *DatabaseOperations) Retrieve(data *TableRetrieveOperation) []map[string]interface{} {
	table := *dbo.getTableDefinition(data.Table)
	// get all files in the appropriate data dir
	dataDirPath := filepath.Join(MainDirectory, "tables", table.Name, "data")
	entries, err := os.ReadDir(dataDirPath)
	fmt.Printf("Entries: %d\n", len(entries))
	if err != nil {
		panic(fmt.Sprintf("could not load the files from the directory: %s: %s", dataDirPath, err.Error()))
	}
	var allResults []map[string]interface{}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".json") {
			docPath := filepath.Join(dataDirPath, entry.Name())
			docContents, err := os.ReadFile(docPath)
			if err != nil {
				panic(fmt.Sprintf("could not load the document: %s: %s", docPath, err.Error()))
			}
			result := make(map[string]interface{})
			err = json.Unmarshal(docContents, &result)
			if err != nil {
				panic(fmt.Sprintf("could not unmarshall the document: %s: %s", docPath, err.Error()))
			}
			allResults = append(allResults, result)
		}
	}
	return allResults
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

type TableRetrieveOperation struct {
	Table  string
	Filter RetrievalFilter
}

type RetrievalFilter struct {
}

type TableFieldType string

const (
	INT     string = "INT"
	STRING  string = "STRING"
	DATE    string = "DATE"
	DOUBLE  string = "DOUBLE"
	BOOLEAN string = "BOOLEAN"
)
