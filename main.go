package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"strings"
)

var MainDirectory = os.Getenv("BASED_DB_HOME")

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
				Name:     "Name",
				DataType: TableFieldType(STRING),
			},
			{
				Name:     "date_of_birth",
				DataType: TableFieldType(DATE),
			},
		},
		Indices: []TableIndex{
			{Name: "id", Primary: true},
		},
	})

	dbo.Add(&TableInsertionOperation{
		Table: "people",
		Fields: map[string]string{
			"id":            "1",
			"Name":          "John Doe",
			"date_of_birth": "2000-01-01",
		},
	})

	dbo.Add(&TableInsertionOperation{
		Table: "people",
		Fields: map[string]string{
			"id":            "2",
			"Name":          "Jane Doe",
			"date_of_birth": "2001-01-01",
		},
	})

	dbo.Add(&TableInsertionOperation{
		Table: "people",
		Fields: map[string]string{
			"id":            "3",
			"Name":          "Glenn Doe",
			"date_of_birth": "2002-01-01",
		},
	})

	allResults := dbo.RetrieveAll(&TableRetrieveOperation{
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
	var path = table.DataDir()
	var err = os.MkdirAll(path, 0777)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error occurred: %s\n", err.Error())
		return
	}

	path = table.IndicesDir()
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

	path = table.DefinitionFile()
	err = os.WriteFile(path, bytes, 0644)

	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error occurred: %s\n", err.Error())
		return
	}

	for _, tableIndex := range table.Indices {
		filename := fmt.Sprintf("%s.json", tableIndex.Name)
		path = filepath.Join(table.IndicesDir(), filename)
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
	documentName := fmt.Sprintf("%s.json", uuid.New().String())
	documentPath := filepath.Join(table.DataDir(), documentName)
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
		indexValue, isPresent := data.Fields[index.Name]
		if isPresent {
			indexFile := filepath.Join(table.IndicesDir(), fmt.Sprintf("%s.json", index.Name))
			file, err := os.OpenFile(indexFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "error occurred while opening the index file: %s", err.Error())
				return
			}

			_, err = file.WriteString(fmt.Sprintf("%s:%s\n", indexValue, documentName))
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

type Result = map[string]interface{}
type ResultSet = []Result

func (dbo *DatabaseOperations) RetrieveAll(data *TableRetrieveOperation) ResultSet {
	table := *dbo.getTableDefinition(data.Table)

	primaryIndexName := table.GetPrimaryIndexName()
	indexMap := dbo.serializeIndex(&table, primaryIndexName)

	var allResults ResultSet

	for _, documentName := range indexMap {
		docPath := filepath.Join(table.DataDir(), documentName)
		docContents, err := os.ReadFile(docPath)
		if err != nil {
			panic(fmt.Sprintf("could not load the document: %s: %s", docPath, err.Error()))
		}
		result := make(Result)
		err = json.Unmarshal(docContents, &result)
		if err != nil {
			panic(fmt.Sprintf("could not unmarshall the document: %s: %s", docPath, err.Error()))
		}
		allResults = append(allResults, result)
	}
	return allResults
}

func (dbo *DatabaseOperations) Delete(data *TableDeleteByIndexOperation) bool {
	table := *dbo.getTableDefinition(data.Table)

	if !table.DoesIndexExist(data.Index) {
		return false
	}

	// TODO:
	return false
}

type TableDeleteByIndexOperation struct {
	Table string
	Index string
	Id    string
}

type DatabaseIndexKey = string
type DatabaseIndexDocumentName = string
type DatabaseIndexMap = map[DatabaseIndexKey]DatabaseIndexDocumentName

func (dbo *DatabaseOperations) serializeIndex(table *TableDefinition, indexName string) DatabaseIndexMap {
	indexFilePath := filepath.Join(table.IndicesDir(), fmt.Sprintf("%s.json", indexName))

	file, err := os.Open(indexFilePath)

	if err != nil {
		panic(fmt.Sprintf("could not read the index file: %s: %s", indexFilePath, err.Error()))
	}

	scanner := bufio.NewScanner(file)

	scanner.Split(bufio.ScanLines)
	var text []string

	for scanner.Scan() {
		text = append(text, scanner.Text())
	}

	if err = file.Close(); err != nil {
		panic(fmt.Sprintf("could not close the file: %s : %s", indexFilePath, err.Error()))
	}

	var indexResult = make(map[string]string)

	for _, line := range text {
		pieces := strings.Split(line, ":")
		id := pieces[0]
		documentName := pieces[1]
		indexResult[id] = documentName
	}

	return indexResult
}

type TableDefinition struct {
	Name    string
	Fields  []TableField
	Indices []TableIndex
}

type TableIndex struct {
	Name    string
	Primary bool
}

func (td *TableDefinition) TableDir() string {
	return filepath.Join(MainDirectory, "tables", td.Name)
}

func (td *TableDefinition) IndicesDir() string {
	return filepath.Join(td.TableDir(), "indices")
}

func (td *TableDefinition) DataDir() string {
	return filepath.Join(td.TableDir(), "data")
}

func (td *TableDefinition) DefinitionFile() string {
	return filepath.Join(td.TableDir(), "definition.json")
}

func (td *TableDefinition) GetPrimaryIndexName() string {
	var primaryIndex *TableIndex
	for _, index := range td.Indices {
		if index.Primary {
			primaryIndex = &index
			break
		}
	}

	if primaryIndex == nil {
		panic("no Primary index")
	}

	return primaryIndex.Name
}

func (td *TableDefinition) DoesIndexExist(indexName string) bool {
	for _, index := range td.Indices {
		if index.Name == indexName {
			return true
		}
	}
	return false
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
