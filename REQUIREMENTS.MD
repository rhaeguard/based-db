# Requirements for the based-db

or more like a set of steps to create a working database.

- Determine the file format that we will use to persist the data
  - Idea: DynamoDb style primary/secondary/tertiary indices and documents attached to those
    - the process can be sped up by using a better lookup for the indices
- Desired operations:
  - Create a table
    - [x] creating the folder structure
    - [ ] validation for the table attributes
  - Add data
    - [x] basic add
    - [ ] field validation
      - [ ] is field name supported in the table
      - [ ] does the field value conform to its data type
  - Select data
    - [ ] select all by specific table
    - [ ] filter data by a certain criteria
  - Update data
    - [ ] update a particular field in a document
  - Delete data
    - [ ] delete a particular document
- File operations:
  - Read data from a specific file created for a database
  - Update that specific file