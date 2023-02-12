# Requirements for the based-db

or more like a set of steps to create a working database.

- Determine the file format that we will use to persist the data
  - Idea: DynamoDb style primary/secondary/tertiary indices and documents attached to those
    - the process can be sped up by using a better lookup for the indices
- Desired operations:
  - Create a table
  - Add data
  - Select data
  - Filter data
  - Update data
  - Delete data
- File operations:
  - Read data from a specific file created for a database
  - Update that specific file