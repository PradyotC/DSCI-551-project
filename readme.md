# SimpleDB System User Guide

## Introduction
Welcome to the SimpleDB System User Guide. This document serves as an instructional manual for operating the SimpleDB relational database system. Below, you will find detailed instructions on how to use various commands to interact with the database as well as an overview of the file structure.

## File Structure Description

### Code Components

- **SimpleDB (SimpleTableManager.py)**: Serves as the core Database Manager responsible for managing data storage, retrieval, and manipulation.
- **CLI (cli.py)**: Functions as the user interface, interpreting user commands and directing them to SimpleDB. It is the system's entry point.

### Database

- **Data Directory (data/)**: The SimpleDB system stores all data in a directory called `data`. This directory contains all the tables in the database, as well as the metadata for each table. The `data` directory is organized as follows:
  - `data/table_name/`: The main directory for a specific table.
    - `table_name_schema.json`: Contains the schema details, including column names, data types, and the primary key.
    - `table_name_metadata.json`: Stores metadata such as the auto-increment ID and a list of deleted IDs.
    - `table_name_0.csv`, `table_name_1.csv`, ...: These are CSV files, with each file storing up to 64 rows of table data.
    - `table_name_primary_key_index.json`: Provides an index for quick primary key lookups.




## Getting Started

To begin using the SimpleDB system, open your terminal and navigate to the directory containing the SimpleDB code. Then, start the system by running the command-line interface with Python 3:

```
python3 cli.py
```

This will launch the MyDB CLI, allowing you to enter and execute database commands.

## Command Structure and Examples

### Basic Commands

- **Show Tables**: Lists all the tables within the database.
  ```
  show tables
  ```

- **Exit**: Exits the system.
  ```
  exit
  ```

### Table Operations

- **Create Table**: Creates a new table with the specified schema.
  ```
  make table TABLE_NAME where COLUMN1_TYPE=COLUMN2_TYPE, ..., PRIMARY_KEY=PRIMARY_KEY_COLUMN
  ```
  **Example**:
  ``` 
  make table test where name=str, price=float, quantity=int, PRIMARY_KEY=name
  ```

- **View Table Schema**: Displays the schema of a specified table.
  ```
  schema TABLE_NAME
  ```
  **Example**:
  ```  
  schema test
  ```

- **Delete Table**: Removes an existing table from the database.
  ```
  drop table TABLE_NAME
  ```
  **Example**:
  ``` 
  drop table test
  ```

### Data Manipulation

- **Add Entry**: Inserts a new entry into the specified table.
  ```
  add an entry to TABLE_NAME with COLUMN1=VALUE1, COLUMN2=VALUE2, ...
  ```
  **Example**:
  ``` 
  add an entry to test with name=iphone , price=23223.00 , quantity=23
  add an entry to test with name=computer , price=50000.49 , quantity=10
  add an entry to test with name=mobile , price=10000.00 , quantity=100
  ```

- **Delete Entry**: Deletes an entry from the table based on column value.
  ```
  delete entry with COLUMN=VALUE from TABLE_NAME
  ```
  **Example**:
  ``` 
  delete entry with name=computer from test
  ```

- **Modify Entry**: Updates the values of specific columns in an entry.
  ```
  modify entry with COLUMN=VALUE from TABLE_NAME with COLUMN1=VALUE1, COLUMN2=VALUE2, ...
  ```
  **Example**:
  ```  
  modify entry with name=mobile from test with quantity=200,price=20000.00
  ```

### Data Retrieval

- **Get Entries**: Retrieves all entries from a table.
  ```
  get from TABLE_NAME
  ```
  **Example**:
  ``` 
  get from test
  ```

- **Conditional Retrieval (Where Clause)**: Retrieves entries based on specific conditions.
  ```
  get from TABLE_NAME when COLUMN==VALUE AND/OR COLUMN==VALUE
  ```
  **Example**:
  ``` 
  get row_id,circuitId,location from circuits when country==Italy
  get from races when year>=2010 AND name==Australian_Grand_Prix
  get from races when (year>=2010 AND name==Australian_Grand_Prix) OR (year<2009 AND name==Brazilian_Grand_Prix)
  ```

- **Sorted Retrieval (Order By Clause)**: Retrieves and sorts entries by a specified column.
  ```
  get from TABLE_NAME sorted_by COLUMN ASC/DESC
  ```
  **Example**:
  ```  
  get from constructors when nationality==British sorted_by name DESC
  ```

- **Grouped Retrieval (Group By Clause)**: Retrieves and groups entries by a specified column, often used with aggregate functions.
  ```
  get COLUMN,AGG_FUNCTION(COLUMN_NAME) from TABLE_NAME group_by COLUMN
  ```
  **Example**:
  ``` 
  get nationality,COUNT() from constructors group_by nationality
  get driverId,sum(points) from driver_standings group_by driverId
  get name,min(round) from races group_by round
  ```

### Advanced Data Retrieval

- **Join Tables (Join Clause)**: Retrieves combined data from two tables based on a join condition.
  ```
  get TABLE1.COLUMN, TABLE2.COLUMN from TABLE1 merged_with TABLE2 if TABLE1.KEY==TABLE2.KEY
  ```
  **Example**:
  ``` 
  get races.year, races.round, circuits.name from races merged_with circuits if races.circuitId==circuits.circuitId
  ```

- **Join and Sort (Join with Order By Clause)**: Combines join and sorting operations for retrieved data.
  ```
  get TABLE1.COLUMN, TABLE2.COLUMN from TABLE1 merged_with TABLE2 if TABLE1.KEY==TABLE2.KEY sorted_by TABLE1.COLUMN ASC/DESC
  ```
  **Example**:
  ``` 
  get races.year, races.round, circuits.name from races merged_with circuits if races.circuitId==circuits.circuitId when races.year==2003 sorted_by races.round ASC
  ```