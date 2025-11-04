# Platypus Package
A utility package for data engineering, analytics, and machine learning tasks.

```
       _       _                         
 _ __ | | __ _| |_ _   _ _ __  _   _ ___ 
| '_ \| |/ _` | __| | | | '_ \| | | / __|
| |_) | | (_| | |_| |_| | |_) | |_| \__ \
| .__/|_|\__,_|\__|\__, | .__/ \__,_|___/
|_|                |___/|_|              

```
```
                                                                                      0000005
                                                                                 00000      5000
                                                                               000          00800
                             0000006 0009                                    400 0        00    00
                              0   8000 000                                  008   00   004       00
                         00000000     0 000000000                          008      000          00
                        00004892  000           60000                      00     90010           0 
                          7000                      000                   00    00     05      00005
                        800                           000                 00  00        00   00   00
                       000                              00                0000            000     00
                      00                                 00              30  00          0 00     00
                     00                                   00             90   00      00     0    00
                     00                                    00            40     00  00        00  00
                    00   00              000               00             0      000            000 
                    00   80              0  00             00             00   00  0          00  0 
                    00   004             09000             00             00 00     00      00   00 
                     0                   000001            00             000         0   00     00 
                     005  000   000        46             00000005         0           009       03 
                    800 00         00           0000     00  7900000000000000        00  0      00  
                    000 07           00      000    0   0                  0000    00     07    00  
               200000                  000000       0006                       9000        00  00   
            00009      001                         00                              00        0000   
         0000                    00         6900000                                  00       00    
      0004                       0       00                                            0      00    
    000                                00                                               01   00     
  000                                 0                                                  0  00      
 00                                  0                                                    0004      
00                                 00                          00                         000       
 0                                00                           00                         00        
 000                            00                             00              0          00        
   0000                       00        162                    0               0          00        
      000000               000    0000000000000000             0               0         00         
           0000000000000000      00              00           05               00        00         
                          00    007               00          00             00000      00          
                     90009 8    00                 00        00000000000000000   002   4000000      
                       0000    00                  00       00                    00          000   
                   0000        00                 002        000                  004       000000  
                      60000   000                 00            00                 000    4  0      
                         000000                   00        200000                   0000000000     
                        70                        00  000    00                        01     6     
                                                   0000 1000000                                     
                                                             0                                      
```

# Platypus Package Overview

The Platypus package is designed to provide common utility functions, scripts, and modules for data engineering, analytics, and machine learning tasks. The package follows a layered structure with a focus on modularity, decoupling, and reusability.

---

## Setup Instructions

### 1. Install the Package from Bitbucket
To install the package directly from the Bitbucket repository, use the following command. This fetches the package via SSH and installs it in "editable" mode.

```bash
pip install git+ssh...
```

### Uninstall the Package
```bash
pip remove platypus
```

---

## Project Structure

The package is organized into several modules, each with a clear responsibility:

## Configuration

- **Output Folder**: The default output folder is `output/` at the root of the user's project. If the output folder does not exists in your project it will create one. You can also specify your own output path which can configured via an environment variable (`OUTPUT_FOLDER`) set in the `.env` file.

- **.env File**: The package uses `python-dotenv` to manage credentials and environment-specific variables. Users store their Dremio connection credentials and other configurations in a `.env` file.

## Documentation

- Comprehensive docstrings are provided for all classes, methods, and functions.
- Examples of how to use components are included in the README and other documentation files.

# Dremio Client Module

This package provides tools for interacting with a Dremio database, executing queries, and saving the results in various formats. The main components of the package are:

1. **DremioDatabase**: Handles authentication and query execution.
2. **DremioQueryExecutor**: Executes queries and saves the results.
3. **DataFetcher**: Fetches data in chunks and cleans it.
4. **DataSaver**: Saves data in different formats.

## Components

### 1. DremioDatabase

The `DremioDatabase` class handles authentication with the Dremio server and provides methods to execute SQL queries.

**Attributes**:
- `username`: The username for authentication.
- `password`: The password for authentication.
- `endpoint`: The Dremio server endpoint.
- `port`: The port for the Dremio server.
- `headers`: A list to store authentication headers.
- `client`: The FlightClient instance for interacting with the Dremio server.

**Methods**:
- `get_client(max_retries: int = 2) -> flight.FlightClient`: Authenticates with the Dremio server and returns a FlightClient instance.
- `execute_query(query: str) -> flight.FlightStreamReader`: Executes a SQL query on the Dremio server and returns a FlightStreamReader to read the results.

---

### 2. DremioQueryExecutor

The `QueryExecutor` class executes queries and either saves the result to the specified format and path or returns the DataFrame.

**Attributes**:
- `db`: An instance of `DremioDatabase`.
- `data_saver`: An instance of `DataSaver`.
- `data_fetcher`: An instance of `DremioDataFetcher`.

**Methods**:
- `execute_and_save(query: str, table_name: str = None, output_format: str = None, output_path: str = None) -> pd.DataFrame`: Executes a query and either saves the result to the specified format and path or returns the DataFrame.

  **Args**:
  - `query (str)`: The SQL query to execute.
  - `table_name (str, optional)`: The name of the table/file. Required if saving to a file.
  - `output_format (str, optional)`: The format to save the data in ('csv', 'parquet', 'duckdb', 'dataframe', 'polars'). Required if saving to a file.
  - `output_path (str, optional)`: The path to save the file. If not provided, it will create an 'output' folder in the root of the project.

  **Returns**:
  - `pd.DataFrame`: The result of the query as a DataFrame.

- `execute_queries_with_configs(queries: list[str], output_configs: list[dict]) -> dict`: Executes multiple queries and saves the results based on the provided configurations.
    **Args**:
  - `queries (list[str])`: List of SQL queries to execute.
  - `output_configs (list[dict])`: List of output configurations. Each configuration should be a dictionary with keys:
    - `output_format`: The format to save the data ('csv', 'parquet', 'duckdb', 'dataframe', 'polars').
    - `output_path`: The path to save the output file.
    - `output_filename`: The name of the output file (without extension).

  **Returns**:
  - `dict`: A dictionary of DataFrames if the output format is 'dataframe' or 'polars'.


---

### 3. DataFetcher

The `DremioDataFetcher` class fetches data in chunks from the Dremio server and cleans it.

**Methods**:
- `fetch_chunk(query: str, chunk_size: int, full_output_path: str)`: Fetches data in chunks and saves it to the specified path.
- `fetch_and_clean_data(query: str, chunk_size: int, full_output_path: str, output_format: str)`: Fetches data, cleans it, and saves it to the specified path.

**Cleaning Process**:
- If the output format is 'csv', it cleans column names and drops columns that contain all null values.
- If the output format is 'polars', it cleans column names and resolves mixed data types.
- For other formats, it drops columns that contain all null values and saves the cleaned data.

---

### 4. DataSaver

The `DataSaver` class saves data in different formats such as CSV, Parquet, DuckDB, and Polars.

**Methods**:
- `save_data(df: pd.DataFrame, table_name: str, output_format: str, output_path: str = None) -> None`: Saves the DataFrame to the specified format and path.

  **Args**:
  - `df (pd.DataFrame)`: The DataFrame to save.
  - `table_name (str)`: The name of the table/file.
  - `output_format (str)`: The format to save the data in ('csv', 'parquet', 'duckdb', 'polars').
  - `output_path (str, optional)`: The path to save the file. If not provided, it will use the default output path.

  **Raises**:
  - `ValueError`: If the output format is unsupported.


---

### Example Usage

Examples of how to use the package.

# Example pandas dataframe

```python
import os
from platypus.dremio_client import DremioQueryExecutor
import pandas as pd

# Create the QueryExecutor
executor = DremioQueryExecutor()

# Define the query
query = "select * from account_output limit 10"

# Execute the query and return polars dataframe.  
# **Pandas dataframe is the default. you can choose to pass 'dataframe' or not. It is your preference**
df_pandas = executor.execute_and_save(query, table_name="account_output", output_format="dataframe")

# Display the DataFrame
df_pandas.head()

```

# Example polars dataframe

```python
import os
from platypus.dremio_client import DremioQueryExecutor
import polars as pl

# Create the QueryExecutor
executor = DremioQueryExecutor()

# Define the query
query = "select * from account_output limit 10"

# Execute the query and return polars dataframe
df_polars = executor.execute_and_save(query, table_name="account_output", output_format="polars")

# Display the DataFrame
df_polars.head()

```

# Example duckdb

```python
import duckdb
import pandas as pd
import os
from platypus.dremio_client import DremioQueryExecutor

# Create the QueryExecutor
executor = DremioQueryExecutor()

# Define the query
query = "select * from account_output limit 10"

# Execute the query and save the result as a DuckDB file
df = executor.execute_and_save(query, table_name='account_output', output_format='duckdb')

# Connect to the DuckDB file and query the table
con = duckdb.connect(f'output/account_output.duckdb')
df_duckdb = con.execute('SELECT * FROM account_output').fetchdf()

# Display the result DataFrame
df_duckdb.head()

```

# Example parquet

```python
import os
from platypus.dremio_client import DremioQueryExecutor
import pandas as pd


# Create the QueryExecutor
executor = DremioQueryExecutor()

# Define the query
query = "select * from account_output limit 10"

# Execute the query and save the result as a Parquet file
executor.execute_and_save(query, table_name='account_output', output_format='parquet')

# Read the Parquet file into a DataFrame
df_parquet = pd.read_parquet('output/account_output.parquet')

# Display the DataFrame
df_parquet.head()
```


# Example csv

```python
import os
from platypus.dremio_client import DremioQueryExecutor
import pandas as pd

# Create the QueryExecutor
executor = DremioQueryExecutor()

# Define the query
query = "select * from account_output limit 10"

# Execute the query and save the result as a CSV file
executor.execute_and_save(query, table_name='account_output', output_format='csv')

# Read the CSV file into a DataFrame
df_csv = pd.read_csv('output/account_output.csv')

# Display the DataFrame
print("Data read from CSV file:")
df_csv.head()

```

# Example Multiple Queries with Configs

```python

import os
import duckdb
import pandas as pd
from platypus.dremio_client import DremioQueryExecutor

# Define the queries
queries = [
    "select * from account_output limit 10",
    "select * from account_output limit 10",
    "select * from account_output limit 10",
    "select * from account_output limit 10",
    "select * from account_output limit 10"
]

# Define Configurations
# Options: output_format, output_filename, output_path  ** output_path default to the output folder at the root. If it does not exists it will be created. 
output_configs = [
    {
        'output_format': 'dataframe',
        'output_filename': 'table1_data'
    },
    {
        'output_format': 'polars',
        'output_filename': 'table2_data'
    },
    {
        'output_format': 'parquet',
        'output_filename': 'table3_data'
    },
    {
        'output_format': 'csv',
        'output_filename': 'table4_data'
    },
    {
        'output_format': 'duckdb',
        'output_filename': 'table5_data'
    }
]

# Create the QueryExecutor
executor = DremioQueryExecutor()

# Execute queries and return DataFrames and Files
dataframes = executor.execute_queries_with_configs(queries, output_configs)


#Read Files into DataFrames
df1 = dataframes['table1_data']
df2 = dataframes['table2_data']
df_polars = pd.read_parquet('output/table3_data.parquet')
df_csv = pd.read_csv('output/table4_data.csv')
con = duckdb.connect(f'output/table5_data.duckdb')
df_duckdb = con.execute('SELECT * FROM table5_data').fetchdf()


## Display the DataFrames
df1.head()
df2.head()
df_polars.head()
df_csv.head()
df_duckdb.head()


```


---

# Salesforce Client Module

This package provides tools for interacting with Salesforce, executing queries, and saving the results in various formats. The main components of the package are:

1. **SalesforceQueryExecutor**: Executes queries and saves the results.


## Components

### 1. SalesforceQueryExecutor

The `SalesforceQueryExecutor` class executes queries and either saves the result to the specified format and path or returns the DataFrame.

**Attributes**:
- `client`: An instance of `SalesforceClient`.
- `data_saver`: An instance of `SalesforceDataSaver`.
- `data_fetcher`: An instance of `SalesforceDataFetcher`.

**Methods**:
- `execute_and_save(query: str, output_filename: str, output_format: str = None, output_path: str = None) -> pd.DataFrame`: Executes a query and either saves the result to the specified format and path or returns the DataFrame.

  **Args**:
  - `query (str)`: The SOQL query to execute.
  - `output_filename (str)`: The name of the output file.
  - `output_format (str, optional)`: The format to save the data in ('csv', 'parquet', 'duckdb', 'dataframe', 'polars'). Defaults to 'dataframe'.
  - `output_path (str, optional)`: The path to save the file. If not provided, it will create an 'output' folder in the root of the project.

  **Returns**:
  - `pd.DataFrame`: The result of the query as a DataFrame.

- `execute_queries_with_configs(queries: list[str], output_configs: list[dict]) -> dict`: Executes multiple queries and saves the results based on the provided configurations.

  **Args**:
  - `queries (list[str])`: List of SOQL queries to execute.
  - `output_configs (list[dict])`: List of output configurations. Each configuration should be a dictionary with keys:
    - `output_format`: The format to save the data ('csv', 'parquet', 'duckdb', 'dataframe', 'polars').
    - `output_path`: The path to save the output file.
    - `output_filename`: The name of the output file (without extension).

  **Returns**:
  - `dict`: A dictionary of DataFrames if the output format is 'dataframe' or 'polars'.

---


### Example Usage 

Examples of how to use methods in this package. 

# Example Parquet File

```python

import pandas as pd
from platypus.salesforce_client import SalesforceQueryExecutor

# Define the query
query = """ SELECT Id FROM Account limit 10 """

# Create the QueryExecutor
executor = SalesforceQueryExecutor()

# Execute the query and save the result as a Parquet file in the 'output' directory
executor.execute_and_save(query, output_format='parquet', output_filename='account')

# Read the Parquet file into a DataFrame
df_read = pd.read_parquet('output/account.parquet')

# Display the DataFrame
df_read.head()

```


# Example CSV File

```python
import pandas as pd
from platypus.salesforce_client import SalesforceQueryExecutor

# Define the query
query = """ SELECT Id FROM Account limit 10 """

# Create the QueryExecutor
executor = SalesforceQueryExecutor()

# Execute the query and save the result as a CSV file in the 'output' directory
executor.execute_and_save(query, output_format='csv', output_filename='Account')

# Read the CSV file into a DataFrame
df_read = pd.read_csv('output/account.csv')

# Display the DataFrame
df_read.head()


```

# Example duckdb

```python
import duckdb
from platypus.salesforce_client import SalesforceQueryExecutor

# Define the query
query = """ SELECT Id FROM Account limit 10 """

# Create the QueryExecutor
executor = SalesforceQueryExecutor()

# Execute the query and save the result as a DuckDB file
executor.execute_and_save(query, output_format='duckdb', output_filename='account')

# Connect to the DuckDB file and query the table
con = duckdb.connect(f'output/account.duckdb')
df_duckdb = con.execute('SELECT * FROM account').fetchdf()

# Display the result DataFrame
df_duckdb.head()

```

# Example Multiple Queries with Configs

```python

import pandas as pd
import duckdb
from platypus.salesforce_client import SalesforceQueryExecutor
from soql_queries import *

# Note: Create a .py file in the root directory named soql_queries and assign your queries to a variable.
# Make sure to import the soql_queries file



queries = [
    account_soql,
    account_soql,
    account_soql,
    account_soql,
    account_soql
]

output_configs = [
    {
        'output_format': 'dataframe',
        'output_filename': 'sf_table1_data'
    },
    {
        'output_format': 'polars',
        'output_filename': 'sf_table2_data'
    },
    {
        'output_format': 'parquet',
        'output_filename': 'sf_table3_data'
    },
    {
        'output_format': 'csv',
        'output_filename': 'sf_table4_data'
    },
    {
        'output_format': 'duckdb',
        'output_filename': 'sf_table5_data'
    }
]

executor = SalesforceQueryExecutor()
dataframes = executor.execute_queries_with_configs(queries, output_configs)

df1 = dataframes['sf_table1_data']
df2 = dataframes['sf_table2_data']

df_parquet = pd.read_parquet('output/sf_table3_data.parquet')

df_csv = pd.read_csv('output/sf_table4_data.csv')

con = duckdb.connect(f'output/sf_table5_data.duckdb')
df_duckdb = con.execute('SELECT * FROM sf_table5_data').fetchdf()

df1.head()
df2.head()
df_parquet.head()
df_csv.head()
df_duckdb.head()


```

# Example Polars DataFrame

```python

import polars as pl
from platypus.salesforce_client import SalesforceQueryExecutor

# Create the QueryExecutor
executor = SalesforceQueryExecutor()

# Define the query
query = """ SELECT Id FROM Account limit 10 """

# Execute the query and return polars dataframe
df_polars = executor.execute_and_save(query, output_format='polars', output_filename='account')

# Display the DataFrame
df_polars.head()


```

# Example Pandas DataFrame

```python

import pandas as pd
from platypus.salesforce_client import SalesforceQueryExecutor

# Create the QueryExecutor
executor = SalesforceQueryExecutor()

# Define the query
query = """ SELECT Id FROM Account limit 10 """

# Execute the query and return polars dataframe
df_pandas = executor.execute_and_save(query, output_format='dataframe', output_filename='account')

# Display the DataFrame
df_pandas.head()

```

# Utility Modules

Contains common helper functions and utilities.

1. **data_cleaning**: Contains common data cleaning functions.
2. **file_utils**: Contains common functions to handle file processing and manipulation.
3. **hyper_file_creator**: Creates hyper file for Tableau.


# Creating Tableau Hyper File From CSV

```python


```

---

## Technical Considerations

- **Avoid Tight Coupling**: The Strategy Pattern is used to decouple data retrieval from the format in which it's saved.
- **Error Handling**: Strong error handling and logging practices are implemented throughout the package.
- **Avoid Global State**: No global variables or states are used to ensure predictable behavior.
- **Pin Dependencies**: Dependencies are pinned to specific versions to ensure compatibility.
- **Extensibility**: Designed to be extended with new functionalities without major refactoring.
- **Dependency Injection**: Used to avoid tight coupling between modules.
- **Performance Optimization**: Data processing functions are optimized for performance.
- **Testing**: Test key functions and integration for external dependencies.

---

## Adding New Features

When adding new features to the package, ensure that you follow these guidelines:

- **Encapsulation**: Encapsulate related data and behaviors into well-defined classes. Avoid global variables and ensure that classes have a single responsibility.
- **Modularity**: Write modular code where each class and function has a clear, singular purpose. Break down large functions into smaller, more manageable pieces.
- **Reusability**: Design classes and functions to be reusable and extendable. Avoid hardcoding values; use parameters and configuration files.
- **Design Patterns**: Favor using well-known design patterns to ensure code maintainability and scalability. Prefer a design pattern that fits with your teams skills and knowledge. Consider those who come after you.
- **Idiomatic Python**: Prefer Pythonic constructs like list comprehensions, generator expressions, and context managers over verbose loops and manual resource management.
- **OOP Principles**: Always prioritize object-oriented principles. Use classes and objects to encapsulate data and functionality.
- **Exception Handling**: Always use try-except blocks for error-prone code. Define custom exceptions when necessary and ensure meaningful error messages are provided.
- **Type Annotations**: All function and method signatures must include type hints for both parameters and return types.


---

## Config Files

- **MANIFEST.in**: Specifies additional files to include in the source distribution.

- **pyproject.toml**: Defines build system requirements and configurations.

- **requirements-dev.txt**: Lists development and testing dependencies.

- **requirements.txt**: Lists runtime dependencies.

- **setup.cfg**: Configuration file for setuptools with package metadata and options.

- **setup.py**: Script for setuptools with package metadata and options.

- **tox.ini**: Configuration file for tox to automate testing in multiple environments.




