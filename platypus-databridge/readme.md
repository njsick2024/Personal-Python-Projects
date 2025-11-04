# ETL Process

This ETL process is designed to pull core data tables and objects from Dremio and Salesforce. The process leverages the custom-built **Platypus** package for executing queries against both Dremio and Salesforce.

The extracted data is transformed and saved into a specified format (e.g., Parquet) for further downstream processing and analytics tasks.

Each code block in main.py is commented and what is does and what to run to kick off the process.

You may also execute this from the command line. Scroll to the end of main.py to comment or uncomment this code. 

## 📚 Table of Contents
1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [Output Format](#output-format)
6. [Query Runner CLI](#query-runner-cli)
7. [Contributing](#contributing)
8. [Frequently Asked Questions](#frequently-asked-questions)


## 📦 Prerequisites
Before running the ETL process, ensure that the following are installed and configured:
- Python 3.10+
- Platypus package (see below)
- Dremio and Salesforce access credentials

## 🔧 Installation
1. Clone the repository:
   ```bash
   git clone ,,...
   ```
2. Install the **Platypus** package into your virtual environment 
   pip install git+ssh://...
## ⚙️ Configuration
The ETL script relies on environment variables for access to Dremio and Salesforce. Make sure you have the following environment variables set up:
- `DREMIO_USERNAME` – Your Dremio username
- `DREMIO_PASSWORD` – Your Dremio password
- `SALESFORCE_USERNAME` – Your Salesforce username
- `SALESFORCE_PASSWORD` – Your Salesforce password
- `SALESFORCE_TOKEN` – Your Salesforce security token (if applicable)

You can set these in a `.env` file, or export them directly in your environment:
```bash
export DREMIO_ENDPOINT = 'your_dremio_endpoint'
export DREMIO_PASSWORD='your_dremio_password'
export SALESFORCE_USERNAME='your_salesforce_username'
export SALESFORCE_PASSWORD='your_salesforce_password'
export SALESFORCE_TOKEN='your_salesforce_token'
```

## 🚀 Usage
To execute the ETL process, simply run the main Python script. The process will automatically query Dremio and Salesforce based on predefined queries, transform the data, and save the output in the desired format.

## 📄 Output Format
All data extracted from Dremio and Salesforce is stored in Parquet format by default, but other formats (e.g., CSV) can be configured in the output settings.

## 🧪 Query Runner CLI

A command-line interface for managing and executing SQL queries based on predefined schedules or ad hoc needs.

## 🧰 Query Runner CLI Features

- List available SQL queries
- View recent query logs
- Run all queries based on their schedule
- Run one or multiple queries manually
- Force execution regardless of schedule

## 🖥️ Query Runner CLI Usage
 
```bash
python main.py [OPTIONS]
```

### ⚙️ Options

| Option       | Description                                |
|--------------|--------------------------------------------|
| `--list`     | List all available queries                 |
| `--show-log` | Show the last 20 log entries               |
| `--run-all`  | Run all queries according to schedule      |
| `--run-one`  | Run a single query ad hoc                  |
| `--run-many` | Run multiple queries ad hoc                |
| `--force-run`| Force query(s) to run                      |
|--------------|--------------------------------------------|

### 📘 Examples

```bash
# List all queries
python test_new.py --list

# Show the last 20 log entries
python test_new.py --show-log

# Run all queries according to their schedule
python test_new.py --run-all

# Run a single query now, even if it already ran today
python test_new.py --run-one account_summary.sql --force-run

# Run multiple queries ad hoc
python test_new.py --run-many contact.sql event.sql

# Run multiple queries and force them now
python test_new.py --run-many contact.sql event.sql --force-run
```



## ❓ Frequently Asked Questions
1. What happens if the ETL process fails during execution?
Make note of the error and debug. Generally, most errros have occured due to Dremio being unavailable. 
2. How do I add new queries to the ETL process?
Simply add the .sql files containing the queries to the sql folder. The script automatically reads all .sql files in that folder and processes them.
3. How can I run only certain queries ad-hoc?
Use run_single_query() or run_multiple_queries()


# 📁 Parquet File Comparison and Move Utility

### 🧾 What It Does:
- Compares new parquet files to existing core files by name and size
- Moves files that are more than 1% smaller than the existing version into a "review" folder
- Overwrites core files when the new version is the same size or larger
- Skips and reports errors, prints summary of operations

### 🚀 How to Use:
1. Make sure files are in the `./output` folder
2. Update the `core_files_dir` if needed
3. Run the script: `python parquet_file_mover.py`
4. *Note: This is to run a one off move. Otherwise this is built into main.py

### 🧰 Features

- **Moves new parquet files:** into either the core folder or the review folder
- **Auto open Explorer:** If any file is moved to the review folder, both the core and review folders are opened automatically

### 🧪 Rules

- **Small file rule**: Files below `min_size_bytes` go to the review folder
- **Ratio rule**: If the new file size is below `size_threshold` percent of the core file size, it goes to the review folder

### ⚙️ Per-File Overrides

- `ignore_min_size`: Skip the small file rule
- `ignore_ratio`: Skip the ratio rule
- `force_core`: Always send the file to the core folder
- `force_review`: Always send the file to the review folder


### ⚙️ Options:
- Set `dry_run=True` to simulate
- Change `size_threshold=0.99` to adjust sensitivity
- Logs print directly to the console

No extra dependencies are required. No backups are created. Files are moved in-place.

### ✅ Key Features

- ✅ Compares file sizes to detect regressions
- ✅ Configurable threshold (e.g. 99% of original size)
- ✅ Dry-run mode supported
- ✅ Designed for parquet files
- ✅ Cross-platform compatibility (resolves paths cleanly)

#### 📂 Directory Parameters

| Name            | Description                                      |
|-----------------|--------------------------------------------------|
| `new_files_dir` | Path to folder containing newly generated files  |
| `core_files_dir`| Path to folder containing canonical versions     |
| `review_dir`    | Path to store suspiciously smaller files         |


