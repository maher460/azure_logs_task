# Azure Logs Task

## Overview

**Azure Logs Task** is a project designed to streamline the process of downloading and processing Azure logs. The project provides scripts to download logs either sequentially or concurrently, and includes a Jupyter notebook for data processing.

## Author

Maher Khan

## Prerequisites

- Python 3.x
- Azure SAS URIs for the logs

## Setup

### Step 1: Clone the Repository

```
git clone https://github.com/yourusername/azure-logs-task.git
cd azure-logs-task
```

### Step 2: Set Up Python Virtual Environment

Create and activate a virtual environment:

```
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

### Step 3: Install Required Packages

Install the required packages using `requirements.txt`:

```
pip install -r requirements.txt
```

### Step 4: Set Environment Variables

Set the environment variables `AUDIT_LOGS_URI` and `SIGNIN_LOGS_URI` with the respective Azure SAS URIs:

```
export AUDIT_LOGS_URI="your_audit_logs_sas_uri"
export SIGNIN_LOGS_URI="your_signin_logs_sas_uri"
```

On Windows, use:

```
set AUDIT_LOGS_URI="your_audit_logs_sas_uri"
set SIGNIN_LOGS_URI="your_signin_logs_sas_uri"
```

## Usage

### Download Logs

You can choose to download the logs sequentially or concurrently.

#### Option 1: Sequential Download

Run `get_data.py` to download and save blobs one by one:

```
python get_data.py
```

#### Option 2: Concurrent Download

Run `get_data_concurrent.py` to download and save blobs asynchronously:

```
python get_data_concurrent.py
```

### Process Data

Use Jupyter Notebook to work with the stored data.

#### Process Data

Use `process_data.ipynb` to work with the stored data. Note that this is still a work in progress and currently very limited.

## Parquet File Processor `process_data.py`

This script processes Parquet files located in specified directories, flattens nested JSON structures, and combines the data into a single DataFrame. It also saves the processed data in both CSV and Parquet formats.

### Features

- Recursively finds Parquet files in a directory
- Validates date formats for filtering
- Flattens nested JSON columns
- Combines and saves processed data

### Run the Script

Execute the script using Python:

```
python script.py --start_date YYYYMMDD --end_date YYYYMMDD
```

If you do not specify `--start_date` or `--end_date`, the default values will be used.

Example:

```
python script.py --start_date 20240601 --end_date 20240801
```

This command will process Parquet files in the specified directories, flatten nested JSON columns, and save the combined data to `combined_table.csv` and `combined_table.parquet`.

## DuckDB Log Analysis

This repository contains a Jupyter notebook for analyzing audit and signin logs using DuckDB. The notebook demonstrates the following steps:

1. Loading Parquet files into DuckDB tables `combined_table_signinlogs` and `combined_table_auditlogs`.
2. Merging audit and signin logs into a single `events` table.
3. Creating a `sessions` table by aggregating the `events` table.
4. Adding detailed explanations and comments throughout the process.
5. Verifying schemas and sample rows of the tables.
6. Exporting results to CSV files for further analysis in Excel or similar applications.

### Running the Notebook

1. Install the required Python packages: [Jupyter Notebook Installation Instructions](https://jupyter.org/install)
   ```
   pip install duckdb pandas jupyter
   ```

2. Launch Jupyter Notebook:
   ```
   jupyter notebook
   ```

3. Open the `DuckDB_Log_Analysis.ipynb` notebook and run the cells step by step.

### Accessing the DuckDB File via CLI

After running the notebook, you can access the `azure_logs.db` DuckDB file using the DuckDB CLI:

1. Install DuckDB CLI by following the [DuckDB installation instructions](https://duckdb.org/docs/installation/).

2. Open your terminal.

3. Run the DuckDB CLI:
   ```
   duckdb azure_logs.db
   ```

4. You can now execute SQL queries against your DuckDB database. For example:
   ```
   .tables
   SELECT * FROM combined_table_signinlogs LIMIT 10;
   SELECT * FROM combined_table_auditlogs LIMIT 10;
   SELECT * FROM events LIMIT 10;
   SELECT * FROM sessions LIMIT 10;
   ```

### Exported CSV Files

The notebook also exports the results to CSV files that can be opened with Excel or similar applications:

- `events.csv`: Contains the merged audit and signin logs.
- `sessions.csv`: Contains the aggregated session data.

You can find these files in the same directory as the notebook.

---
