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

---
