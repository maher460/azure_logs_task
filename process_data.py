import pandas as pd
import numpy as np
import os

signinlogs_dir = "./parquet_data/insights-logs-signinlogs"
auditlogs_dir = "./parquet_data/insights-logs-auditlogs"

import re
from datetime import datetime

def is_valid_date_format(date_str):
    pattern = r'^\d{8}$'
    if not re.match(pattern, date_str):
        return False
    
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False

# Function to recursively find all Parquet files in a directory
def find_parquet_files(directory, start_date=None, end_date=None):
    print(f"Searching for Parquet files in directory: {directory}")
    parquet_files = []
    
    if start_date and not is_valid_date_format(start_date):
        print("start_date format is not correct. It should be 'YYYYMMDD'")
        return parquet_files
    
    if end_date and not is_valid_date_format(end_date):
        print("start_date format is not correct. It should be 'YYYYMMDD")
        return parquet_files
    
    
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".parquet"):
                d = os.path.basename(root)
                if(is_valid_date_format(d)):
                    try:
                        date = int(d)
                        # Filter blobs based on the date range
                        if (((not start_date) or (start_date and date >= int(start_date)))
                            and ((not end_date) or (end_date and date <= int(end_date)))):
                                parquet_files.append(os.path.join(root, file))
                    except ValueError:
                        print(f"Skipping directory '{root}' as its name is not a valid date")
                
    print(f"Found {len(parquet_files)} Parquet files in directory: {directory}")      
    return parquet_files


def flatten_json(df, column):
    """Flatten the JSON data in the specified column."""
    print(f"Flattening JSON column: {column}")
    if df[column].apply(lambda x: isinstance(x, (np.ndarray,list))).any():
        df = df.explode(column, ignore_index=True)
    
    df_flat = pd.DataFrame()
    
    for _, row in df.iterrows():
        json_data = row[column]
        
        if isinstance(json_data, dict):
            json_data = pd.json_normalize(json_data)
        elif isinstance(json_data, (np.ndarray,list)) and json_data and isinstance(json_data[0], dict):
            json_data = pd.json_normalize(json_data)
        else:
            continue
        
        # Rename columns to include the original column name as a prefix
        json_data.columns = [f"{column}.{col}" for col in json_data.columns]
        
        # Concatenate the normalized data
        df_flat = pd.concat([df_flat, json_data], ignore_index=True)
    
    return pd.concat([df.drop(columns=[column]), df_flat], axis=1)

def recursively_flatten(df, depth=None, current_depth=0):
    """Recursively flatten all list and dictionary columns in the DataFrame with depth control."""
    print("Starting recursive flattening of the DataFrame")
    while True:
        # Identify list or dictionary columns
        complex_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (np.ndarray, list, dict))).any()]
        
        # Check if there are no more complex columns to flatten
        if not complex_columns:
            break
        
        for col in complex_columns:
            df = flatten_json(df, col)
        
        # Increment the current depth
        current_depth += 1
        
        # Stop if the maximum depth is reached
        if depth is not None and current_depth >= depth:
            break
        
        nested_columns = [col for col in df.columns if any(df[col].apply(lambda x: isinstance(x, (np.ndarray,list))))]
        
        if not nested_columns:
            break
    
    print("Finished recursive flattening of the DataFrame")
    return df

def load_parquet_files_to_df(parquet_dir, start_date=None, end_date=None):
    print(f"Loading Parquet files from directory: {parquet_dir}")
    file_list = find_parquet_files(parquet_dir, start_date, end_date)
    
    if not file_list:
        print(f"No Parquet files found in the directory '{parquet_dir}' for the given date range.")
        return pd.DataFrame()
    
    # Read each Parquet file and concatenate them into a single DataFrame
    df = pd.concat([pd.read_parquet(file) for file in file_list], ignore_index=True)
    print(f"Loaded {len(file_list)} Parquet files into DataFrame from directory: {parquet_dir}")
    
    return df

def save_combined_table_to_disk(parquet_dir, df):
    print(f"Saving combined table to disk at directory: {parquet_dir}")
    df.to_csv(os.path.join(parquet_dir, "combined_table.csv"))
    df.to_parquet(os.path.join(parquet_dir, "combined_table.parquet"))
    print("Combined table saved successfully")

def combine_parquet_files(parquet_dir, start_date=None, end_date=None):
    df = load_parquet_files_to_df(parquet_dir, start_date, end_date)
    if not df.empty:
        df = recursively_flatten(df)
        save_combined_table_to_disk(parquet_dir, df)
    else:
        print("No data to combine")

def main():
    combine_parquet_files(signinlogs_dir, start_date="20240601", end_date="20240801")
    combine_parquet_files(auditlogs_dir,  start_date="20240601", end_date="20240801")
    
if __name__ == "__main__":
    main()