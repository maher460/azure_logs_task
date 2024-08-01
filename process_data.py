import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
import argparse
import logging

# Directories containing Parquet files
signinlogs_dir = "./parquet_data/insights-logs-signinlogs"
auditlogs_dir = "./parquet_data/insights-logs-auditlogs"

# Set up logging to file
os.makedirs("app_logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join("app_logs","process_data.log")),
        logging.StreamHandler()
    ]
)

def is_valid_date_format(date_str):
    """
    Check if the provided date string is in the format 'YYYYMMDD'.
    
    Args:
    date_str (str): The date string to validate.
    
    Returns:
    bool: True if the date string is in the correct format and a valid date, False otherwise.
    """
    # Define regex pattern for 'YYYYMMDD' format
    pattern = r'^\d{8}$'
    
    # Check if the date string matches the pattern
    if not re.match(pattern, date_str):
        return False
    
    # Check if the date string represents a valid date
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False

def find_parquet_files(directory, start_date=None, end_date=None):
    """
    Recursively find all Parquet files in the specified directory and filter by date range.
    
    Args:
    directory (str): The directory to search for Parquet files.
    start_date (str, optional): The start date in 'YYYYMMDD' format to filter files.
    end_date (str, optional): The end date in 'YYYYMMDD' format to filter files.
    
    Returns:
    list of str: List of file paths to Parquet files that match the date range criteria.
    """
    logging.info(f"Searching for Parquet files in directory: {directory}")
    parquet_files = []
    
    # Validate start and end date formats
    if start_date and not is_valid_date_format(start_date):
        logging.error("start_date format is not correct. It should be 'YYYYMMDD'")
        return parquet_files
    
    if end_date and not is_valid_date_format(end_date):
        logging.error("start_date format is not correct. It should be 'YYYYMMDD")
        return parquet_files
    
    # Walk through the directory tree
    for root, _, files in os.walk(directory):
        for file in files:
            # Check if file has a '.parquet' extension
            if file.endswith(".parquet"):
                d = os.path.basename(root)
                
                # Check if the directory name is a valid date
                if(is_valid_date_format(d)):
                    try:
                        date = int(d)
                        # Filter blobs based on the date range
                        if (((not start_date) or (start_date and date >= int(start_date)))
                            and ((not end_date) or (end_date and date <= int(end_date)))):
                                parquet_files.append(os.path.join(root, file))
                    except ValueError:
                        logging.warning(f"Skipping directory '{root}' as its name is not a valid date")
                
    logging.info(f"Found {len(parquet_files)} Parquet files in directory: {directory}")      
    return parquet_files

def flatten_json(df, column):
    """
    Flatten JSON data in the specified column of a DataFrame.
    
    Args:
    df (pd.DataFrame): The DataFrame containing JSON data.
    column (str): The name of the column containing JSON data to be flattened.
    
    Returns:
    pd.DataFrame: A DataFrame with the specified column flattened.
    """
    # Explode the JSON column into separate rows
    df_exploded = df.explode(column, ignore_index=True)
    df_flat = pd.DataFrame()

    # Process each row in the exploded DataFrame
    for _, row in df_exploded.iterrows():
        json_data = row[column]
        
        # Check if the JSON data is a dictionary
        if isinstance(json_data, dict):
            json_data = pd.json_normalize(json_data)
            json_data.columns = [f"{column}.{col}" for col in json_data.columns]
        # Check if the JSON data is a list of dictionaries
        elif isinstance(json_data, (list, np.ndarray)) and json_data and isinstance(json_data[0], dict):
            json_data = pd.json_normalize(json_data)
            json_data.columns = [f"{column}.{col}" for col in json_data.columns]
        else:
            # Handle non-dictionary and non-list JSON data
            json_data = pd.DataFrame({column: [json_data]})

        # Concatenate the flattened data
        df_flat = pd.concat([df_flat, json_data], ignore_index=True)

    # Combine the original DataFrame with the flattened data
    return pd.concat([df_exploded.drop(columns=[column]), df_flat], axis=1)

def recursively_flatten(df, depth=None, current_depth=0):
    """
    Recursively flatten all list columns in the DataFrame, with optional depth control.
    
    Args:
    df (pd.DataFrame): The DataFrame to be flattened.
    depth (int, optional): The maximum depth of recursion. If None, will flatten all levels.
    current_depth (int, optional): The current recursion depth. Default is 0.
    
    Returns:
    pd.DataFrame: The DataFrame with all list columns flattened up to the specified depth.
    """
    # Identify list columns in the DataFrame
    list_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (list, np.ndarray))).any()]

    # Flatten each list column
    for col in list_columns:
        df = flatten_json(df, col)
    
    # Recursively flatten if required and depth allows
    if depth is None or current_depth < depth:
        new_list_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (list, np.ndarray))).any()]
        if new_list_columns:
            df = recursively_flatten(df, depth, current_depth + 1)

    return df


def load_parquet_files_to_df(parquet_dir, start_date=None, end_date=None):
    """
    Load Parquet files from a directory into a single DataFrame.
    
    Args:
    parquet_dir (str): The directory containing the Parquet files.
    start_date (str, optional): The start date to filter files (in 'YYYYMMDD' format).
    end_date (str, optional): The end date to filter files (in 'YYYYMMDD' format).
    
    Returns:
    pd.DataFrame: A DataFrame containing the data from all loaded Parquet files.
    """
    logging.info(f"Loading Parquet files from directory: {parquet_dir}")
    
    # Find Parquet files in the directory
    file_list = find_parquet_files(parquet_dir, start_date, end_date)
    
    # Check if any files were found
    if not file_list:
        logging.error(f"No Parquet files found in the directory '{parquet_dir}' for the given date range.")
        return pd.DataFrame()
    
    # Read each Parquet file and concatenate them into a single DataFrame
    df = pd.concat([pd.read_parquet(file) for file in file_list], ignore_index=True)
    logging.info(f"Loaded {len(file_list)} Parquet files into DataFrame from directory: {parquet_dir}")
    
    return df

def save_combined_table_to_disk(parquet_dir, df):
    """
    Save the combined DataFrame to disk in both CSV and Parquet formats.
    
    Args:
    parquet_dir (str): The directory to save the files.
    df (pd.DataFrame): The DataFrame to save.
    
    Returns:
    None
    """
    logging.info(f"Saving combined table to disk at directory: {parquet_dir}")
    # Save DataFrame to CSV format
    df.to_csv(os.path.join(parquet_dir, "combined_table.csv"))
    # Save DataFrame to Parquet format
    df.to_parquet(os.path.join(parquet_dir, "combined_table.parquet"))
    logging.info("Combined table saved successfully")

def combine_parquet_files(parquet_dir, start_date=None, end_date=None):
    """
    Combine Parquet files from the directory into a single flattened DataFrame and save it.
    
    Args:
    parquet_dir (str): The directory containing Parquet files.
    start_date (str, optional): The start date to filter files (in 'YYYYMMDD' format).
    end_date (str, optional): The end date to filter files (in 'YYYYMMDD' format).
    
    Returns:
    None
    """
    # Load Parquet files into a DataFrame
    df = load_parquet_files_to_df(parquet_dir, start_date, end_date)
    
    if not df.empty:
        # Recursively flatten the DataFrame
        df = recursively_flatten(df)
        # Save the combined and flattened DataFrame to disk
        save_combined_table_to_disk(parquet_dir, df)
    else:
        logging.warning("No data to combine")

def log_runtime(func):
    """
    Decorator to log the start and end time of a function run.
    
    Args:
    func (function): The function to be decorated.
    
    Returns:
    function: The wrapped function with added logging.
    """
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        logging.info(f"Program started at {start_time}")
        
        result = func(*args, **kwargs)
        
        end_time = datetime.now()
        logging.info(f"Program ended at {end_time}")
        logging.info(f"Total run time: {end_time - start_time}")
        
        return result
    return wrapper

@log_runtime
def main():
    """
    Main function to combine Parquet files from predefined directories with specific date ranges.
    
    Returns:
    None
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Combine Parquet files from directories.")
    
    # Add arguments for start and end dates
    parser.add_argument('--start_date', type=str, default="20240601", 
                        help="Start date in 'YYYYMMDD' format (default: '20240601').")
    parser.add_argument('--end_date', type=str, default="20240801", 
                        help="End date in 'YYYYMMDD' format (default: '20240801').")
    
    # Parse the arguments
    args = parser.parse_args()
    
    # Process signin logs and audit logs with user-provided or default date range
    combine_parquet_files(signinlogs_dir, start_date=args.start_date, end_date=args.end_date)
    combine_parquet_files(auditlogs_dir, start_date=args.start_date, end_date=args.end_date)
    
if __name__ == "__main__":
    main()