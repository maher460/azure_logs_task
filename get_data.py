from azure.storage.blob import ContainerClient
import pandas as pd
import json
import re
import os
from time import sleep
import logging
from datetime import datetime

# Directory to save parquet data
parquet_dir = "./parquet_data"

# SAS URIs for audit logs and sign-in logs
audit_logs_uri = os.getenv('AUDIT_LOGS_URI')
signin_logs_uri = os.getenv('SIGNIN_LOGS_URI')

# Set the logging level for Azure SDK to WARNING
azure_logger = logging.getLogger('azure')
azure_logger.setLevel(logging.WARNING)

# Set up logging to file
os.makedirs("app_logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join("app_logs","get_data.log")),
        logging.StreamHandler()
    ]
)

def get_container_name_from_uri(uri):
    """
    Extracts the container name from the given Azure Blob Storage URI using regex.
    
    Args:
    uri (str): The Azure Blob Storage URI.
    
    Returns:
    str: The container name extracted from the URI.
    """
    # Define the regex pattern to match the container name
    pattern = r"https?://[^/]+/([^/?]+)"
    
    # Use re.search to find the pattern in the input string
    match = re.search(pattern, uri)
    
    # If a match is found, return the container name
    if match:
        return match.group(1)
    else:
        return None

def extract_date_from_name(name):
    """
    Extracts the date from the blob name using a regex pattern.
    
    Args:
    name (str): The name of the blob.
    
    Returns:
    str: The extracted date in the format 'yyyymmdd'. Returns an empty string if no match is found.
    """
    res_date = ""
    # Define the regex pattern to capture the year, month, and day
    pattern = r'y=(\d{4})/m=(\d{2})/d=(\d{2})'

    # Use re.search to find the pattern in the input string
    match = re.search(pattern, name)

    # If a match is found, extract the year, month, and day
    if match:
        year = match.group(1)
        month = match.group(2)
        day = match.group(3)
        res_date = year + month + day
    else:
        logging.warning(f"No date match found in blob name: {name}")
    
    return res_date

def save_logs_to_disk(base_dir, date, df_logs):
    """
    Saves logs DataFrame to CSV and Parquet files.
    
    Args:
    base_dir (str): The base directory where Parquet and CSV files are stored.
    date (str): The date corresponding to the logs.
    df_logs (DataFrame): The DataFrame containing logs.
    """
    try:
        csv_path = os.path.join(base_dir, date, "data.csv")
        parquet_path = os.path.join(base_dir, date, "data.parquet")
        df_logs.to_csv(csv_path, index=False)
        df_logs.to_parquet(parquet_path, index=False)
        # logging.info(f"Saved logs for date {date} to disk")
    except Exception as e:
        logging.error(f"Error saving logs to disk for date {date}: {e}")


def load_or_create_dataframe(base_dir, date):
    """
    Load existing Parquet or CSV file if it exists, otherwise create a new DataFrame.
    
    Args:
    base_dir (str): The base directory where Parquet and CSV files are stored.
    date (str): The specific date for which to load or create the DataFrame.
    
    Returns:
    DataFrame: The loaded or newly created DataFrame.
    """
    parquet_new_date_dir = os.path.join(base_dir, date)

    try:
        # Check if the directory exists and is a directory
        if os.path.exists(parquet_new_date_dir) and os.path.isdir(parquet_new_date_dir):
            # Check for Parquet file first
            if os.path.exists(os.path.join(parquet_new_date_dir, "data.parquet")):
                df_logs = pd.read_parquet(os.path.join(parquet_new_date_dir, "data.parquet"))
            # If no Parquet file, check for CSV file
            elif os.path.exists(os.path.join(parquet_new_date_dir, "data.csv")):
                df_logs = pd.read_csv(os.path.join(parquet_new_date_dir, "data.csv"))
            # If neither Parquet nor CSV file exists, create a new DataFrame
            else:
                df_logs = pd.DataFrame()
        else:
            # Create the directory if it doesn't exist
            os.makedirs(parquet_new_date_dir, exist_ok=True)
            # Create a new DataFrame
            df_logs = pd.DataFrame()
            
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        df_logs = pd.DataFrame()

    return df_logs
    
def download_save_json_logs(container_uri, start_date=None, end_date=None, max_retries=3):
    """
    Downloads JSON logs from Azure Blob Storage, processes them, and saves them as CSV and Parquet files.
    
    Args:
    container_uri (str): The SAS URI of the Azure Blob Storage container.
    start_date (str, optional): The start date in the format 'yyyymmdd'. Defaults to None.
    end_date (str, optional): The end date in the format 'yyyymmdd'. Defaults to None.
    max_retries (int, optional): The maximum number of retries for transient errors. Defaults to 3.
    """
    
    container_name = get_container_name_from_uri(container_uri)
    cur_date = ""
    df_logs = None
    done = 0
        
    try:
        # Connect to the container using the container URI
        container = ContainerClient.from_container_url(container_uri)
        
        # List all blobs in the container
        blob_list = container.list_blobs()
        
        # Iterate over the blobs
        for blob in blob_list:
            date = extract_date_from_name(blob.name)
            
            # Filter blobs based on the date range
            if (((not start_date) or (start_date and int(date) >= int(start_date)))
                and ((not end_date) or (end_date and int(date) <= int(end_date)))):
                
                # If the date changes, save the current DataFrame to CSV and Parquet
                if cur_date != date:
                    
                    if cur_date and df_logs is not None:
                        save_logs_to_disk(os.path.join(parquet_dir, container_name), cur_date, df_logs)
                    
                    # Load existing Parquet or CSV if it exists, otherwise create a new DataFrame
                    df_logs = load_or_create_dataframe(os.path.join(parquet_dir, container_name), date)
                        
                    cur_date = date

                # Download and process the blob data
                for attempt in range(max_retries):
                    try:
                        blob = container.get_blob_client(blob.name)
                        blob_data = blob.download_blob().readall().decode('utf-8')
                        break # If successful, break out of the retry loop
                    except Exception as e:
                        logging.error(f"Error downloading blob {blob.name}: {e}")
                        if attempt < max_retries - 1:
                            sleep(2**attempt) # Exponential backoff
                        else:
                            raise
                
                for blob_json in blob_data.splitlines():
                    json_data = json.loads(blob_json)
                    df_data = pd.json_normalize(json_data)
                    df_logs = pd.concat([df_logs, df_data], ignore_index=True)

                done += 1
                if done % 50 == 0:
                    logging.info(f"Processed: {done} blobs")

        # Save the last DataFrame to CSV and Parquet
        if cur_date and df_logs is not None:
            save_logs_to_disk(os.path.join(parquet_dir, container_name), cur_date, df_logs)
        
        logging.info(f"Processed: {done} blobs")
            
    except Exception as e:
        logging.error(f"Error fetching JSON logs using container URI: {e}")

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
    # Download and save sign-in logs within the specified date range
    download_save_json_logs(signin_logs_uri, start_date="20240618", end_date="20240801")
    download_save_json_logs(audit_logs_uri, start_date="20240618", end_date="20240801")

if __name__ == "__main__":
    # Run the main function
    main()
