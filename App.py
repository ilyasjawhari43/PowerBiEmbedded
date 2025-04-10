import os
import uuid
import pandas as pd
import requests
import json
from dotenv import load_dotenv
import time
from datetime import datetime

load_dotenv()

CSV_DIRECTORY = "Data"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
WORKSPACE_ID = os.getenv("WORKSPACE_ID")
REPORT_ID = os.getenv("REPORT_ID")
NEW_REPORT_NAME = f"Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# Base URL for Power BI API
POWER_BI_API_URL = "https://api.powerbi.com/v1.0/myorg"

def get_access_token():
    """Get an access token for the Power BI API"""
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "resource": "https://analysis.windows.net/powerbi/api"
    }
    
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        print(f"Error getting access token: {response.status_code}")
        print(response.text)
        return None

# Dataset
def read_csv_files(directory):
    """Read all CSV files in a directory and return a dictionary of DataFrames"""
    dataframes = {}
    
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist")
        return dataframes
    
    for file in os.listdir(directory):
        if file.endswith('.csv'):
            file_path = os.path.join(directory, file)
            table_name = os.path.splitext(file)[0]
            try:
                df = pd.read_csv(file_path)
                dataframes[table_name] = df
            except Exception as e:
                print(f"Error reading file {file}: {str(e)}")
    
    return dataframes

def convert_df_to_table_schema(df, table_name):
    """Convert DataFrame schema to Power BI table schema"""
    columns = []
    
    for col_name, dtype in df.dtypes.items():
        data_type = "String"
        
        # Map pandas dtypes to Power BI data types
        if pd.api.types.is_integer_dtype(dtype):
            data_type = "Int64"
        elif pd.api.types.is_float_dtype(dtype):
            data_type = "Double"
        elif pd.api.types.is_bool_dtype(dtype):
            data_type = "Boolean"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            data_type = "DateTime"
        
        columns.append({
            "name": col_name,
            "dataType": data_type
        })
    
    return {
        "name": table_name,
        "columns": columns
    }

def create_dataset(access_token, dataset_name, tables):
    """Create a dataset in Power BI"""
    url = f"{POWER_BI_API_URL}/groups/{WORKSPACE_ID}/datasets"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Create dataset schema
    dataset_schema = {
        "name": dataset_name,
        "defaultMode": "Push",
        "tables": tables
    }
    
    response = requests.post(url, headers=headers, json=dataset_schema)
    if response.status_code == 201 or response.status_code == 202:
        return response.json()["id"]
    else:
        print(f"Error creating dataset: {response.status_code}")
        print(response.text)
        return None

def convert_df_to_rows(df):
    """Convert DataFrame to list of dictionaries for Power BI API"""
    # Handle datetime objects by converting to ISO format strings
    df_copy = df.copy()
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    
    # Convert NaN to None for JSON serialization
    return df_copy.where(pd.notnull(df_copy), None).to_dict('records')
  
def upload_data_to_dataset(access_token, dataset_id, table_name, data):
    """Push data to a Power BI dataset table"""
    url = f"{POWER_BI_API_URL}/groups/{WORKSPACE_ID}/datasets/{dataset_id}/tables/{table_name}/rows"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Convert data to required format
    rows = {"rows": data}
    
    response = requests.post(url, headers=headers, json=rows)
    if response.status_code == 200:
        print(f"Data uploaded successfully to table {table_name}")
        return True
    else:
        print(f"Error uploading data to table {table_name}: {response.status_code}")
        print(response.text)
        return False

# Clone the report and rebind to a new dataset
def clone_report(workspace_id, report_id, new_report_name, new_dataset_id, access_token):
    url = f"{POWER_BI_API_URL}/groups/{workspace_id}/reports/{report_id}/Clone"
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    body = {
        "name": new_report_name, 
        "targetModelId": new_dataset_id,
        "targetWorkspaceId": workspace_id
    }

    response = requests.post(url, headers=headers, data=json.dumps(body))

    if response.status_code == 200:
        print("Report cloned successfully!")
        print(response.json())
    else:
        print(f"Failed to clone report: {response.status_code}")
        print(response.text)
    
        # Detailed 404 troubleshooting
        print("\nTroubleshooting 404 error:")
        print(f"1. Verify workspace ID '{workspace_id}' exists and you have access")
        print(f"2. Verify report ID '{report_id}' exists in this workspace")
        print(f"3. Verify dataset ID '{new_dataset_id}' exists and is accessible")
        print(f"4. Verify your access token has these permissions:")
        print("   - Report.ReadWrite.All")
        print("   - Dataset.ReadWrite.All")
        
        print("\nFull error response:")
        print(response.text)


def main():
    
    # Get access token
    access_token = get_access_token()
    if not access_token:
        print("Failed to get access token.")
        return

    # Read CSV files
    print(f"Reading CSV files from {CSV_DIRECTORY}...")
    dataframes = read_csv_files(CSV_DIRECTORY)
    if not dataframes:
        print("No CSV files found. Exiting.")
        return
    
    # Create dataset schema from dataframes
    tables = []
    for table_name, df in dataframes.items():
        table_schema = convert_df_to_table_schema(df, table_name)
        tables.append(table_schema)
    
    # Create dataset
    dataset_name = f"Dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"Creating dataset '{dataset_name}'...")
    dataset_id = create_dataset(access_token, dataset_name, tables)
    if not dataset_id:
        print("Failed to create dataset. Exiting.")
        return
    
    # Wait for dataset to be ready
    print("Waiting for dataset to be ready...")
    time.sleep(5)
    
    # Upload data to dataset
    for table_name, df in dataframes.items():
        print(f"Uploading data to table '{table_name}'...")
        rows = convert_df_to_rows(df)
        success = upload_data_to_dataset(access_token, dataset_id, table_name, rows)
        if not success:
            print(f"Failed to upload data to table '{table_name}'. Continuing with other tables.")
 
    clone_report(WORKSPACE_ID, REPORT_ID, NEW_REPORT_NAME, dataset_id, access_token)


if __name__ == "__main__":
    main()