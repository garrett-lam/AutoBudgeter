import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3
import pandas as pd
from datetime import datetime
import logging
from io import StringIO
import src.utils as utils
import src.transform as transform

# S3 Configurations
S3_BUCKET = 'personal-finance-transactions'
RAW_FOLDER = "raw-transactions"  # Folder where transactions are uploaded
PROCESSED_FOLDER = "processed-transactions"  # Folder for cleaned data
AWS_CONN_ID = "airflow-s3"  # Use Airflow connection to retrieve credentials

# # Initialize S3 client
# s3 = boto3.client(
#     "s3", 
#     aws_access_key_id=ACCESS_KEY,
#     aws_secret_access_key=SECRET_KEY,
#     aws_session_token=SESSION_TOKEN
# )

def get_boto3_s3_client():
    """Retrieve AWS credentials from Airflow's S3Hook and initialize a Boto3 S3 client."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    credentials = s3_hook.get_credentials()

    # Initialize Boto3 client with retrieved credentials
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token if credentials.token else None  # Handle None case
    )
    return s3_client


def extract_transactions_from_S3(**kwargs):
    """Downloads multiple transaction files from S3 and stores it in XCom."""
    ti = kwargs["ti"]
    s3 = get_boto3_s3_client()

    # Get the list of all files in `raw-transactions/`
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=RAW_FOLDER)

    # Check if the response is valid
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logging.error(f"Failed to list objects in bucket s3://{S3_BUCKET}/{RAW_FOLDER}")
        return
    # Check if there are any files in the folder
    if "Contents" not in response:
        logging.info("No new transaction files found in S3.")
        return

    # Get bucket path to files
    files = [obj["Key"] for obj in response["Contents"] if obj["Key"] != RAW_FOLDER and obj["Key"].lower().endswith(".csv")]
    logging.info(f"Found {len(files)} files in s3://{S3_BUCKET}/{RAW_FOLDER}")

    # Download each file and store it in a list
    raw_data_jsons = []
    for s3_key in files:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)

        # Check if the response is valid
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            logging.error(f"Failed to download s3://{S3_BUCKET}/{s3_key} from S3")
            return
        
        # Read and decode the CSV file
        df = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")), keep_default_na=False, dtype=str)
        logging.info(f"Successfully downloaded s3://{S3_BUCKET}/{s3_key} from S3!")

        # Store raw data and file key in a list
        raw_data_jsons.append({"s3_key": s3_key, "raw_data": df.to_json()})

    # Push raw data to XCom
    ti.xcom_push(key="raw_transactions_list", value=raw_data_jsons)
    logging.info("Successfully pushed all raw transaction files to XCom!")


def transform_transaction_files(**kwargs):
    """Transforms raw transaction files and stores it in XCom."""
    ti = kwargs["ti"]
    # Get the list of raw transactions from XCom
    raw_transactions = ti.xcom_pull(task_ids="extract_transactions_from_S3", key="raw_transactions_list")

    # Check if there are any transactions
    if not raw_transactions:
        logging.error("No raw transaction data found in XCom.")
        return

    # Process each transaction file
    transformed_data_list = []
    for transaction in raw_transactions:
        s3_key = transaction["s3_key"]
        raw_data_json = transaction["raw_data"]
        df = pd.read_json(StringIO(raw_data_json))

        if "chase" in s3_key.lower():
            logging.info("Chase transaction file detected.")
            df_transformed = transform.process_chase_transactions(df)
            # transaction_provider = "CHASE"
        elif "capital" in s3_key.lower():
            logging.info("Capital One transaction file detected.")
            df_transformed = transform.process_capital_one_transactions(df)
            # transaction_provider = "CAPITAL_ONE"
        else:
            logging.error(f"Unsupported transaction provider for s3://{S3_BUCKET}/{s3_key}")
            return
        
        # Append the transformed data
        transformed_data_list.append(df_transformed)
        logging.info(f"Successfully transformed s3://{S3_BUCKET}/{s3_key}!")
        
    # Push all transformed data as a list to XCom
    ti.xcom_push(key="transformed_data_list", value=[df.to_json() for df in transformed_data_list])


def load_transactions_to_S3(**kwargs):
    """Consolidates transformed transaction files and uploads to S3."""
    ti = kwargs["ti"]

    # Get transformed data from XCom
    transformed_data_list = ti.xcom_pull(task_ids="transform_transaction_files", key="transformed_data_list")
    # Check if there are any transformed data
    if not transformed_data_list:
        logging.error("No transformed data found in XCom.")
        return

    # Convert JSON strings back to DataFrames
    transformed_dfs = [pd.read_json(StringIO(data)) for data in transformed_data_list]

    # Consolidate all DataFrames into one
    df_final = pd.concat(transformed_dfs, ignore_index=True)

    logging.info(f"Consolidated {len(transformed_dfs)} transaction files.")

    # Generate a new consolidated file key
    consolidated_key = f"{PROCESSED_FOLDER}/{utils.get_previous_month()}-consolidated-transactions.csv"

    # Convert DataFrame to CSV in-memory
    csv_buffer = StringIO()
    df_final.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # Reset buffer position

    # Upload to S3
    s3 = get_boto3_s3_client()
    s3.put_object(Bucket=S3_BUCKET, Key=consolidated_key, Body=csv_buffer.getvalue())
    logging.info(f"Uploaded consolidated file to s3://{S3_BUCKET}/{consolidated_key}")


# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

# Define DAG
with DAG(
    "s3_transactions_dag",
    default_args = default_args,
    description = "DAG for processing transaction files from S3 and consolidating them into one file.",
    schedule_interval=None, 
    catchup=False,
) as dag:
    
    # Task to extract transactions from S3
    extract_transactions_from_S3 = PythonOperator(
        task_id="extract_transactions_from_S3",
        python_callable=extract_transactions_from_S3,
        provide_context=True,
    )

    # Task to transform transactions
    transform_transaction_files = PythonOperator(
        task_id="transform_transaction_files",
        python_callable=transform_transaction_files,
        provide_context=True,
    )

    # Task to load transformed transactions to S3
    load_transactions_to_S3 = PythonOperator(
        task_id="load_transactions_to_S3",
        python_callable=load_transactions_to_S3,
        provide_context=True,
    )

    # Set task dependencies
    extract_transactions_from_S3 >> transform_transaction_files >> load_transactions_to_S3