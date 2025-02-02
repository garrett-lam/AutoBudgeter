from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
import boto3
import pandas as pd
from datetime import datetime, timedelta
import logging
from io import StringIO
from scripts import transform

# S3 Configurations
S3_BUCKET = 'personal-finance-transactions'
RAW_FOLDER = "raw-transactions/"  # Folder where transactions are uploaded
PROCESSED_FOLDER = "processed-transactions/"  # Folder for cleaned data

# Initialize S3 client
s3 = boto3.client("s3")

def extract_transactions_from_S3(s3_key: str, **kwargs):
    """Downloads transaction file from S3 and stores it in XCom."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key) # Download file from S3

    # Check if the request was successful
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logging.error(f"Failed to download s3://{S3_BUCKET}/{s3_key} from S3")
    
    # Read from StreamingBody, decode to UTF-8, and convert to DataFrame
    df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')), keep_default_na=False, dtype=str)
    print(df)

    # Get Airflow task instance
    ti = kwargs["ti"]
    ti.xcom_push(key="raw_data", value=df.to_json()) # Store df as JSON 
    ti.xcom_push(key="s3_key", value=s3_key) # Store original s3 file path
        
    logging.info(f"Successfully downloaded s3://{S3_BUCKET}/{s3_key} from S3!")


def determine_transaction_provider(**kwargs):
    """Determines the transaction provider based on the S3 key."""
    ti = kwargs["ti"]
    s3_key = ti.xcom_pull(task_ids="wait_for_s3_file", key="s3_key")

    if not s3_key:
        logging.error("No S3 key found in XCom")
        return
    
    if "chase" in s3_key.lower():
        transaction_provider = "CHASE"
        logging.info("Chase transaction file detected.")
    elif "capital" in s3_key.lower():
        transaction_provider = "CAPITAL_ONE"
        logging.info("Capital One transaction file detected.")
    else:
        logging.error(f"Unsupported transaction provider for s3://{S3_BUCKET}/{s3_key}")
        return
    
    logging.info(f"Transaction provider determined: {transaction_provider}")
    ti.xcom_push(key="transaction_provider", value=transaction_provider)


def transform_transaction_file(**kwargs):
    """Transforms transaction file based on transaction provider."""
    ti = kwargs["ti"]
    # Get raw data from XCom
    raw_data_json = ti.xcom_pull(task_ids="extract_files_from_S3", key="raw_data")
    # Get transaction provider from XCom
    transaction_provider = ti.xcom_pull(task_ids="determine_transaction_provider", key="transaction_provider")

    df = pd.read_json(StringIO(raw_data_json))
    
    # Process data based on transaction provider
    if transaction_provider == "CHASE":
        df_transformed = transform.process_chase_transactions(df)
    elif transaction_provider == "CAPITAL_ONE":
        df_transformed = transform.process_capital_one_transactions(df)
    else:
        logging.error(f"Unsupported transaction provider: {transaction_provider}")
        return

    logging.info(f"Successfully transformed transaction file from {transaction_provider}!")
    ti.xcom_push(key="transformed_data", value=df_transformed.to_json())

def load_transactions_to_S3(**kwargs):
    """Loads transformed transaction file to processed S3 bucket."""
    ti = kwargs["ti"]

    # Get transformed data from XCom
    transformed_data_json = ti.xcom_pull(task_ids="transform_transaction_file", key="transformed_data")

    if not transformed_data_json:
        logging.error("No transformed data found in XCom")
        return
    
    # Convert JSON back to pd.DataFrame
    df_transformed = pd.read_json(StringIO(transformed_data_json))

    


# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
with DAG(
    "s3_transactions_dag",
    default_args = default_args,
    description = "Senses transactions uploads to S3 bucket, consolidates transaction into one file, and uploads back to S3",
    schedule_interval=None, # Triggered by S3 upload
    catchup=False,
) as dag:
    
    # Task to sense for new transaction files in S3 bucket
    s3_sensor = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name=S3_BUCKET,
        bucket_key="raw-transactions/*.csv",  
        wildcard_match=True,  
        aws_conn_id='aws_default'
    )

