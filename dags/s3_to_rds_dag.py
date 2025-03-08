import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from datetime import datetime
import logging
import os
import psycopg2
import io
import json
from dotenv import load_dotenv
from io import StringIO
import src.utils as utils
import src.transform as transform


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s() - %(message)s"
)
logger = logging.getLogger(__name__)

# S3 Configurations
S3_BUCKET = 'personal-finance-transactions'
RAW_FOLDER = "raw-transactions"  # Folder where transactions are uploaded
PROCESSED_FOLDER = "processed-transactions"  # Folder where processed transactions are stored

def get_s3_client():
    """
    Retrieve a Boto3 S3 client using default credentials (via the EC2 IAM role).
    """
    return boto3.client("s3",
                        # aws_access_key_id=ACCESS_KEY,
                        # aws_secret_access_key=SECRET_KEY,
                        # aws_session_token=SESSION_TOKEN
    )

def extract_transactions_from_S3(**kwargs):
    """
    Downloads CSV transaction files from the single folder present under RAW_FOLDER.
    Pushes the raw data along with the folder name to XCom for further processing.
    """
    ti = kwargs["ti"]
    s3 = get_s3_client()
    
    # List subdirectories under RAW_FOLDER 
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=RAW_FOLDER, Delimiter='/')
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.error(f"Failed to list objects in bucket s3://{S3_BUCKET}/{RAW_FOLDER}") # TODO: log response?
        return
    if "CommonPrefixes" not in response or len(response["CommonPrefixes"]) == 0:
        logger.info(f"No subdirectories found under s3://{S3_BUCKET}/{RAW_FOLDER}.")
        return

    # Extract this month's transactions directory
    dir = response["CommonPrefixes"][0]["Prefix"]
    # Remove the RAW_FOLDER prefix to get the folder name.
    dir_name = dir.replace(RAW_FOLDER, "", 1)
    logger.info(f"Processing folder: {dir}")

    # List objects in this dir.
    dir_response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=dir)
    if dir_response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.error(f"Failed to list objects in dir {dir}")
        return
    if "Contents" not in dir_response:
        logger.info(f"No files found in dir {dir}")
        return

    # Filter for CSV files.
    files = [obj["Key"] for obj in dir_response["Contents"] if obj["Key"].lower().endswith(".csv")]
    logger.info(f"Found {len(files)} CSV files in dir {dir}")

    raw_data_jsons = []
    for s3_key in files:
        file_response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        if file_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            logger.error(f"Failed to download {s3_key}")
            continue

        df = pd.read_csv(StringIO(file_response["Body"].read().decode("utf-8")),
                         keep_default_na=False, dtype=str)
        logger.info(f"Downloaded {s3_key} successfully!")
        raw_data_jsons.append({"s3_key": s3_key, "raw_data": df.to_json()})

    if raw_data_jsons:
        ti.xcom_push(key="raw_transactions_list", value=raw_data_jsons)
        ti.xcom_push(key="processed_folder", value=dir_name) # for moving
        logger.info("Pushed raw data and dir name to XCom!")
    else:
        logger.info("No CSV files processed from the dir.")

def transform_transaction_files(**kwargs):
    """
    Transforms raw transaction files and pushes the transformed data to XCom.
    """
    ti = kwargs["ti"]
    raw_transactions = ti.xcom_pull(task_ids="extract_transactions_from_S3", key="raw_transactions_list")
    if not raw_transactions:
        logger.error("No raw transaction data found in XCom.")
        return

    transformed_data_list = []
    for transaction in raw_transactions:
        s3_key = transaction["s3_key"]
        raw_data_json = transaction["raw_data"]
        df = pd.read_json(StringIO(raw_data_json))
        
        if "chase" in s3_key.lower():
            logger.info("Chase transaction file detected.")
            df_transformed = transform.process_chase_transactions(df)
        elif "capital" in s3_key.lower():
            logger.info("Capital One transaction file detected.")
            df_transformed = transform.process_capital_one_transactions(df)
        elif "bilt" in s3_key.lower():
            logger.info("Bilt transaction file detected.")
            df_transformed = transform.process_bilt_transactions(df)
        else:
            logger.error(f"Unsupported transaction provider for {s3_key}")
            continue

        transformed_data_list.append(df_transformed)
        logger.info(f"Transformed {s3_key} successfully!")
    
    if transformed_data_list:
        ti.xcom_push(key="transformed_data_list", value=[df.to_json() for df in transformed_data_list])
        logger.info("Pushed transformed data to XCom!")

def get_rds_credentials():
    secret_name = "personal-finance-transactions/postgres"
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager',region_name=region_name )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Error retrieving secret {secret_name}: {e}")
        raise e

    secret = get_secret_value_response['SecretString']

    return json.loads(secret)

def load_transactions_to_RDS(**kwargs):
    """
    Consolidates the transformed transaction files and loads them into an RDS PostgreSQL database.
    """
    ti = kwargs["ti"]
    transformed_data_list = ti.xcom_pull(task_ids="transform_transaction_files", key="transformed_data_list")
    if not transformed_data_list:
        logger.error("No transformed data found in XCom.")
        return

    # Convert JSON strings back to DataFrames and consolidate data
    transformed_dfs = [pd.read_json(StringIO(data)) for data in transformed_data_list]
    df_final = pd.concat(transformed_dfs, ignore_index=True)
    logger.info(f"Consolidated {len(transformed_dfs)} transaction files.")

    # Retrieve RDS connection details from environment variables.
    secret = get_rds_credentials()
    rds_host = secret.get('RDS_HOST')
    rds_db = secret.get('RDS_DB')
    rds_user = secret.get('RDS_USER')
    rds_password = secret.get('RDS_PASSWORD')
    
    if not all([rds_host, rds_db, rds_user, rds_password]):
        logger.error("Not all RDS connection details are provided.")
        return
    try:
        # Connect to RDS PostgreSQL database
        conn = psycopg2.connect(
            host=rds_host,
            database=rds_db,
            user=rds_user,
            password=rds_password
        )
        cursor = conn.cursor()

        # Using StringIO (memory file-like object) to create a CSV buffer from the consolidated DataFrame
        csv_buffer = io.StringIO()
        df_final.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)
        # Bulk load more efficient than inserting row by row
        cursor.copy_expert("COPY transactions.credit_card_transactions FROM STDIN WITH CSV", csv_buffer)
        conn.commit()
        logger.info("Loaded transactions into RDS successfully!")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading transactions to RDS: {e}")
    finally:
        # Close the cursor and connection regardless of success or failure
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def move_processed_folder(**kwargs):
    """
    Retrieves the processed folder names from XCom (set by the extract task)
    and moves each folder from RAW_FOLDER to PROCESSED_FOLDER in S3.
    This function inlines the folder-moving logic rather than calling a separate helper.
    """
    ti = kwargs["ti"]
    processed_folders = ti.xcom_pull(task_ids="extract_transactions_from_S3", key="processed_folders")
    if not processed_folders:
        logger.info("No folder names found in XCom; nothing to move.")
        return

    s3 = get_s3_client()
    for folder in processed_folders:
        source_prefix = f"{RAW_FOLDER}{folder}"
        destination_prefix = f"{PROCESSED_FOLDER}{folder}"
        logger.info(f"Moving objects from {source_prefix} to {destination_prefix}")

        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=source_prefix)
        if "Contents" not in response:
            logger.info(f"No objects found under {source_prefix}")
            continue

        for obj in response["Contents"]:
            source_key = obj["Key"]
            destination_key = source_key.replace(RAW_FOLDER, PROCESSED_FOLDER, 1)
            copy_source = {"Bucket": S3_BUCKET, "Key": source_key}
            s3.copy_object(Bucket=S3_BUCKET, CopySource=copy_source, Key=destination_key)
            logger.info(f"Copied {source_key} to {destination_key}")
            s3.delete_object(Bucket=S3_BUCKET, Key=source_key)
            logger.info(f"Deleted {source_key} after copying.")
        
        logger.info(f"Moved folder {folder} to {PROCESSED_FOLDER}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    "s3_to_rds_transactions_dag",
    default_args=default_args,
    description="DAG for processing arbitrary folders uploaded to S3/raw-transactions, loading transactions into RDS, then moving folders to processed-transactions.",
    schedule_interval="@monthly",  # or another schedule as needed
    catchup=False,
) as dag:
    
    extract_transactions_from_S3_task = PythonOperator(
        task_id="extract_transactions_from_S3",
        python_callable=extract_transactions_from_S3,
        provide_context=True,
    )
    
    transform_transaction_files_task = PythonOperator(
        task_id="transform_transaction_files",
        python_callable=transform_transaction_files,
        provide_context=True,
    )
    
    load_transactions_to_RDS_task = PythonOperator(
        task_id="load_transactions_to_RDS",
        python_callable=load_transactions_to_RDS,
        provide_context=True,
    )
    
    move_processed_folder_task = PythonOperator(
        task_id="move_processed_folder",
        python_callable=move_processed_folder,
        provide_context=True,
    )

    # Task dependencies: Extract -> Transform -> Load -> Move folders.
    extract_transactions_from_S3_task >> transform_transaction_files_task >> load_transactions_to_RDS_task >> move_processed_folder_task
