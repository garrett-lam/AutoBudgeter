import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import pandas as pd
from datetime import datetime
import logging
import os
import psycopg2
import io
from io import StringIO
import src.utils as utils
import src.transform as transform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s() - %(message)s"
)
logger = logging.getLogger(__name__)

# S3 Configurations
S3_BUCKET = 'personal-finance-transactions'
RAW_FOLDER = "raw-transactions"  # Folder where transactions are uploaded

def get_boto3_s3_client():
    """
    Retrieve a Boto3 S3 client using default credentials (via the EC2 IAM role).
    """
    return boto3.client("s3")


def extract_transactions_from_S3(**kwargs):
    """
    Downloads multiple transaction files from S3 from the RAW_FOLDER and pushes them to XCom.
    """
    ti = kwargs["ti"]
    s3 = get_boto3_s3_client()

    # Get the list of all files in the RAW_FOLDER
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=RAW_FOLDER)

    # Check if the response is valid
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.error(f"Failed to list objects in bucket s3://{S3_BUCKET}/{RAW_FOLDER}")
        return
    # Check if there are any files in the folder
    if "Contents" not in response:
        logger.info("No new transaction files found in S3.")
        return

    # Filter for CSV files (ignore the folder itself)
    files = [obj["Key"] for obj in response["Contents"]
             if obj["Key"] != RAW_FOLDER and obj["Key"].lower().endswith(".csv")]
    logger.info(f"Found {len(files)} files in s3://{S3_BUCKET}/{RAW_FOLDER}")

    # Download each file and store it in a list
    raw_data_jsons = []
    for s3_key in files:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            logger.error(f"Failed to download s3://{S3_BUCKET}/{s3_key} from S3")
            return

        # Read and decode the CSV file
        df = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")), 
                         keep_default_na=False, dtype=str)
        logger.info(f"Successfully downloaded s3://{S3_BUCKET}/{s3_key} from S3!")
        raw_data_jsons.append({"s3_key": s3_key, "raw_data": df.to_json()})

    # Push raw data to XCom
    ti.xcom_push(key="raw_transactions_list", value=raw_data_jsons)
    logger.info("Successfully pushed all raw transaction files to XCom!")


def transform_transaction_files(**kwargs):
    """
    Transforms raw transaction files and pushes the transformed data to XCom.
    """
    ti = kwargs["ti"]
    # Get the list of raw transactions from XCom
    raw_transactions = ti.xcom_pull(task_ids="extract_transactions_from_S3", key="raw_transactions_list")

    # Check if there are any transactions
    if not raw_transactions:
        logger.error("No raw transaction data found in XCom.")
        return

    # Process each transaction file
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
        else:
            logger.error(f"Unsupported transaction provider for s3://{S3_BUCKET}/{s3_key}")
            return

        # Append the transformed data
        transformed_data_list.append(df_transformed)
        logger.info(f"Successfully transformed s3://{S3_BUCKET}/{s3_key}!")
    
    # Push all transformed data as a list to XCom
    ti.xcom_push(key="transformed_data_list", value=[df.to_json() for df in transformed_data_list])


def load_transactions_to_RDS(**kwargs):
    """
    Consolidates the transformed transaction files and loads them into an RDS PostgreSQL database.
    Assumes that the RDS connection details (RDS_HOST, RDS_DB, RDS_USER, RDS_PASSWORD)
    are provided as environment variables and that the target table 'transactions' already exists.
    """
    ti = kwargs["ti"]
    transformed_data_list = ti.xcom_pull(task_ids="transform_transaction_files", key="transformed_data_list")

    if not transformed_data_list:
        logger.error("No transformed data found in XCom.")
        return

    # Convert JSON strings back to DataFrames and consolidate them
    transformed_dfs = [pd.read_json(StringIO(data)) for data in transformed_data_list]
    df_final = pd.concat(transformed_dfs, ignore_index=True)
    logger.info(f"Consolidated {len(transformed_dfs)} transaction files.")

    # Retrieve RDS connection details from environment variables
    rds_host = os.getenv('RDS_HOST')
    rds_db = os.getenv('RDS_DB')
    rds_user = os.getenv('RDS_USER')
    rds_password = os.getenv('RDS_PASSWORD')

    if not all([rds_host, rds_db, rds_user, rds_password]):
        logger.error("RDS connection details are not fully provided in environment variables.")
        return

    try:
        conn = psycopg2.connect(
            host=rds_host,
            database=rds_db,
            user=rds_user,
            password=rds_password
        )
        cursor = conn.cursor()

        # Using StringIO to create a CSV buffer from the consolidated DataFrame
        csv_buffer = io.StringIO()
        df_final.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        # Copy the CSV data into the 'transactions' table
        cursor.copy_expert("COPY transactions FROM STDIN WITH CSV", csv_buffer)
        conn.commit()
        logger.info("Successfully loaded transactions into RDS.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading transactions to RDS: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    "s3_to_rds_transactions_dag",
    default_args=default_args,
    description="DAG for processing transaction files from S3 and loading them into RDS.",
    schedule_interval=None,
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

    extract_transactions_from_S3_task >> transform_transaction_files_task >> load_transactions_to_RDS_task
