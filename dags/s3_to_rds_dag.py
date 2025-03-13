import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
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

def get_s3_client():
    """
    Retrieve a Boto3 S3 client using airflow's connection.
    """
    aws_conn = BaseHook.get_connection("airflow-user")
    return boto3.client(
        "s3",
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password
    )

def extract_transactions_from_S3(**kwargs):
    """
    Downloads CSV transaction files from the S3 bucket.
    Pushes the raw data to XCom for further processing.
    """
    ti = kwargs["ti"]
    s3 = get_s3_client()
    
    # List all objects in the bucket
    response = s3.list_objects_v2(Bucket=S3_BUCKET)
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.error(f"Failed to list objects in bucket {S3_BUCKET}")
        return
    if "Contents" not in response:
        logger.info(f"No files found in bucket {S3_BUCKET}.")
        return

    # Filter for CSV files.
    files = [obj["Key"] for obj in response["Contents"] if obj["Key"].lower().endswith(".csv")]
    logger.info(f"Found {len(files)} CSV files in bucket {S3_BUCKET}")

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
        logger.info("Pushed raw data to XCom!")
    else:
        logger.info("No CSV files processed from the bucket.")

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

    # Retrieve RDS connection details from the Airflow connection 'postgres_default'
    pg_conn = BaseHook.get_connection("transactionsDB")
    rds_host = pg_conn.host
    rds_db = pg_conn.schema
    rds_user = pg_conn.login
    rds_password = pg_conn.password
    
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
        cursor.copy_expert(
            """
            COPY transactions.credit_card_transactions (
                transaction_date,
                merchant,
                category,
                amount,
                card_provider
            )
            FROM STDIN
            WITH CSV HEADER
            """,
            csv_buffer
        )
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

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}
with DAG(
    "s3_to_rds_transactions_dag",
    default_args=default_args,
    description="DAG for processing CSV files uploaded to S3, loading transactions into RDS.",
    schedule_interval="@monthly",
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
