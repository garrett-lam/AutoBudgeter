from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
import boto3
import pandas as pd
import os
from datetime import datetime, timedelta

# S3 Configurations
S3_BUCKET = 'personal-finance-transactions'
RAW_FOLDER = "raw-transactions/"  # Folder where transactions are uploaded
PROCESSED_FOLDER = "processed-transactions/"  # Folder for cleaned data

# Initialize S3 client
s3 = boto3.client("s3")



# Define functions here:
def list_s3_transaciton_files():
    # List objects in bucket
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=RAW_FOLDER)

    if 'Contents' not in response:
        print("No files found in raw-transactions folder.")
        return []
    
    files = [obj['Key'] for obj in response['Contents'] if obj['Key'] != RAW_FOLDER]
    print(f"Found {len(files)} transaction files in S3.")
    return files


# # Define default arguments for the DAG
# default_args = {
#     "owner": "airflow",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# # Define DAG
# with DAG(
#     "s3_transactions_dag",
#     default_args = default_args,
#     description = "Senses transactions uploads to S3 bucket, consolidates transaction into one file, and uploads back to S3"
#     schedule_interval=None, # Triggered by S3 upload
#     catchup=False,
# ) as dag:
#     pass

def main():
    print(list_s3_transaciton_files())

if __name__ == "__main__":
    main()