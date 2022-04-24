#bq_ingestion_dag.py

import os
import logging
from re import S

#airflow
from airflow import DAG
from airflow.utils.dates import days_ago

#operator import
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#installing requirements specified in dockerfile

#module to interact with GCP
from google.cloud import storage
from airflow.providers.google.cloud.bigquery import BigQueryCreateExternalTableOperator

#convert data to parquet format
import pyarrow.csv as pv
import pyarrow.parquet as pq

#set local GCP variables based on env variables in docker compose
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_PROJECT_BUCKET")

#specify our dataset
dataset_file = "yellow_tripdata_2021-01.csv"
dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
#Replace CSV with Parquet for filename template
parquet_file = dataset_file.replace('.csv', '.parquet')
#Store env variable locally in docker container, second arguments is default value
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all"


#format to parquet task
def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source file in CSV format for the moment")
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

#upload to gcs task
def upload_to_gcs(bucket, object_name, local_file):
    """
    Reference from https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    bucket: GCS bucket name
    object_name: target path & file-name
    local_file: source path & file-name
    """
    #prevent imeout when upload file larger than 6MB on internet speed < 800 kbps using google.cloud.storage.Blob.upload_from_filename() f
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # set default size of a multipart upload to 5 MB (if done multipart)
    
    client = storage.Client()
    bucket = client.bucket(bucket)
    
    blob = bucket.blob(object_name)
    blob.chunk_size =  5 * 1024 * 1024 #set chunk of data when iterating before uploading
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


#DAG declaration 


