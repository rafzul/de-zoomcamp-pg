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
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

#convert data to parquet format
import pyarrow.csv as pv
import pyarrow.parquet as pq

#set local GCP variables based on env variables in docker compose
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

#specify our dataset
dataset_file = "yellow_tripdata_2021-01.csv"
dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
#Replace CSV with Parquet for filename template
parquet_file = dataset_file.replace('.csv', '.parquet')
#get env variable locally in docker container, second arguments is default value kalo ngga nemu
path_to_local_home = os.environ.get("AIRFLOW_HOME"  , "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")


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


#DAG declaration w context manager
with DAG(
    dag_id="bq_ingestion_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['dtc-de-homework'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )  

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}"
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,  
        op_kwargs={
            "bucket": BUCKET, 
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        }
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
                "sourceFormat": "PARQUET"
            }
        }
    )


    download_dataset_task >> format_to_parquet_task >> upload_to_gcs_task >> bigquery_external_table_task