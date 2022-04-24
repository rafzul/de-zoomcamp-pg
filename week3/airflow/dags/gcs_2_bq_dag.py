#gcs_2_bq_dag.py

import os
import logging

#airflow
from airflow import DAG
from airflow.utils.dates import days_ago

#installing requirements specified in dockerfile

#module to interact with GCP
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCStoGCSOperator


#set local GCP variables based on env variables in docker compose
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME"  , "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

#parametrize file name
DATASET = "tripdata"
TAXI_TYPES = {'yellow': 'tpep_pickup_datetime', 'fhv': 'Pickup_datetime', 'green': 'lpep_pickup_datetime'}
INPUT_PART = 'raw'


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


#DAG declaration w context manager
with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for taxi_type, ds_col in TAXI_TYPES.items():
        gcs_2_gcs_task = GCStoGCSOperator(
            task_id=f"move_{taxi_type}_{DATASET}_files_task",
            source_bucket=BUCKET,
            source_object=f"{INPUT_PART}/{taxi_type}_*",
            destination_bucket=BUCKET,
            destination_object=f"{taxi_type}/{taxi_type}_",
            move_object=False
        )

        gcs_2_bq_ext_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_{taxi_type}_{DATASET}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": "external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True"
                    "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
                    "sourceFormat": "PARQUET",
                }
            }
        )


        bq_ext_2_task = BigQueryInsertJobOperator(
            task_id="bq_create_{taxi_type}_{DATASET}_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{taxi_type}_{DATASET}_partitoned_table_task \
            PARTITION BY DATE({ds_col}) \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{taxi_type}_{DATASET}_external_table;"
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


    gcs_2_gcs >> gcs_2_bq_ext >> bq_ext_2_part