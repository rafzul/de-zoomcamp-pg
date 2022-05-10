from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime
from pathlib import path
import pyspark
from pyspark.sql import SparkSession, types


#setting up Bash parametrization
TAXI_TYPE="yellow"
URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"

FMONTH= `printf "%02d" ${MONTH}`
URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
LOCAL_PREFIX="/media/rafzul/"Terminal Dogma"/nytaxidata/raw/${TAXI_TYPE}/${YEAR}/${MONTH}"
LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

#getting month and year
logical_date = "{{ ds }}"
MONTH = datetime.strptime(logical_date, "%m")
YEAR = datetime.strptime(logical_date, "%y")

#setting up external script path
EXTSCRIPT_PATH = "../scripts/"


#instance a spark session
spark = SparkSession.builder \
    .maste("local[*]") \
    .appName('sparknytaxi') \ 
    .getOrCreate()

#setting up script for parquetizing
def parquetize_data(schema_file, csv_file):
    df_parquetized = spark.read \
    .option("header", "true") \
    .schema(schema_file) \
    .csv(csv_file)

    df_parquetized = df_parquetized.repartition(24)
    df.write.parquet("media/rafzul/"Terminal Dogma"/nytaxidata/raw/{TAXI_TYPE}/{YEAR}/{MONTH}")


#setting up DAG
default_args = {
    "owner": "rafzul",
    "start_date": datetime(2020,1,1),
    "end_date": datetime(2020,2,1)
    "schedule_interval"="@monthly",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="download_dag",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['nytaxi-dag'],
) as dag:

    # for MONTH in {1..12}: ini didefine di schedule_interval buat jaraknya, trus define start_date dan end_date buat start dan mulenya

    # for TAXI_TYPE in {yellow,green}:

    download_data_task = BashOperator(
        task_id='download_data',
        bash_command="../scripts/download_data.sh",
        params= {"TAXI_TYPE": TAXI_TYPE, "YEAR": YEAR, "MONTH": MONTH, "URL_PREFIX": URL_PREFIX},        
    )

    schema_file = Path(f"../schemas/nytaxi_schema_{TAXI_TYPE}")
   
    parquetize_data_task = PythonOperator(
        task_id="parquetize_data",
        python_callable=parquetize_data,
        op_kwargs={
            "schema": schema_file,
            "csv_file": ,
        },
    )
    download_data_task >> parquetize_data_task




