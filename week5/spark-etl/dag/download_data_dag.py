from zmq import TYPE
from airflow.operators import BashOperator
from airflow.models import DAG
# from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


#setting up Bash parametrization
TAXI_TYPE=$1
YEAR=$2
URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"

#setting up bash script for downloading data (minus the looping, will be done together with the parquetization task for each taxi type/month/year inside dag)

BASH_SCRIPT = (
            f"
            set -e 

            FMONTH= `printf "%02d" ${MONTH}`
            URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
            LOCAL_PREFIX="/media/rafzul/Terminal Dogma/nytaxidata/raw/${TAXI_TYPE}/${YEAR}/${MONTH}"
            LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
            LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

            echo "downloading ${URL} to ${LOCAL_PATH}"
            mkdir -p ${LOCAL_PREFIX}
            wget ${URL} -O ${LOCAL_PATH}

            echo "compressing ${LOCAL_PATH}"
            gzip
            "
        )

with DAG(
    dag_id="download_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['nytaxi-dag'],
) as dag:

    for MONTH in {1..12}:
        download_data_task = BashOperator(
            task_id='download_data',
            bash_command=BASH_SCRIPT,
            
        )

        if scheme == TRUE:
            scheme_and_parquetize_task = PythonOperator(
            
        )
        else:
        