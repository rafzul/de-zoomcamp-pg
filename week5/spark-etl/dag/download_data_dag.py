from airflow.operators import BashOperator
from airflow.models import DAG
# from datetime import datetime, timedelta
from datetime import datetime


#setting up Bash parametrization
TAXI_TYPE="yellow"
URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"

#getting month and year
logical_date = "{{ ds }}"
MONTH = datetime.strptime(logical_date, "%m")
YEAR = datetime.strptime(logical_date, "%y")

#setting up bash script for downloading data (minus the looping, will be done together with the parquetization task for each taxi type/month/   x`year inside dag)

default_args = {
    "owner": "rafzul",
    "start_date": datetime(2020,1,1),
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
        params= {"TAXI_TYPE: {TAXI_TYPE}", "YEAR": {YEAR}, "MONTH": {MONTH}},        
    )

    if scheme == TRUE:
        scheme_and_parquetize_task = PythonOperator(
  
        )
    else:
        