from datetime import datetime

default_args = {
    'start_date': datetime(2016, 1, 1),
    'owner': 'airflow'
}

with DAG(
    dag_id="my_dag_name", 
    default_args=default_args,
    schedule_interval="0 6 2 * *"
    start_date=datetime(2021,1,1)
    ) as dag:
    op1 = DummyOperator(task_id="task1")
    op2 = DummyOperator(task_id="task2")
    op1 >> op2
    print(op1.owner)


# #bashoperator
# download_dataset_task = BashOperator(
#     task_id="download_dataset_task",
#     bash_command=f"curl -sS {dataset_url} > {path_to_local_home}/{dataset_file}"
# )

# #pythonoperator
# format_to_parquet_task = PythonOperator(
#     task_id="format_to_parquet_task",
#     python_callable=format_to_parquet,
#     op_kwargs={
#         "src_file": f"{path_to_local_home}/{dataset_file}",
#     },
# )
