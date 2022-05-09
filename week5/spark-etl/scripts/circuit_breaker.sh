 # if schema_file.exists(): 
 # if error -> schema gak cocok:
        
    # else:
    #     get_sampledatas_task = BashOperator(
    #         task_id='get_sampledatas',
    #         bash_command="../scripts/get_sampledatas.sh",
    #         params= {"TAXI_TYPE": TAXI_TYPE, "YEAR": YEAR, "MONTH": MONTH},
    #     )

    #     get_schema_task = PythonOperator(
    #         task_id='get_schema',
    #         python_callable=get_schema_data,
    #         op_kwargs={

    #         },
    #     )

#apply di sebelum query SQL, mau di BQ ato di Spark
#conditionalsnya +