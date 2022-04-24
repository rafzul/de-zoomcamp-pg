This is my repository for my progress on DataTalksClub's self-paced Data Engineering Zoomcamp. 
https://www.youtube.com/watch?v=bkJZDmreIpA&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb
I'm doing this materials in hopes of refreshing materials on general Data Engineering pipeline building.

Course is divided into 8 Weeks. I'm on my way on Week 5, though some sections have been skipped for my own focus. 

Course guide us by building ETL and ELT pipeline to ingest raw data from NY Cab Data from NY TLC Website, transform and store it throughout Google Cloud Storage and BigQuery using Python & Spark. The workflow are orchestrated by Airflow

### Week 1
Week 1 is focused on learning basic data pipelines, building simple pipeline using Docker & using Postgres as database for simple tests. In this week, we
- creating custom pipeline in docker
- running postgres inside container
- testing scripts to ingest data using Jupyter Notebook
- use Docker network to connect two container (Postgres & Pgadmin)
- turn previous ingestion script inside Jupyter Notebook into .py file
- Dockerizing the script
- Orchestrate the Postgres & Pgadmin container using Docker-compose
- Refreshment on SQL query
- Setup GCP infrastructures programatically using Terraform

### Week 2
Week 2 is focused on learning data ingestion to Data Lake using Airflow
- Refreshment on concept of Data Lake, Data Warehouse, ELT vs ETL, and workflow orchestration with Airflow
- Setting up Airflow through Docker
- Creating DAGs
- Running DAG
- Creating workflow to ingesting New York Taxi raw data to Data Lake
Skipped on materials of Transfer Service due to focus on hand-craft scripts

### Week 3
Week 3 is focused on learning Data Warehouse and BigQuery
- Refreshment on OLTP vs OLAP
- BigQuery's external table
- Refreshment on Partitioning vs Clustering strategy
- Creating Airflow workflow to automate the creation of BQ tables (normal & partitioned) and ingestion of the data from Week 2's data lake
- Creating cloud storage to BQ DAGs
Skipped on BigQuery ML to focus on learning Airflow more

### Week 4
Skipped on week 4 to focus on next week's material

### Week 5 (Ongoing)
Week 5 is focused on batch processing
- Refreshment on Batch vs Streaming
- Spark setup
- Using Spark in Jupyter Notebook
- Create pipeline with Sparks 
    - Reading csv files
    - Partitioning data
    - Create Spark Dataframes
- Writing batch jobs in SQL queries to be ran in Spark
- SQL in Sparks

