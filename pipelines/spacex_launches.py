import os
import json
import requests
import pandas as pd
import datetime as dt
from airflow import DAG
from pymongo import MongoClient
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# set the job interval 
# staging path 
# project home directory
# airflow home directory
# Mongodb connection path
# json and csv file location
# fetch launches data from spacex api and store into json file
# load fetched data into mongodb spacex_lake database and launches collection
# transform json data and store it into csv file
# define default arguments for dag
# define the dag
# define extract task with python operator
# define transform task
# define load mongodb task 
# define load postgres task
# define task to load into Google Cloud Storage
# define task to load BigQuery from GCS
# define the task dependencies

# set the job interval 
SCHEDULE_INTERVAL = "@once"

# staging path 
SPACEX_STAGING = os.environ.get("SPACEX_STAGING")

# project home directory
SPACEX_HOME = os.environ.get("SPACEX_HOME")

# airflow home directory
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

# Mongodb connection path
MONGO_CONNECTION = "mongodb://localhost:27017"

# json and csv file location
JSON_FILE_LOCATION = f"{SPACEX_STAGING}/json/spacex_launches.json"
CSV_FILE_LOCATION = f"{SPACEX_STAGING}/csv/spacex_launches_transformed.csv"


# fetch launches data from spacex api and store into json file
def extract():
    url = "https://api.spacexdata.com/v4/launches"
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(JSON_FILE_LOCATION, "w") as file:
            json.dump(response.json(), file)

# load fetched data into mongodb spacex_lake database and launches collection
def load_mongo():
    client = MongoClient(MONGO_CONNECTION)
    db = client["spacex_lake"]
    collection = db["launches"]

    with open(JSON_FILE_LOCATION, "r") as file:
        records = json.load(file)
    
    collection.delete_many({})
    collection.insert_many(records)
    client.close()

# transform json data and store it into csv file
def transform():
    fields = ['id','rocket','success','launchpad','flight_number','name','date_utc','upcoming','links']
    launches = pd.read_json(JSON_FILE_LOCATION)
    launches = launches[fields]
    
    launches["success"] = launches["success"].astype("boolean")
    launches["links"] = launches["links"].apply(lambda row: row["webcast"])
    launches = launches.set_index("id")
    
    launches.rename({"links": "webcast"}, axis=1)
    launches.to_csv(CSV_FILE_LOCATION)

# define default arguments for dag
default_args = {
    "owner" : "airflow",
    "start_date" : dt.datetime(2023, 9, 6),
    "email" : ["myairflowcommandcentre@outlook.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay" : dt.timedelta(minutes=1)
}

# define the dag
dag = DAG(
    dag_id = "spacex_launches_pipeline",
    description = "This dag extracts launches data from api and loads into mongodb launches collection",
    schedule_interval = SCHEDULE_INTERVAL,
    template_searchpath = [f"{AIRFLOW_HOME}/dags/"],
    default_args = default_args
)

# define extract task with python operator
extract_task = PythonOperator(
    task_id = "spacex_launches_extract",
    python_callable = extract,
    dag = dag
)

# define transform task
transform_task = PythonOperator(
    task_id = "spacex_launches_transform_to_csv",
    python_callable = transform,
    dag = dag
)

# define load mongodb task 
load_mongo_task = PythonOperator(
    task_id = "spacex_launches_load_to_mongo",
    python_callable = load_mongo,
    dag = dag
)

# define load postgres task
load_postgres_task = PostgresOperator(
    task_id="spacex_launches_csv_to_postgres",
    postgres_conn_id="spacex_postgres",
    sql="sql/copy_launches.sql",
    dag=dag
)

# define task to load into Google Cloud Storage
load_gcs_task = LocalFilesystemToGCSOperator(
    task_id = 'spacex_launches_csv_gcs',
    src = CSV_FILE_LOCATION,
    dst = 'spacex_launches_transformed.csv',
    bucket = "spacex_data_lake",
    gcp_conn_id = 'google_cloud_default',
    dag = dag
)

# define task to load BigQuery from GCS
load_bigquery_task = GCSToBigQueryOperator(
    task_id = "spcex_launches_gcs_bigquery",
    bucket="spacex_data_lake",
    source_objects=["spacex_launches_transformed.csv"],
    destination_project_dataset_table="SpaceX_dataset.Launches",
    gcp_conn_id="google_cloud_default",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

# define the task dependencies
extract_task >> transform_task
transform_task >> [load_mongo_task, load_postgres_task, load_gcs_task]
load_gcs_task >> load_bigquery_task