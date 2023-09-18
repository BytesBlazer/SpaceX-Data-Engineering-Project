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

SCHEDULE_INTERVAL = "@once"
SPACEX_STAGING = os.environ.get("SPACEX_STAGING")
SPACEX_HOME = os.environ.get("SPACEX_HOME")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
MONGO_CONNECTION = "mongodb://localhost:27017"
JSON_FILE_LOCATION = f"{SPACEX_STAGING}/json/spacex_history.json"
CSV_FILE_LOCATION = f"{SPACEX_STAGING}/csv/spacex_history_transformed.csv"

def extract():
    url = "https://api.spacexdata.com/v4/history"
    response = requests.get(url)
    if response.status_code == 200:
        with open(JSON_FILE_LOCATION, "w") as file:
            json.dump(response.json(), file)

def load_mongo():
    client = MongoClient(MONGO_CONNECTION)
    db = client["spacex_lake"]
    collection = db["history"]

    with open(JSON_FILE_LOCATION, "r") as file:
        records = json.load(file)

    collection.delete_many({})
    collection.insert_many(records)
    client.close()

def transform():
    fields = ["id", "title", "links", "event_date_utc"]
    history = pd.read_json(JSON_FILE_LOCATION)

    history = history[fields]
    history["links"] = history["links"].apply(lambda row: row["article"])
    history = history.set_index("id")

    history.to_csv(CSV_FILE_LOCATION)

default_args = {
    "owner" : "airflow",
    "start_date" : dt.datetime(2023, 9, 6),
    "email" : ["myairflowcommandcentre@outlook.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay" : dt.timedelta(minutes=1)
}

dag = DAG(
    dag_id = "spacex_history_pipeline",
    description = "This dag extracts historical events from api and loads into mongodb history collection",
    schedule_interval = SCHEDULE_INTERVAL,
    template_searchpath = [f"{AIRFLOW_HOME}/dags/"],
    default_args = default_args
)

extract_task = PythonOperator(
    task_id = "spacex_history_extract",
    python_callable = extract,
    dag = dag
)

transform_task = PythonOperator(
    task_id = "spacex_history_transform_to_csv",
    python_callable = transform,
    dag = dag
)

load_mongo_task = PythonOperator(
    task_id="spacex_history_load_mongo",
    python_callable=load_mongo,
    dag = dag
)

load_postgres_task = PostgresOperator(
    task_id="spacex_history_load_postgres",
    postgres_conn_id="spacex_postgres",
    sql="sql/copy_history.sql",
    dag=dag
)

load_gcs_task = LocalFilesystemToGCSOperator(
    task_id='spacex_history_csv_gcs',
    src=CSV_FILE_LOCATION,
    dst='spacex_history_transformed.csv',
    bucket="spacex_data_lake",
    gcp_conn_id='google_cloud_default',
    dag = dag
)

load_bigquery_task = GCSToBigQueryOperator(
    task_id = "spcex_history_gcs_bigquery",
    bucket="spacex_data_lake",
    source_objects=["spacex_history_transformed.csv"],
    destination_project_dataset_table="SpaceX_dataset.history",
    gcp_conn_id="google_cloud_default",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

extract_task >> transform_task
transform_task >> [load_mongo_task, load_postgres_task, load_gcs_task]
load_gcs_task >> load_bigquery_task