# Import necessary libraries and modules
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

# Define constants and environment variables
SCHEDULE_INTERVAL = "@once"
SPACEX_STAGING = os.environ.get("SPACEX_STAGING")
SPACEX_HOME = os.environ.get("SPACEX_HOME")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
MONGO_CONNECTION = "mongodb://localhost:27017"
JSON_FILE_LOCATION = f"{SPACEX_STAGING}/json/spacex_payloads.json"
CSV_FILE_LOCATION = f"{SPACEX_STAGING}/csv/spacex_payloads_transformed.csv"

# Task 1: Extract data from SpaceX API
def extract():
    url = "https://api.spacexdata.com/v4/payloads"
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(JSON_FILE_LOCATION, "w") as file:
            json.dump(response.json(), file)

# Task 2: Load extracted data into MongoDB
def load_to_mongo():
    client = MongoClient(MONGO_CONNECTION)
    db = client["spacex_lake"]
    collection = db["payloads"]

    with open(JSON_FILE_LOCATION, "r") as file:
        records = json.load(file)
    
    collection.delete_many({})
    collection.insert_many(records)
    client.close()

# Task 3: Transform data into CSV format
def transform():
    fields = ["id","name","type","reused",
              "launch","mass_kg","orbit",
              "regime","reference_system","periapsis_km",
              "apoapsis_km","inclination_deg",
              "period_min","lifespan_years"]
    
    payloads = pd.read_json(f"{SPACEX_STAGING}/json/spacex_payloads.json")
    pay_man = payloads[["id", "manufacturers"]]
    pay_man = pay_man.explode("manufacturers", ignore_index=True)
    pay_man.columns = ["payload_id", "manufacturer"]

    payloads = payloads[fields]
    payloads = payloads.set_index("id")

    payloads.to_csv(CSV_FILE_LOCATION)
    pay_man.to_csv(f"{SPACEX_STAGING}/csv/spacex_payload_manufacturers_transformed.csv", index_label="id")

# Define default arguments for the DAG
default_args = {
    "owner" : "airflow",
    "start_date" : dt.datetime(2023, 9, 6),
    "email" : ["myairflowcommandcentre@outlook.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay" : dt.timedelta(minutes=1)
}

# Create the DAG
dag = DAG(
    dag_id = "spacex_payloads_pipeline",
    description = "This DAG extracts payloads data from the API and loads it into MongoDB payloads collection",
    schedule_interval = SCHEDULE_INTERVAL,
    template_searchpath = [f"{AIRFLOW_HOME}/dags/"],
    default_args = default_args
)

# Task 1: Extract data from SpaceX API
extract_task = PythonOperator(
    task_id = "spacex_payloads_extract",
    python_callable = extract,
    dag = dag
)

# Task 2: Transform data into CSV format
transform_task = PythonOperator(
    task_id = "spacex_payloads_transform_csv",
    python_callable = transform,
    dag = dag
)

# Task 3: Load data into MongoDB
load_mongo_task = PythonOperator(
    task_id = "spacex_payloads_load_to_mongo",
    python_callable = load_to_mongo,
    dag = dag
)

# Task 4: Load data into PostgreSQL
load_postgres_task = PostgresOperator(
    task_id="spacex_payloads_csv_to_postgres",
    postgres_conn_id="spacex_postgres",
    sql="sql/copy_payloads.sql",
    dag=dag
)

# Task 5: Load CSV data into Google Cloud Storage (GCS)
load_gcs_task = LocalFilesystemToGCSOperator(
    task_id = 'spacex_payloads_csv_gcs',
    src = CSV_FILE_LOCATION,
    dst = 'spacex_payloads_transformed.csv',
    bucket = "spacex_data_lake",
    gcp_conn_id = 'google_cloud_default',
    dag = dag
)

# Task 6: Load data from GCS into BigQuery
load_bigquery_task = GCSToBigQueryOperator(
    task_id = "spcex_payloads_gcs_bigquery",
    bucket="spacex_data_lake",
    source_objects=["spacex_payloads_transformed.csv"],
    destination_project_dataset_table="SpaceX_dataset.Payloads",
    gcp_conn_id="google_cloud_default",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

# Task 7: Load manufacturer data from GCS into BigQuery
load_manufacturers_bigquery_task = GCSToBigQueryOperator(
    task_id = "spcex_payload_manufacturer_gcs_bigquery_manufacturer",
    bucket="spacex_data_lake",
    source_objects=["spacex_payload_manufacturers_transformed.csv"],
    destination_project_dataset_table="SpaceX_dataset.Payloads_Manufacturer",
    gcp_conn_id="google_cloud_default",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

# Define task dependencies
extract_task >> transform_task
transform_task >> [load_mongo_task, load_postgres_task, load_gcs_task, load_manufacturer_gcs_task]
load_gcs_task >> load_bigquery_task
load_manufacturer_gcs_task >> load_manufacturers_bigquery_task
