import os
import json
import requests
import datetime as dt
from airflow import DAG
from pymongo import MongoClient
from airflow.operators.python_operator import PythonOperator

SPACEX_STAGING = os.environ.get("SPACEX_STAGING")

def extract():
    url = "https://api.spacexdata.com/v4/launchpads"
    response = requests.get(url)
    if response.status_code == 200:
        with open(f"{SPACEX_STAGING}/spacex_launchpads.json", "w") as file:
            json.dump(response.json(), file)

def load():
    client = MongoClient("mongodb://localhost:27017")
    db = client["spacex_lake"]
    collection = db["launchpads"]

    with open(f"{SPACEX_STAGING}/spacex_launchpads.json", "r") as file:
        records = json.load(file)
    
    collection.delete_many({})
    collection.insert_many(records)
    client.close()

default_args = {
    "owner" : "airflow",
    "start_date" : dt.datetime(2023, 8, 28),
    "email" : ["myairflowcommandcentre@outlook.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay" : dt.timedelta(minutes=1)
}

dag = DAG(
    dag_id = "spacex_launchpads_pipeline",
    description = "This dag extracts launchpads data from api and loads into mongodb launchpads collection",
    schedule_interval = dt.timedelta(minutes=5),
    default_args = default_args
)

extract_task = PythonOperator(
    task_id = "spacex_launchpads_extract",
    python_callable = extract,
    dag = dag
)

load_task = PythonOperator(
    task_id = "spacex_launchpads_load",
    python_callable = load,
    dag = dag
)

extract_task >> load_task