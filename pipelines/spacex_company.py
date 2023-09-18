import os
import json
import requests
import datetime as dt
from pymongo import MongoClient
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator

SPACEX_STAGING = os.environ.get("SPACEX_STAGING")

def extract():
    url = "https://api.spacexdata.com/v4/company"
    response = requests.get(url)

    if response.status_code == 200:
        with open(f"{SPACEX_STAGING}/spacex_company.json", "w") as file:
            json.dump(response.json(), file)

def load():
    client = MongoClient("mongodb://localhost:27017")
    db = client["spacex_lake"]
    collection = db["company"]
    with open(f"{SPACEX_STAGING}/spacex_company.json", "r") as file:
        records = json.load(file)
    
    collection.delete_many({})
    collection.insert_one(records)
    client.close()

default_args = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 8, 28),
    "email": ["myairflowcommandcentre@outlook.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=1)
}

dag = DAG(
    dag_id="spacex_company_pipeline",
    description="This pipeline extract spacex company details",
    schedule_interval=dt.timedelta(weeks=1),
    default_args=default_args,
)

extract_task = PythonOperator(
    task_id="spacex_company_extract",
    python_callable=extract,
    dag=dag
)

load_task = PythonOperator(
    task_id="spacex_company_load",
    python_callable=load,
    dag=dag
)

extract_task >> load_task