# SpaceX Data Engineering Project 🚀

Welcome to my data engineering project of SpaceX.
## Architecture
The project contains various components
- SpaceX API
- Airflow
- Postgresql
- MongoDB
- Google Cloud Storage
- BigQuery
- Power BI
## Overview
Data is captured in real time from the [SpaceX API](https://github.com/r-spacex/SpaceX-API), The data collected from the SpaceX api is then stored on local disk and timely moved to MongoDB and Google Cloud Storage, ETL jobs are scheduled using Airflow to run every 10 minutes.
## ETL Pipeline
- **Fetch SpaceX API Data:** Retrieve data from SpaceX API using Python's `requests` library and save it as a local JSON file.
- **Store Data in MongoDB:** Ingest the data into MongoDB for flexible analysis and querying capabilities.
- **Transform Data with Pandas:** Utilize Pandas to read the JSON file, apply necessary transformations, and store the refined data as CSV files.
- **Utilize Google Cloud Storage:** Store the transformed data securely and efficiently in Google Cloud Storage.
- **Load Data into BigQuery:** Transfer the data from Google Cloud Storage into BigQuery for robust data warehousing and querying capabilities.
- **Visualize with Power BI:** Leverage Power BI to create insightful visualizations and reports based on data retrieved from BigQuery.
# Environment Setup

### Airflow Setup
- Follow official documentation to install and configure airflow from [here](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html)
- Additionally install Postgresql and Google Operators 
```bash
pip install apache-airflow[postgresql]
pip install apache-airflow[google]
```
### MongoDB Setup
- Follow [this](https://www.mongodb.com/docs/manual/installation/) guide to install mongodb 
- We will be using **pymongo** library to load raw data into mongodb to install it
```bash
pip install pymongo
```
### Postgresql Setup
- Follow the instructions [here](https://ubuntu.com/server/docs/databases-postgresql) to install and configure postgresql.
- You can use default `postgres` user account or create another account and add necessary roles to work with database.
- Once the setup is complete login to postgresql and create new database named `SpaceX` and connect to it.
```sql
CREATE DATABASE SpaceX;
\c SpaceX;
```
- After creating database we will create necessary tables using SQL script located at `sql` directory
```sql
\i path/to/project/directory/sql/DDL_spacex_schema.sql
```
- In order to enable airflow to communicate with postgresql we will use Airflow Connections
- login to **Airflow Web UI** from **Admin** tab select **Connections**
- Edit **postgres_default** connection, in the next page provide **Host**, **Login**, **Password** details
- Test connection by clicking **Test** ( testing connection is disabled by default enable it by editing airflow.cfg file ), if everything works fine **Save** connection.
### Google Cloud Storage Setup
- Sign in or sign up to [GCP Console](https://console.cloud.google.com) enable free credits if available to avoid charges.
- From project drop down click **New Project**, give a name as `SpaceX-Project`. 
- From the **Navigation Menu** choose **Cloud Storage > Buckets** then click **CREATE**.
- Name your bucket as `spacex_data_lake` and follow the instructions on screen.
### BigQuery Setup
- From **Navigation Menu** choose **BigQuery**.
- Click on **view actions** ( three dots ) from project choose **Create Dataset**.
- Give unique name for dataset `SpaceX_Dataset`.
- Click on **Compose A New Query** and copy paste sql statement from `sql/BigQuery_spacex_schema.sql` Click **Run**.
- In order to enable airflow to communicate with Google Cloud we will use Airflow Connection
- From **Navigation Menu** go to **IAM and Admin** then select **Service Accounts**
- Click **Create Service Account** give it a name and assign **Editor** role and a new service account is created.
- After service account is created click on it and go to **Keys** tab click on **Add Keys > Create Keys** a private key is downloaded, keep it safe from unauthorized access. 
- login to **Airflow Web UI** from **Admin** tab select **Connections**
- Edit **google_cloud_default** connection, in the next page provide GCP **Project ID**, **Keyfile Path** absolute path where key is stored
- Test connection by clicking **Test** ( testing connection is disabled by default enable it by editing airflow.cfg file ),  if everything works fine **Save** the connection.
