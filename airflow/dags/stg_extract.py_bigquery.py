from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import meteomatics.api as api
import pandas as pd
import os

# Meteomatics extraction
coordinates = [
    (52.37, 4.89),  # Amsterdam (example)
    # Add more tuples for more cities!
]
parameters = ['t_2m:C', 'precip_1h:mm', 'wind_speed_10m:ms']
model = 'mix'

def extract_from_meteomatics(**context):
    # Get credentials and config from environment variables
    username = os.environ.get("STG_METEOMATICS_USERNAME")
    password = os.environ.get("STG_METEOMATICS_PASSWORD")
    
    # Date range for extraction (last 24 hours as example, or adjust as needed)
    enddate = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    startdate = enddate - timedelta(days=1)
    interval = timedelta(hours=1)

    # Fetch data from Meteomatics API
    try:
        df = api.query_time_series(
            coordinates,
            startdate,
            enddate,
            interval,
            parameters,
            username,
            password,
            model=model
        )
    except Exception as e:
        print(f"Error fetching Meteomatics data: {e}")
        raise

    print(f"Fetched Meteomatics DataFrame shape: {df.shape}\n")

    return df.reset_index().to_json(orient="records")

def load_to_bigquery(**context):
    # Retrieve DataFrame dict from XCom
    df_json = context['ti'].xcom_pull(task_ids='extract_from_meteomatics')
    df = pd.read_json(df_json, orient="records")
    hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    client = hook.get_client()
    project = hook.project_id
    dataset = os.environ.get("BQ_DATASET")
    table = os.environ.get("BQ_TABLE")
    table_id = f"{project}.{dataset}.{table}"
    job = client.load_table_from_dataframe(df, table_id)  # Table and schema are inferred if not present
    job.result()  # Wait for the job to complete
    print(f"Loaded {job.output_rows} rows into {table_id}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='weather_to_bq',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['meteomatics', 'bigquery'],
) as dag:

    extract = PythonOperator(
        task_id='extract_from_meteomatics',
        python_callable=extract_from_meteomatics,
    )

    load = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
        provide_context=True,
    )

    extract >> load
