from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='stg_transform_dbt',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # or any schedule you want
    catchup=False,
    tags=['dbt', 'transform'],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_staging_run",
        bash_command="cd /opt/airflow/dbt/weather_dbt && dbt run --target staging",
        dag=dag,
    )
