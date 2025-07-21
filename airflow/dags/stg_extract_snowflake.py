from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import meteomatics.api as api
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os

# Load to Snowflake config
def load_to_snowflake(df):

    user = os.environ.get('SNOWFLAKE_USER')
    password = os.environ.get('SNOWFLAKE_PASSWORD')
    account = os.environ.get('SNOWFLAKE_ACCOUNT')
    database = os.environ.get('SNOWFLAKE_DATABASE')
    schema = os.environ.get('SNOWFLAKE_SCHEMA')
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
    role = os.environ.get('SNOWFLAKE_ROLE')
    table_name = 'WEATHER_FACT'

    # Reset Meteomatics DF index to columns for access to lat, lon, validdate 
    df_reset = df.reset_index()

    # Select only the columns we need
    sf_columns = [
        'validdate',
        'lat',
        'lon',
        't_2m:C',
        'msl_pressure:hPa',
        'precip_1h:mm',
        'wind_speed_10m:ms',
        'wind_dir_10m:d',
        'weather_symbol_1h:idx'
    ]

    df_out = df_reset[sf_columns]

    # Make parameter columns compatible with Snowflake
    column_mapping = {
        't_2m:C': 'T_2M_C',
        'msl_pressure:hPa': 'MSL_PRESSURE_HPA',
        'precip_1h:mm': 'PRECIP_1H_MM',
        'wind_speed_10m:ms': 'WIND_SPEED_10M_MS',
        'wind_dir_10m:d': 'WIND_DIR_10M_D',
        'weather_symbol_1h:idx': 'WEATHER_SYMBOL_1H_IDX'
    }

    df_out.rename(columns=column_mapping, inplace=True)

    # Convert all column names to uppercase to match Snowflake
    df_out.columns = [col.upper() for col in df_out.columns]

    # Connect and upload
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role,
    )

    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df_out,
            table_name=table_name,
            schema=schema,
            database=database,
            overwrite=False  # set to True to replace table
        )
        print(f"Loaded {nrows} rows to Snowflake table '{schema}.{table_name}' (success: {success})")
    except Exception as e:
        print(f"Error loading to Snowflake: {e}")
        raise
    finally:
        conn.close()

# Meteomatics extraction
coordinates = [
    (52.37, 4.89),  # Amsterdam (example)
]

parameters = ['t_2m:C', 'msl_pressure:hPa', 'precip_1h:mm', 'wind_speed_10m:ms', 'wind_dir_10m:d', 'weather_symbol_1h:idx']
model = 'mix'        

def extract_and_load(**context):
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

    load_to_snowflake(df)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='weather_to_sf',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['meteomatics', 'snowflake'],
) as dag:

    load = PythonOperator(
        task_id='extract_and_load_weather_data',
        python_callable=extract_and_load,
        provide_context=True,
    )