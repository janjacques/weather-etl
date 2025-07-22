from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import meteomatics.api as api
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import os

# Snowflake connections details
def get_snowflake_conn_params():
    return dict(
        user=os.environ.get('PROD_SNOWFLAKE_USER'),
        password=os.environ.get('PROD_SNOWFLAKE_PASSWORD'),
        account=os.environ.get('PROD_SNOWFLAKE_ACCOUNT'),
        database=os.environ.get('PROD_SNOWFLAKE_DATABASE'),
        schema=os.environ.get('PROD_SNOWFLAKE_SCHEMA'),
        warehouse=os.environ.get('PROD_SNOWFLAKE_WAREHOUSE'),
        role=os.environ.get('PROD_SNOWFLAKE_ROLE')
    )

def get_snowflake_connection():
    return snowflake.connector.connect(**get_snowflake_conn_params())

# Get location from Snowflake table
def get_locations_from_snowflake():
    
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        # Adjust this query to your table/column names as needed
        cur.execute("SELECT LOCATION_ID, LATITUDE, LONGITUDE FROM LOCATION_DIM")
        rows = cur.fetchall()
        # Returns list of tuples: [(id1, lat1, lon1), (id2, lat2, lon2), ...]
        return rows
    finally:
        conn.close()

# Load to Snowflake config
def load_to_snowflake(df):

    table_name = 'WEATHER_FACT'
    conn = get_snowflake_connection()

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
        'weather_symbol_1h:idx',
        'LOCATION_ID'
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

    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df_out,
            table_name=table_name,
            schema=os.environ.get('SNOWFLAKE_SCHEMA'),
            database=os.environ.get('SNOWFLAKE_DATABASE'),
            overwrite=False  # set to True to replace table
        )
        print(f"Loaded {nrows} rows to Snowflake table '{os.environ.get('SNOWFLAKE_SCHEMA')}.{table_name}' (success: {success})")
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

    # Get locations from Snowflake
    locations = get_locations_from_snowflake()
    print(f"Found {len(locations)} locations")

    # Fetch data from Meteomatics API
    all_dfs = []
    for loc_id, lat, lon in locations:
        coords = [(lat, lon)]
        try:
            df = api.query_time_series(
                coords,
                startdate,
                enddate,
                interval,
                parameters,
                username,
                password,
                model=model
            )
        except Exception as e:
            print(f"Error fetching data for location {loc_id} ({lat}, {lon}): {e}")
            continue

        df_reset = df.reset_index()  # lat/lon/date are now columns

        # Add the location id as a new column
        df_reset['LOCATION_ID'] = loc_id

        all_dfs.append(df_reset)

    # Combine all locations into one dataframe
    if not all_dfs:
        print("No data was fetched from Meteomatics.")
        return
    full_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Combined DataFrame shape: {full_df.shape}")

    load_to_snowflake(full_df)

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