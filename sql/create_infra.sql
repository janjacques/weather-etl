CREATE WAREHOUSE IF NOT EXISTS airflow_wh
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS weather_etl;

CREATE SCHEMA IF NOT EXISTS weather_etl.staging;

-- Create Staging Role/User
CREATE ROLE IF NOT EXISTS airflow_stg_role;
CREATE USER IF NOT EXISTS airflow_stg_user
  PASSWORD = 'Bestseller2025'
  DEFAULT_WAREHOUSE = airflow_wh
  DEFAULT_ROLE = airflow_stg_role
  DEFAULT_NAMESPACE = weather_etl.staging
  MUST_CHANGE_PASSWORD = FALSE;

-- Grant Privileges to Staging Role
GRANT USAGE ON WAREHOUSE airflow_wh TO ROLE airflow_stg_role;
GRANT USAGE ON DATABASE weather_etl TO ROLE airflow_stg_role;
GRANT USAGE ON SCHEMA weather_etl.stg TO ROLE airflow_stg_role;
GRANT ALL PRIVILEGES ON SCHEMA weather_etl.staging TO ROLE airflow_stg_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA weather_etl.staging TO ROLE airflow_stg_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA weather_etl.staging TO ROLE airflow_stg_role;
GRANT ROLE airflow_stg_role TO USER airflow_stg_user;

CREATE TABLE weather_etl.staging.location_dim IF NOT EXISTS (
  location_id INTEGER AUTOINCREMENT PRIMARY KEY,
  stad VARCHAR,
  adres VARCHAR,
  latitude FLOAT,
  longitude FLOAT,
  land VARCHAR
);

INSERT INTO weather_etl.staging.location_dim (stad, adres, latitude, longitude, land) VALUES
  ('Amsterdam', 'Leidsestraat 77', 52.377956, 4.897070, 'Nederland'),
  ('Rotterdam', 'Lijnbaan 111', 51.919430, 4.477763, 'Nederland'),
  ('Den Haag', 'Spuistraat 71', 52.076000, 4.295000, 'Nederland'),
  ('Utrecht', 'Radboudkwartier 28', 52.090737, 5.121420, 'Nederland');

CREATE TABLE weather_etl.staging.weather_fact (
    validdate timestamp_ntz not null,
    lat float not nudelete ll,
    lon float not null,
    t_2m_c float,
    msl_pressure_hpa float,
    precip_1h_mm float,
    wind_speed_10m_ms float,
    wind_dir_10m_d float,
    weather_symbol_1h_idx integer
);