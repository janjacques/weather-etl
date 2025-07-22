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
  city VARCHAR,
  adsress VARCHAR,
  latitude FLOAT,
  longitude FLOAT,
  country VARCHAR
);

INSERT INTO weather_etl.staging.location_dim (city, `address`, latitude, longitude, country) VALUES
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

CREATE TABLE WEATHER_CONDITION_DIM (
    CONDITION_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    SYMBOL_CODE INTEGER,
    MAIN_CONDITION VARCHAR,
    DESCRIPTION VARCHAR
);

INSERT INTO WEATHER_CONDITION_DIM (SYMBOL_CODE, MAIN_CONDITION, DESCRIPTION) VALUES
  (1,  'Clear',              'Clear sky'),
  (2,  'Mainly Clear',       'Mainly clear'),
  (3,  'Partly Cloudy',      'Partly cloudy'),
  (4,  'Overcast',           'Overcast'),
  (5,  'Fog',                'Fog'),
  (6,  'Depositing Rime Fog','Depositing rime fog'),
  (7,  'Drizzle',            'Drizzle'),
  (8,  'Freezing Drizzle',   'Freezing drizzle'),
  (9,  'Rain',               'Rain'),
  (10, 'Freezing Rain',      'Freezing rain'),
  (11, 'Snow Fall',          'Snow fall'),
  (12, 'Snow Grains',        'Snow grains'),
  (13, 'Rain Showers',       'Rain showers'),
  (14, 'Snow Showers',       'Snow showers'),
  (15, 'Thunderstorm',       'Thunderstorm'),
  (16, 'Thunderstorm with Hail', 'Thunderstorm with hail'),
  (17, 'Isolated Rain Shower', 'Isolated rain shower'),
  (18, 'Scattered Rain Showers', 'Scattered rain showers'),
  (19, 'Heavy Rain Shower',     'Heavy rain shower'),
  (20, 'Isolated Snow Shower',  'Isolated snow shower'),
  (21, 'Scattered Snow Showers','Scattered snow showers'),
  (22, 'Heavy Snow Shower',     'Heavy snow shower'),
  (23, 'Isolated Thunderstorm', 'Isolated thunderstorm'),
  (24, 'Scattered Thunderstorms','Scattered thunderstorms'),
  (25, 'Heavy Thunderstorm',     'Heavy thunderstorm'),
  (26, 'Hail',                   'Hail'),
  (27, 'Unknown',                'Unknown weather type');

CREATE OR REPLACE TABLE DATE_DIM (
    DATE_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    DATE DATE,
    YEAR INTEGER,
    MONTH INTEGER,
    DAY_OF_MONTH INTEGER,
    DAY_OF_WEEK INTEGER,
    WEEK_OF_YEAR INTEGER,
    IS_WEEKEND BOOLEAN
);

INSERT INTO DATE_DIM (DATE, YEAR, MONTH, DAY_OF_MONTH, DAY_OF_WEEK, WEEK_OF_YEAR, IS_WEEKEND)
SELECT
    d,
    YEAR(d),
    MONTH(d),
    DAY(d),
    DAYOFWEEK(d),
    WEEK(d),
    CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN TRUE ELSE FALSE END
FROM
    (
        SELECT
            DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2025-01-01') AS d
        FROM TABLE(GENERATOR(ROWCOUNT => 366))
    )
WHERE d <= '2025-12-31';