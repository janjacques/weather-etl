#FROM apache/airflow:slim-3.0.3-python3.12
FROM apache/airflow:2.9.1-python3.10

USER airflow

# Install extra Python dependencies
RUN pip install --no-cache-dir "protobuf>=3.20.3,<4.0.0" "proto-plus<2.0.0" pandas meteomatics apache-airflow-providers-google google-cloud-bigquery dbt-snowflake snowflake-connector-python[pandas]
