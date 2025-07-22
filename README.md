# Weather ETL Pipeline

This project provides a reproducible ETL pipeline to extract weather data from a public API, store it in Snowflake, and transform it with dbt. The solution is built on Airflow, Docker, and dbt, with all code and configuration managed in this repository.

## Overview

- **Extraction:** Airflow DAG fetches weather data from the Meteomatics API, processes it for compatibility, and loads it into a Snowflake source table.
- **Transformation:** A separate Airflow DAG triggers dbt models that transform the raw data and materialize it into new Snowflake tables.
- **Reproducibility:** All dependencies are managed via Docker Compose. Airflow, dbt, and supporting services (PostgreSQL, etc.) run in containers.
- **Version Control:** All pipeline code, dbt models, and configuration templates are tracked in Git.

## Quick Start

### Prerequisites

- Meteomatics API access (either via paid or trial account)
- [Docker](https://www.docker.com/products/docker-desktop)
- [Docker Compose](https://docs.docker.com/compose/)
- A Snowflake account with required permissions

### Manual Steps

1. **Register with Meteomatics**

   - Go to [Meteomatics](https://www.meteomatics.com/en/api/try-api/) and register for free trial API access.
   - You will receive credentials to access their API.

2. **Fill Out the `.env` File**

   - Copy `.env.example` to `.env`.
   - Fill in your Meteomatics API credentials, Snowflake connection details, and any other required environment variables.

     ```bash
     cp .env.example .env
     # Edit .env with your environment-specific values
     ```

3. **Prepare Snowflake Infrastructure**

   - DDL scripts that must be executed are available in the `/sql` folder in the `create_infra.sql` file
   - Run the provided DDL queries in your Snowflake environment to set up the necessary database(s), schema(s), and tables for storing weather data.
   - Example .

### Running the Pipeline

1. **Build and Start Services**

   terminal: docker compose up --build

2. **Access Airflow UI**

    Navigate to http://localhost:8080

    Default login:
        user: admin
        password: admin

3. **Trigger DAGs**

    Start the extraction DAG to fetch and load data.

    Run the dbt transformation DAG as needed.

```
Project Structure
/
├── airflow/           # DAGs, plugins, Airflow config
├── dbt/               # dbt project (models, seeds, profiles)
├── sql/               # Snowflake DDL scripts
├── docker/            # Dockerfiles, compose configs
├── .env.example       # Template for required environment variables
└── README.md
```

## Local Testing with dbt

You can test and develop dbt models locally without running the full Docker/Airflow stack. Below are steps to set up a local Python virtual environment and configure dbt for Snowflake:

### 1. Create and Activate a Virtual Environment

From the root of your project:

```bash
python3 -m venv .venv
source .venv/bin/activate

### 2. Install dbt and Dependencies

```bash
pip install --upgrade pip
pip install dbt-snowflake

### 3. Configure dbt Profile for Snowflake

   - dbt requires a profiles.yml file (by default located in ~/.dbt/).
   - You can use the template provided in dbt/profiles.yml.example as a starting point.

```bash
mkdir -p ~/.dbt
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Snowflake credentials or use environment variables

### 4. Test the Connection
   - With your .venv activated and your profile configured, test the dbt connection:

```bash
dbt debug

### 5. Run dbt Models Locally
   - Still with your .venv activated, you can now build and test your models:

```bash
cd dbt
dbt run
dbt test

### Notes
   - Sensitive credentials should never be committed. Use .env only.

   - Manual Snowflake setup is required before running the pipeline for the first time.

   - The process is intended to be reproducible across environments with minimal manual steps.