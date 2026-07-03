"""
DAG: medallion_banking
Description: 
    Data-aware transformation DAG powered by Astronomer Cosmos and dbt.
    Triggered automatically by Airflow Datasets when the Bronze layer is updated.
    Executes the Medallion Architecture data modeling (Staging -> Intermediate -> Marts).
    Utilizes incremental materializations in production and full-refreshes in dev environments.
"""

import os
from datetime import datetime
from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from airflow.datasets import Dataset

GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "banking-data-496700")
GCP_DATASET = os.getenv("GCP_DATASET_BRONZE", "bronze")
GCP_LOCATION = os.getenv("GCP_LOCATION", "asia-southeast1")
IS_PROD = os.getenv("IS_PRODUCTION", "False").lower() == "true"
DBT_PROJECT_PATH = Path(os.getenv("AIRFLOW_HOME", "/usr/local/airflow")) /"include/dbt/banking_data_pipeline"

bq_raw_ready = Dataset("bigquery://banking_raw/data_ready")

profile_config = ProfileConfig(
    profile_name="banking_data_pipeline",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="gcp_default",
        profile_args={
            "project" : GCP_PROJECT,
            "dataset" : GCP_DATASET,
            "location" : GCP_LOCATION,
        },
    ),
)

dbtdag = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    operator_args={
        "install_deps" : True,
        "full_refresh" : not IS_PROD,
    },
    profile_config=profile_config,
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        dbt_deps=True
    ),
    schedule=[bq_raw_ready],
    start_date=datetime(2026, 5, 1),
    catchup=False,
    dag_id="3_medallion_banking",
    tags=['transform', 'dbt', 'medallion'],
    default_args={
        "owner" : "Hannan_Razalli",
        "retries" : 2
    }
)