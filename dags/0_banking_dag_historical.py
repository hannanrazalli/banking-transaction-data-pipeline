from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import datetime
from include.ingestion.generators.postgres_historical import historical_to_postgres

default_args = {
    'owner': 'Hannan_Razalli',
    'depends_on_past': False,
    'retries': 2,
}

with DAG(
    dag_id='0_generate_historical_data',
    default_args = default_args,
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["banking", "medallion"]
) as dag:

    run_historical_generator = PythonOperator(
        task_id='Populate_Historical_Postgres',
        python_callable=historical_to_postgres,
        op_kwargs={
            'start_date': os.getenv("HISTORICAL_START_DATE", "2026-06-01"),
            'end_date': os.getenv("HISTORICAL_END_DATE", "2026-07-01"),
            'postgres_conn_id': os.getenv("POSTGRES_CONN_ID", "postgres_default")
        }
    )

    run_historical_generator