from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import datetime
from include.ingestion.generators.postgres_daily import daily_to_postgres

default_args = {
    'owner': 'Hannan_Razalli',
    'depends_on_past': False,
    'retries': 2,
}

with DAG(
    dag_id='0_generate_daily_data',
    default_args = default_args,
    schedule='@daily',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["banking", "medallion"]
) as dag:

    run_daily_generator = PythonOperator(
        task_id='Populate_Daily_Postgres',
        python_callable=daily_to_postgres,
        op_kwargs={'execution_date': '{{ ds }}', 'postgres_conn_id': os.getenv("POSTGRES_CONN_ID", "postgres_default")}
    )

    run_daily_generator