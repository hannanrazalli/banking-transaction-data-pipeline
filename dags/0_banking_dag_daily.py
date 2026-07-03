from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.ingestion.generators.postgres_daily import daily_to_postgres

default_args = {
    'owner': 'Hannan_Razalli',
    'depends_on_past': False,
    'retries': 2,
}

with DAG(
    dag_id='generate_daily_data',
    default_args = default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 6, 5),
    catchup=False,
    tags=['generate', 'postgres', 'daily']
) as dag:

    run_daily_generator = PythonOperator(
        task_id='Populate_Daily_Postgres',
        python_callable=daily_to_postgres,
        op_kwargs={'execution_date': '{{ ds }}'}
    )

    run_daily_generator