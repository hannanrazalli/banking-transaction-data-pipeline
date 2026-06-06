from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.ingestion.generators.postgres_daily import daily_to_postgres

with DAG(
    dag_id='Postgres_Generator_Daily',
    schedule_interval='@daily',
    start_date=datetime(2026, 6, 1),
    catchup=True
) as dag:

    run_daily_generator = PythonOperator(
        task_id='Populate_Daily_Postgres',
        python_callable=daily_to_postgres,
        op_kwargs={'execution_date': '{{ ds }}'}
    )

    run_daily_generator