import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from include.ingestion.generators.daily_transactions import generate_and_stream_daily_transactions
from include.ingestion.api.daily_forex_api import fetch_and_stream_daily_forex

with DAG(
    dag_id='daily_to_s3',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['production', 'zero-disk'],
) as dag:

    # Task terus jana dan upload serentak (1 Task sahaja per entiti!)
    task_tx = PythonOperator(
        task_id='daily_transactions_to_s3',
        python_callable=generate_and_stream_daily_transactions,
        op_kwargs={'execution_date': '{{ ds }}', 'run_id': '{{ run_id }}'}
    )

    task_forex = PythonOperator(
        task_id='daily_forex_to_s3',
        python_callable=fetch_and_stream_daily_forex,
        op_kwargs={
            'execution_date': '{{ ds }}',
            'api_key': os.getenv("FOREX_API_KEY"),
            'run_id': '{{ run_id }}'
        }
    )