import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

from include.ingestion.generators.daily_transactions import generate_daily_transactions
from include.ingestion.api.daily_forex_api import fetch_daily_forex
from include.ingestion.loaders.upload_to_s3 import upload_to_s3

default_args = {
    'owner': 'tier1_data_engineer',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_banking_pipeline_s3',
    default_args=default_args,
    description='Production Tier-1 Daily Data Pipeline',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['production', 'banking', 'daily'],
) as dag:

    task_generate_tx = PythonOperator(
        task_id='generate_daily_transactions',
        python_callable=generate_daily_transactions,
        op_kwargs={'execution_date': '{{ ds }}'}
    )

    task_fetch_forex = PythonOperator(
        task_id='fetch_daily_forex',
        python_callable=fetch_daily_forex,
        op_kwargs={
            'execution_date': '{{ ds }}',
            'api_key': os.getenv("FOREX_API_KEY")
        }
    )

    # SENIOR MOVE: Guna run_id pada nama fail untuk jaminan idempotency (safe retry)
    task_upload_tx_s3 = PythonOperator(
        task_id='upload_tx_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'local_path': "{{ ti.xcom_pull(task_ids='generate_daily_transactions') }}",
            's3_prefix': "raw/transactions/dt={{ ds }}/run_{{ run_id }}.parquet"
        }
    )

    task_upload_forex_s3 = PythonOperator(
        task_id='upload_forex_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'local_path': "{{ ti.xcom_pull(task_ids='fetch_daily_forex') }}",
            's3_prefix': "raw/forex/dt={{ ds }}/run_{{ run_id }}.json"
        }
    )

    task_generate_tx >> task_upload_tx_s3
    task_fetch_forex >> task_upload_forex_s3