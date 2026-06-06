"""
DAG: daily_to_s3
Description: 
    Extracts daily incremental banking transactions and forex data from external APIs 
    and loads them into AWS S3 (Raw/Landing Zone). Scheduled to run daily.
    Serves as the first step in the ELT pipeline.
"""

import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from include.ingestion.generators.daily_transactions import daily_transactions
from include.ingestion.api.daily_forex_api import daily_forex

default_args = {
    'owner': 'hannan_razalli',
    'depends_on_past': False,
    'retries': 2,
}

with DAG(
    dag_id = 'DB_to_S3_Daily',
    default_args=default_args,
    schedule = '0 0 * * *',
    start_date = datetime(2026, 1, 1),
    catchup = False
) as dag:
    
    task_transactions = PythonOperator(
        task_id = 'Daily_transctions_to_S3',
        python_callable = daily_transactions,
        op_kwargs = {
            'execution_date' : '{{ ds }}',
            'run_id' : '{{ run_id }}'
        }
    )

    task_forex = PythonOperator(
        task_id = 'Daily_Forex_to_S3',
        python_callable = daily_forex,
        op_kwargs = {
            'execution_date' : '{{ ds }}',
            'api_key' : os.getenv("FOREX_API_KEY"),
            'run_id' : '{{ run_id }}'
        }
    )