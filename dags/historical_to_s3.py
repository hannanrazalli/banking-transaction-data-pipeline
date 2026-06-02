"""
DAG: historical_to_s3
Description: 
    One-off backfill DAG for data extraction.
    Pulls historical transaction and forex data spanning past periods from source APIs
    and lands the bulk files into AWS S3. 
    Should be paused after the initial historical load is complete.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from include.ingestion.generators.historical_transactions import historical_transactions
from include.ingestion.api.historical_forex_api import historical_forex

START_DATE = '2026-5-1'
END_DATE = 'macros.ds_add(ds, -1)'

with DAG(
    dag_id = 'Historical_to_S3',
    schedule = None,
    start_date = datetime(2026, 5, 1)
    catchup = False
) as dag:
    
    task_transactions = PythonOperator(
        task_id = 'Historical_transactions_to_S3',
        python_callable = historical_transactions,
        op_kwargs = {
            'start_date' : START_DATE,
            'end_date' : END_DATE,
            'run_id' : '{{ run_id }}'
        }
    )

    task_forex = PythonOperator(
        task_id = 'Historical_Forex_to_S3',
        python_callable = historical_forex,
        op_kwargs = {
            'start_date' : START_DATE,
            'end_date' : END_DATE,
            'run_id' : '{{ run_id }}'
        }
    )

    task_transactions
    task_forex