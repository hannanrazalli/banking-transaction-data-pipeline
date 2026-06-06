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

default_args = {
    'owner' : 'hannan_razalli',
    'depends_on_past' : False,
    'retries' : 2
}

START_DATE = '2026-5-1'

with DAG(
    dag_id = 'DB_to_S3_Historical',
    default_args = default_args,
    schedule = None,
    start_date = datetime(2026, 5, 1),
    catchup = False
) as dag:
    
    task_tx_to_s3 = PythonOperator(
        task_id = 'Historical_TX_to_S3',
        python_callable = historical_transactions,
        op_kwargs = {
            'start_date' : START_DATE,
            'end_date' : '{{ macros.ds_add(ds, -2) }}',
            'run_id' : '{{ run_id }}'
        }
    )

    task_fx_to_s3 = PythonOperator(
        task_id = 'Historical_Forex_to_S3',
        python_callable = historical_forex,
        op_kwargs = {
            'start_date' : START_DATE,
            'end_date' : '{{ macros.ds_add(ds, -2) }}',
            'run_id' : '{{ run_id }}'
        }
    )

task_tx_to_s3
task_fx_to_s3