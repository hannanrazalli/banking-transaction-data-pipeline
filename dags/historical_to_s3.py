from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from include.ingestion.generators.historical_transactions import generate_and_stream_historical_transactions
from include.ingestion.api.historical_forex_api import fetch_and_stream_historical_forex

# Kehendak baru: Data paling lama bermula dari 1 Jan 2025 sahaja
START_DATE = '2025-01-01'
HISTORICAL_DATE_FOREX = '2025-01-01'

with DAG(
    dag_id='historical_to_s3',
    schedule=None, 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['production', 'zero-disk', 'backfill'],
) as dag:

    task_hist_tx = PythonOperator(
        task_id='historical_transactions_to_s3',
        python_callable=generate_and_stream_historical_transactions,
        op_kwargs={
            'start_date': START_DATE, 
            'end_date': "{{ macros.ds_add(ds, -1) }}",
            'run_id': '{{ run_id }}'
        }
    )

    task_hist_forex = PythonOperator(
        task_id='historical_forex_to_s3',
        python_callable=fetch_and_stream_historical_forex,
        op_kwargs={'date': HISTORICAL_DATE_FOREX, 'run_id': '{{ run_id }}'}
    )