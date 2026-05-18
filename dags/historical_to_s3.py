from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from include.ingestion.generators.historical_transactions import generate_historical_transactions
from include.ingestion.api.historical_forex_api import fetch_historical_forex
from include.ingestion.loaders.upload_to_s3 import upload_to_s3

default_args = {
    'owner': 'tier1_data_engineer',
    'depends_on_past': False,
    'retries': 1,
}

START_DATE = '2023-01-01'
HISTORICAL_DATE_FOREX = '2023-01-01'

with DAG(
    dag_id='historical_banking_pipeline_s3',
    default_args=default_args,
    description='Production Tier-1 Historical Data Backfill Pipeline',
    schedule=None, 
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['production', 'banking', 'historical'],
) as dag:

    task_generate_historical_tx = PythonOperator(
        task_id='generate_historical_transactions',
        python_callable=generate_historical_transactions,
        op_kwargs={
            'start_date': START_DATE, 
            'end_date': "{{ macros.ds_add(ds, -1) }}"
        }
    )

    task_fetch_historical_forex = PythonOperator(
        task_id='fetch_historical_forex',
        python_callable=fetch_historical_forex,
        op_kwargs={'date': HISTORICAL_DATE_FOREX}
    )

    # SENIOR MOVE: Memuat naik folder berpartition terus ke bawah root 'raw/transactions'
    # Data harian & sejarah akan bergabung di dalam folder tunggal ini secara automatik!
    task_upload_hist_tx_s3 = PythonOperator(
        task_id='upload_hist_tx_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'local_path': "{{ ti.xcom_pull(task_ids='generate_historical_transactions') }}",
            's3_prefix': "raw/transactions"
        }
    )

    task_upload_hist_forex_s3 = PythonOperator(
        task_id='upload_hist_forex_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'local_path': "{{ ti.xcom_pull(task_ids='fetch_historical_forex') }}",
            's3_prefix': f"raw/forex/dt={HISTORICAL_DATE_FOREX}/run_historical_{{ run_id }}.json"
        }
    )

    task_generate_historical_tx >> task_upload_hist_tx_s3
    task_fetch_historical_forex >> task_upload_hist_forex_s3