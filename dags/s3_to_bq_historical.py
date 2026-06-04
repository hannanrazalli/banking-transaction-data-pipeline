import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from include.ingestion.loaders.load_s3_bq import load_transactions, load_forex

default_args = {
    'owner': 'hannan_razalli',
    'depends_on_past': False,
    'retries': 2,
}

def historical_tx_to_bq():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    keys = s3_hook.list_keys(
        bucket_name = bucket_name,
        prefix = 'raw/transactions/'
    )

    if keys:
        for key in keys:
            if key.endswith('.parquet'):
                load_transactions(s3_key=key)

def historical_fx_to_bq():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    keys = s3_hook.list_keys(
        bucket_name = bucket_name,
        prefix = 'raw/forex/'
    )

    if keys:
        for key in keys:
            if key.endswith('.json'):
                load_forex(s3_key=key)

with DAG(
    dag_id = 'S3_to_BQ_Historical',
    default_args=default_args,
    schedule = None,
    start_date = datetime(2026, 5, 1),
    catchup = False
) as dag:
    
    task_hist_transactions = PythonOperator(
        task_id = 'Historical_TX_to_BQ',
        python_callable = historical_tx_to_bq
    )

    task_hist_forex = PythonOperator(
        task_id = 'Historical_Forex_to_BQ',
        python_callable = historical_fx_to_bq
    )

    task_hist_transactions
    task_hist_forex