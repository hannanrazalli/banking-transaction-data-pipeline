"""
DAG: historical_s3_to_bq
Description: 
    One-off backfill DAG for historical data ingestion. 
    Reads bulk historical transaction and forex files from AWS S3 and loads them 
    physically into Google BigQuery native tables (Bronze layer) using WRITE_APPEND.
    Should be paused after the initial historical load is complete.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.ingestion.loaders.load_s3_bq import sync_tx_to_bq, sync_forex_to_bq

def bulk_load_historical_tx_from_s3():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix="raw/transactions/")
    
    if keys:
        for key in keys:
            if key.endswith('.parquet'):
                sync_tx_to_bq(s3_key=key)

def bulk_load_historical_forex_from_s3():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix="raw/forex/")
    
    if keys:
        for key in keys:
            if key.endswith('.json'):
                sync_forex_to_bq(s3_key=key)

with DAG(
    dag_id='historical_s3_to_bq',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['production', 'bigquery', 'backfill'],
) as dag:

    task_bulk_tx = PythonOperator(
        task_id='bulk_load_transactions_to_bq',
        python_callable=bulk_load_historical_tx_from_s3
    )

    task_bulk_forex = PythonOperator(
        task_id='bulk_load_forex_to_bq',
        python_callable=bulk_load_historical_forex_from_s3
    )

    task_bulk_tx
    task_bulk_forex