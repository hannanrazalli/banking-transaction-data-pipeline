"""
DAG: s3_to_bq
Description: 
    Daily ingestion DAG. Moves incremental daily data from AWS S3 landing zone 
    into Google BigQuery (Bronze layer) native tables.
    Emits an Airflow Dataset ('bq_raw_ready') upon successful completion to 
    automatically trigger downstream dbt transformations (Medallion architecture).
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

from include.ingestion.loaders.load_s3_bq import load_transactions, load_accounts, load_forex

bq_raw_ready = Dataset("bigquery://banking_raw/data_ready")

default_args = {
    'owner': 'Hannan_Razalli',
    'depends_on_past': False,
    'retries': 2,
}

def daily_tx_to_bq(ds, **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    keys = s3_hook.list_keys(
        bucket_name = bucket_name,
        prefix = f"raw/transactions/dt={ds}/"
    )

    if keys:
        for key in keys:
            if key.endswith('.parquet'):
                load_transactions(s3_key=key)

def daily_acc_to_bq(ds, **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    keys = s3_hook.list_keys(
        bucket_name = bucket_name,
        prefix = f"raw/accounts/dt={ds}/"
    )

    if keys:
        for key in keys:
            if key.endswith('.parquet'):
                load_accounts(s3_key=key)

def daily_fx_to_bq(ds, **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    keys = s3_hook.list_keys(
        bucket_name = bucket_name,
        prefix = f"raw/forex/dt={ds}/"
    )

    if keys:
        for key in keys:
            if key.endswith('.json'):
                load_forex(s3_key=key)

with DAG(
    dag_id = '3_S3_to_BQ_Daily',
    default_args = default_args,
    schedule = '@daily',
    start_date = datetime(2025, 1, 1),
    catchup = False
) as dag:
    
    task_tx_daily = PythonOperator(
        task_id = 'load_daily_transactions_to_bq',
        python_callable = daily_tx_to_bq,
        outlets = [bq_raw_ready]
    )

    task_acc_daily = PythonOperator(
        task_id = 'load_daily_accounts_to_bq',
        python_callable = daily_acc_to_bq,
        outlets = [bq_raw_ready]
    )

    task_fx_daily = PythonOperator(
        task_id = 'load_daily_forex_to_bq',
        python_callable = daily_fx_to_bq,
        outlets = [bq_raw_ready]
    )

    task_tx_daily
    task_acc_daily
    task_fx_daily