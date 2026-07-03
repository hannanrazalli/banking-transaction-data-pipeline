"""
DAG: s3_to_bq
Description: 
    Daily ingestion DAG. Moves incremental daily data from AWS S3 landing zone 
    into Google BigQuery (Bronze layer) native tables.
    Emits an Airflow Dataset ('bq_raw_ready') upon successful completion to 
    automatically trigger downstream dbt transformations (Medallion architecture).
"""

import os
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

from include.ingestion.loaders.load_s3_bq import load_to_bq

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
                load_to_bq(s3_key=key, table_name='transactions')

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
                load_to_bq(s3_key=key, table_name='accounts')

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
                load_to_bq(s3_key=key, table_name='forex')

with DAG(
    dag_id = '2_s3_to_bq_daily',
    default_args = default_args,
    schedule = '@daily',
    start_date = datetime(2026, 6, 1),
    catchup = False,
    tags=["banking", "medallion"]
) as dag:
    
    task_tx_daily = PythonOperator(
        task_id = 'load_daily_transactions_to_bq',
        python_callable = daily_tx_to_bq,
        op_kwargs={'ds': '{{ ds }}'}
    )

    task_acc_daily = PythonOperator(
        task_id = 'load_daily_accounts_to_bq',
        python_callable = daily_acc_to_bq,
        op_kwargs={'ds': '{{ ds }}'}
    )

    task_fx_daily = PythonOperator(
        task_id = 'load_daily_forex_to_bq',
        python_callable = daily_fx_to_bq,
        op_kwargs={'ds': '{{ ds }}'}
    )

    gatekeeper = EmptyOperator(
        task_id = 's3_to_bq_complete',
        outlets = [bq_raw_ready]
    )

    [task_tx_daily, task_acc_daily, task_fx_daily] >> gatekeeper