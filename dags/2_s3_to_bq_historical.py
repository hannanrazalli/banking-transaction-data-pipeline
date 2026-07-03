import os
from datetime import datetime
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from include.ingestion.loaders.load_s3_bq import load_to_bq_batch

default_args = {
    "owner" : "Hannan_Razalli",
    "depends_on_past" : False,
    "retries" : 2
}

def _filter_keys(keys, suffix):
    return [k for k in keys if k.endswith(suffix)] if keys else []

def historical_tx_to_bq():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    keys = s3_hook.list_keys(
        bucket_name = bucket_name,
        prefix = "raw/transactions/"
    )

    filtered = _filter_keys(keys, ".parquet")
    if filtered:
        load_to_bq_batch(s3_keys=filtered, table_name='transactions')

def historical_acc_to_bq():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    keys = s3_hook.list_keys(
        bucket_name = bucket_name,
        prefix = "raw/accounts/"
    )

    filtered = _filter_keys(keys, ".parquet")
    if filtered:
        load_to_bq_batch(s3_keys=filtered, table_name='accounts')

def historical_fx_to_bq():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    keys = s3_hook.list_keys(
        bucket_name = bucket_name,
        prefix = "raw/forex/"
    )

    filtered = _filter_keys(keys, ".json")
    if filtered:
        load_to_bq_batch(s3_keys=filtered, table_name='forex')

with DAG(
    dag_id = '2_s3_to_bq_historical',
    default_args = default_args,
    schedule = None,
    start_date = datetime(2026, 6, 1),
    catchup = False,
    tags=["banking", "medallion"]
) as dag:
    
    task_tx_to_bq = PythonOperator(
        task_id = 'Historical_TX_to_BQ',
        python_callable = historical_tx_to_bq
    )

    task_acc_to_bq = PythonOperator(
        task_id = 'Historical_Acc_to_BQ',
        python_callable = historical_acc_to_bq
    )

    task_fx_to_bq = PythonOperator(
        task_id = 'Historical_FX_to_BQ',
        python_callable = historical_fx_to_bq
    )

    bq_raw_ready = Dataset("bigquery://banking_raw/data_ready")

    gatekeeper = EmptyOperator(
        task_id = 's3_to_bq_historical_complete',
        outlets = [bq_raw_ready]
    )

    [task_tx_to_bq, task_fx_to_bq] >> task_acc_to_bq >> gatekeeper