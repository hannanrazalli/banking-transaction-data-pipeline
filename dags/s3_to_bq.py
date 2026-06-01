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
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

# Settle: Import tepat ke nama fail baharu kau: load_s3_bq
from include.ingestion.loaders.load_s3_bq import sync_tx_to_bq, sync_forex_to_bq

bq_raw_ready = Dataset("bigquery://banking_raw/data_ready")

default_args = {
    'owner': 'hannan_razalli',
    'depends_on_past': False,
    'retries': 2,
}

with DAG(
    dag_id='s3_to_bq',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['production', 'bigquery', 'decoupled'],
) as dag:

    # Task 1: Sedut transaksi harian dari S3 masuk ke BigQuery
    task_load_daily_tx = PythonOperator(
        task_id='load_daily_transactions_to_bq',
        python_callable=sync_tx_to_bq,
        op_kwargs={'s3_key': 'raw/transactions/dt={{ ds }}/run_{{ run_id }}.parquet'},
        outlets=[bq_raw_ready] # MAGIS BERLAKU DI SINI
    )

    # Task 2: Sedut JSON forex harian dari S3 masuk ke BigQuery
    task_load_daily_forex = PythonOperator(
        task_id='load_daily_forex_to_bq',
        python_callable=sync_forex_to_bq,
        op_kwargs={'s3_key': 'raw/forex/dt={{ ds }}/run_{{ run_id }}.json'},
        outlets=[bq_raw_ready]
    )

    task_load_daily_tx
    task_load_daily_forex