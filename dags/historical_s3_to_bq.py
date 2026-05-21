import sys
import os
# Trik Senior: Paksa Python kenal folder root projek (/usr/local/airflow)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import boto3
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import fungsi dari loader utama
from include.ingestion.loaders.load_s3_bq import sync_tx_to_bq, sync_forex_to_bq

def bulk_load_historical_tx_from_s3():
    """Mengimbas keseluruhan prefix transaksi di S3 dan hantar ke BigQuery."""
    s3_client = boto3.client('s3')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix="raw/transactions/")
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # Tapis hanya fail Parquet
                if key.endswith('.parquet'):
                    sync_tx_to_bq(s3_key=key)

def bulk_load_historical_forex_from_s3():
    """Mengimbas keseluruhan prefix forex di S3 dan hantar ke BigQuery secara pukal."""
    s3_client = boto3.client('s3')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix="raw/forex/")
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # Tapis hanya fail JSON
                if key.endswith('.json'):
                    sync_forex_to_bq(s3_key=key)

with DAG(
    dag_id='historical_s3_to_bq',
    schedule=None,  # Larian manual sahaja
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['production', 'bigquery', 'backfill'],
) as dag:

    # Task 1: Sedut semua fail Transaksi (Parquet) dari S3 ke BigQuery
    task_bulk_tx = PythonOperator(
        task_id='bulk_load_transactions_to_bq',
        python_callable=bulk_load_historical_tx_from_s3
    )

    # Task 2: Sedut semua fail Forex (JSON) dari S3 ke BigQuery
    task_bulk_forex = PythonOperator(
        task_id='bulk_load_forex_to_bq',
        python_callable=bulk_load_historical_forex_from_s3
    )

    # Membenarkan kedua-dua task pukal berjalan secara serentak
    task_bulk_tx
    task_bulk_forex