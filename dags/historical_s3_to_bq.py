import sys
import os
# Trik Senior: Paksa Python kenal folder root projek (/usr/local/airflow)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import boto3
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Settle: Import tepat ke nama fail baharu kau: load_s3_bq
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
                if key.endswith('.parquet'):
                    sync_tx_to_bq(s3_key=key)

def load_historical_forex_snapshot():
    """Menyedut fail snapshot forex sejarah tunggal ke BigQuery."""
    s3_key = 'raw/forex/dt=2025-01-01/run_historical_manual__2026-05-19T01:37:19.689536+00:00.json'
    sync_forex_to_bq(s3_key=s3_key)

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

    task_snap_forex = PythonOperator(
        task_id='load_forex_snapshot_to_bq',
        python_callable=load_historical_forex_snapshot
    )

    task_bulk_tx
    task_snap_forex