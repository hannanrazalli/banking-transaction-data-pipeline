import io
import os
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

def sync_tx_to_bq(s3_key: str) -> None:
    """Standard Pro: Pindahkan Parquet dari S3 ke BigQuery Raw Table."""
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bq_hook = BigQueryHook(gcp_conn_id='gcp_default')
    
    # 1. Sedut fail dari S3 terus ke RAM Dataframe
    file_obj = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)
    df = pd.read_parquet(io.BytesIO(file_obj.get()['Body'].read()))
    
    df['_ingest_at'] = pd.Timestamp.now(tz='UTC')
    
    # 2. Ambil Google SDK Client
    client = bq_hook.get_client()
    table_id = "banking_raw.raw_transactions"
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

def sync_forex_to_bq(s3_key: str) -> None:
    """Standard Pro: Pindahkan JSON mentah dari S3 ke Kolum STRING BigQuery."""
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bq_hook = BigQueryHook(gcp_conn_id='gcp_default')
    
    # 1. Baca string JSON dari S3
    json_string = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
    
    df = pd.DataFrame([{"forex_data": json_string, "_ingest_at": pd.Timestamp.now(tz='UTC')}])
    
    # 2. Ambil Google SDK Client
    client = bq_hook.get_client()
    table_id = "banking_raw.raw_forex"
    
    # FIX TERAKHIR: Tukar 'JSON' kepada 'STRING'. BigQuery API takkan mengamuk dah.
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("forex_data", "STRING"), 
            bigquery.SchemaField("_ingest_at", "TIMESTAMP")
        ]
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()