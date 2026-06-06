import io
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd

from include.ingestion.api.daily_forex_api import daily_forex

default_args = {
    'owner': 'hannan_razalli',
    'depends_on_past': False,
    'retries': 2,
}

def extract_daily_banking_to_s3(execution_date, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    tables = ["accounts", "transactions"]
    
    for table in tables:
        if table == "accounts":
            query = f"SELECT * FROM accounts WHERE updated_at::text LIKE '{execution_date}%'"
        else:
            query = f"SELECT * FROM transactions WHERE transaction_date::text LIKE '{execution_date}%'"
            
        df = pg_hook.get_pandas_df(query)
        
        if not df.empty:
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            
            s3_key = f"raw/{table}/dt={execution_date}/{table}_{execution_date}.parquet"
            s3_hook.load_bytes(
                bytes_data=parquet_buffer.getvalue(),
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )

with DAG(
    dag_id = 'DB_to_S3_Daily',
    default_args=default_args,
    schedule_interval = '0 0 * * *',
    start_date = datetime(2026, 1, 1),
    catchup = False
) as dag:
    
    task_transactions = PythonOperator(
        task_id = 'Daily_transctions_to_S3',
        python_callable = extract_daily_banking_to_s3,
        op_kwargs = {
            'execution_date' : '{{ ds }}'
        }
    )

    task_forex = PythonOperator(
        task_id = 'Daily_Forex_to_S3',
        python_callable = daily_forex,
        op_kwargs = {
            'execution_date' : '{{ ds }}',
            'api_key' : os.getenv("FOREX_API_KEY"),
            'run_id' : '{{ run_id }}'
        }
    )

    task_transactions
    task_forex