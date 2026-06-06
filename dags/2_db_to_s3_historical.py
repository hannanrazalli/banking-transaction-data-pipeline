import io
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd

from include.ingestion.api.historical_forex_api import historical_forex

default_args = {
    'owner' : 'hannan_razalli',
    'depends_on_past' : False,
    'retries' : 2
}

START_DATE = '2026-05-01'
END_DATE = '2026-06-4'

def extract_historical_banking_to_s3(start_date, end_date, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    tables = ["accounts", "transactions"]
    dates = pd.date_range(start=start_date, end=end_date)
    
    for dt in dates:
        date_str = dt.strftime('%Y-%m-%d')
        for table in tables:
            if table == "accounts":
                query = f"SELECT * FROM accounts WHERE updated_at::text LIKE '{date_str}%'"
            else:
                query = f"SELECT * FROM transactions WHERE transaction_date::text LIKE '{date_str}%'"
                
            df = pg_hook.get_pandas_df(query)
            
            if not df.empty:
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                parquet_buffer.seek(0)
                
                s3_key = f"raw/{table}/dt={date_str}/{table}_{date_str}.parquet"
                s3_hook.load_bytes(
                    bytes_data=parquet_buffer.getvalue(),
                    key=s3_key,
                    bucket_name=bucket_name,
                    replace=True
                )

with DAG(
    dag_id = 'DB_to_S3_Historical',
    default_args = default_args,
    schedule_interval = None,
    start_date = datetime(2026, 5, 1),
    catchup = False
) as dag:
    
    task_tx_to_s3 = PythonOperator(
        task_id = 'Historical_TX_to_S3',
        python_callable = extract_historical_banking_to_s3,
        op_kwargs = {
            'start_date' : START_DATE,
            'end_date' : END_DATE
        }
    )

    task_fx_to_s3 = PythonOperator(
        task_id = 'Historical_Forex_to_S3',
        python_callable = historical_forex,
        op_kwargs = {
            'start_date' : START_DATE,
            'end_date' : END_DATE,
            'run_id' : '{{ run_id }}'
        }
    )

    task_tx_to_s3
    task_fx_to_s3