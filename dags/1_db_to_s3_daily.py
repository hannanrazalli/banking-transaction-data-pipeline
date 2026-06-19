import io
import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.ingestion.api.daily_forex_api import daily_forex

default_args = {
    'owner' : 'Hannan_Razalli',
    'depends_on_past' : False,
    'retries' : 2
}

POSTGRES_CONN = os.getenv("POSTGRES_CONN_ID", "postgres_default")
AWS_CONN = os.getenv("AWS_CONN_ID", "aws_default")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

def daily_banking(execution_date, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    s3_hook = S3Hook(aws_conn_id=AWS_CONN)

    tables = ["accounts", "transactions"]

    for table in tables:
        if table == 'accounts':
            query = f"SELECT * FROM accounts WHERE DATE(updated_at) = '{execution_date}'"
        else:
            query = f"SELECT * FROM transactions WHERE DATE(transaction_date) = '{execution_date}'"

        df = pg_hook.get_pandas_df(query)

        if not df.empty:
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)

            s3_key = f"raw/{table}/dt={execution_date}/{table}_{execution_date}.parquet"
            s3_hook.load_bytes(
                bytes_data = parquet_buffer.getvalue(),
                key = s3_key,
                bucket_name = BUCKET_NAME,
                replace = True
            )

with DAG(
    dag_id = '2_DB_to_S3_Daily',
    default_args = default_args,
    schedule = '@daily',
    start_date = datetime(2026, 5, 1),
    catchup = False
) as dag:
    
    task_fx_to_s3 = PythonOperator(
        task_id = 'FX_to_S3_Daily',
        python_callable = daily_forex,
        op_kwargs = {
            'execution_date' : '{{ ds }}',
            'api_key' : os.getenv("FOREX_API_KEY"),
            'run_id' : '{{ run_id }}'
        }
    )

    task_tx_to_s3 = PythonOperator(
        task_id = f'TX_to_S3_Daily',
        python_callable = daily_banking,
        op_kwargs = {
            'execution_date' : '{{ ds }}'
        }
    )

    task_tx_to_s3
    task_fx_to_s3