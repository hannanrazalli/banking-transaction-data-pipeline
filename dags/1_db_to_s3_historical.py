import io
import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.ingestion.api.historical_forex_api import historical_forex

default_args = {
    'owner' : 'Hannan_Razalli',
    'depends_on_past' : False,
    'retries' : 2
}

START_DATE = '2026-5-1'
END_DATE = '2026-6-9'
POSTGRES_CONN = os.getenv("POSTGRES_CONN_ID", "postgres_default")
AWS_CONN = os.getenv("AWS_CONN_ID", "aws_default")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

def historical_banking(date_str, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    s3_hook = S3Hook(aws_conn_id=AWS_CONN)

    tables = ["accounts", "transactions"]

    for table in tables:
        if table == 'accounts':
            query = f"SELECT * FROM accounts WHERE DATE(updated_at) = '{date_str}'"
        else:
            query = f"SELECT * FROM transactions WHERE DATE(transaction_date) = '{date_str}'"

        df = pg_hook.get_pandas_df(query)

        if not df.empty:
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)

            s3_key = f"raw/{table}/dt={date_str}/{table}_{date_str}.parquet"
            s3_hook.load_bytes(
                bytes_data = parquet_buffer.getvalue(),
                key = s3_key,
                bucket_name = BUCKET_NAME,
                replace = True
            )

with DAG(
    dag_id = '2_DB_to_S3_Historical',
    default_args = default_args,
    schedule = None,
    start_date = datetime(2026, 5, 1),
    catchup = False
) as dag:
    
    task_fx_to_s3 = PythonOperator(
        task_id = 'FX_to_S3_Historical',
        python_callable = historical_forex,
        op_kwargs = {
            'start_date' : START_DATE,
            'end_date' : END_DATE,
            'run_id' : '{{ run_id }}'
        }
    )

    historical_dates = pd.date_range(start=START_DATE, end=END_DATE)

    for dt in historical_dates:
        current_date_str = dt.strftime('%Y-%m-%d')

        task_tx_to_s3 = PythonOperator(
            task_id = f'TX_to_S3_Historical_{current_date_str}',
            python_callable = historical_banking,
            op_kwargs = {
                'date_str' : current_date_str
            }
        )

        task_tx_to_s3 >> task_fx_to_s3