import os
import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

def daily_transactions(execution_date: str, run_id: str) -> str:
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    try:
        query_acc = f"""
            SELECT account_id, customer_name, account_status, account_tier, credit_limit, updated_at 
            FROM accounts 
            WHERE updated_at::date = '{execution_date}'::date
        """
        df_acc = pg_hook.get_pandas_df(sql=query_acc)
        
        if not df_acc.empty:
            s3_key_acc = f"raw/accounts/dt={execution_date}/run_{run_id}.parquet"
            s3_hook.load_bytes(
                bytes_data = df_acc.to_parquet(index=False, compression='snappy'),
                key = s3_key_acc,
                bucket_name = bucket_name,
                replace = True
            )
            logger.info(f"Successfully ingested daily accounts to S3 for {execution_date}")

        query_txn = f"""
            SELECT transaction_id, account_id, amount, currency, transaction_type, transaction_date 
            FROM transactions 
            WHERE transaction_date::date = '{execution_date}'::date
        """
        df_txn = pg_hook.get_pandas_df(sql=query_txn)
        
        if not df_txn.empty:
            s3_key_txn = f"raw/transactions/dt={execution_date}/run_{run_id}.parquet"
            s3_hook.load_bytes(
                bytes_data = df_txn.to_parquet(index=False, compression='snappy'),
                key = s3_key_txn,
                bucket_name = bucket_name,
                replace = True
            )
            logger.info(f"Successfully ingested daily transactions to S3 for {execution_date}")
            return s3_key_txn
        else:
            logger.info(f"No transactions found to ingest for date {execution_date}")
            return ""

    except Exception as e:
        logger.error(f"Failed to ingest daily data from Postgres to S3 for {execution_date}: {e}")
        raise e