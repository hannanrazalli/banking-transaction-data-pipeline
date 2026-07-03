import io
import os
import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

BQ_TABLES = {
    "transactions": {
        "table_id": "banking_raw.raw_transactions",
        "file_type": "parquet",
    },
    "accounts": {
        "table_id": "banking_raw.raw_accounts",
        "file_type": "parquet",
    },
    "forex": {
        "table_id": "banking_raw.raw_forex",
        "file_type": "json",
    },
}

def _read_s3_file(s3_hook, bucket_name, s3_key):
    if s3_key.endswith(".parquet"):
        parquet_bytes = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)
        return pd.read_parquet(io.BytesIO(parquet_bytes.get()["Body"].read()))
    else:
        json_string = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
        return pd.DataFrame([{"forex_data": json_string}])

def load_to_bq(s3_key: str, table_name: str) -> None:
    s3_hook = S3Hook(aws_conn_id="aws_default")
    bq_hook = BigQueryHook(gcp_conn_id="gcp_default")
    bucket_name = os.getenv("S3_BUCKET_NAME")

    table_config = BQ_TABLES.get(table_name)
    if not table_config:
        raise ValueError(f"Unknown table: {table_name}")

    df = _read_s3_file(s3_hook, bucket_name, s3_key)
    df["_ingest_at"] = pd.Timestamp.now(tz="UTC")

    client = bq_hook.get_client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    if table_name == "forex":
        job_config.schema = [
            bigquery.SchemaField("forex_data", "STRING"),
            bigquery.SchemaField("_ingest_at", "TIMESTAMP"),
        ]

    job = client.load_table_from_dataframe(df, table_config["table_id"], job_config=job_config)
    job.result()
