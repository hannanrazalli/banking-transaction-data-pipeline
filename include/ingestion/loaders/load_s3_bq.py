import io
import os
import logging
import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

logger = logging.getLogger(__name__)

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
    try:
        if s3_key.endswith(".parquet"):
            parquet_bytes = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)
            with io.BytesIO(parquet_bytes.get()["Body"].read()) as buf:
                return pd.read_parquet(buf)
        else:
            json_string = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
            return pd.DataFrame([{"forex_data": json_string}])
    except Exception as e:
        logger.warning(f"Skipping bad file {s3_key}: {e}")
        return None

def load_to_bq(s3_key: str, table_name: str) -> None:
    s3_hook = S3Hook(aws_conn_id="aws_default")
    bq_hook = BigQueryHook(gcp_conn_id="gcp_default")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if bucket_name is None:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")

    table_config = BQ_TABLES.get(table_name)
    if not table_config:
        raise ValueError(f"Unknown table: {table_name}")

    df = _read_s3_file(s3_hook, bucket_name, s3_key)
    if df is None:
        logger.warning(f"Skipping {s3_key} — no data loaded")
        return

    df["_ingest_at"] = pd.Timestamp.now(tz="UTC")

    client = bq_hook.get_client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    if table_name == "forex":
        job_config.schema = [
            bigquery.SchemaField("forex_data", "STRING"),
            bigquery.SchemaField("_ingest_at", "TIMESTAMP"),
        ]

    job = client.load_table_from_dataframe(df, table_config["table_id"], job_config=job_config)
    try:
        job.result()
    except Exception as e:
        logger.error(f"BigQuery load job failed for {table_name}: {e}")
        raise


def load_to_bq_batch(s3_keys: list, table_name: str) -> None:
    if not s3_keys:
        return

    s3_hook = S3Hook(aws_conn_id="aws_default")
    bq_hook = BigQueryHook(gcp_conn_id="gcp_default")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if bucket_name is None:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")

    table_config = BQ_TABLES.get(table_name)
    if not table_config:
        raise ValueError(f"Unknown table: {table_name}")

    batch_size = 10
    for i in range(0, len(s3_keys), batch_size):
        batch = s3_keys[i:i + batch_size]
        frames = []
        for key in batch:
            df = _read_s3_file(s3_hook, bucket_name, key)
            if df is not None:
                frames.append(df)

        if not frames:
            logger.warning(f"No valid data in batch {i // batch_size}, skipping")
            continue

        combined = pd.concat(frames, ignore_index=True)
        combined["_ingest_at"] = pd.Timestamp.now(tz="UTC")

        client = bq_hook.get_client()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

        if table_name == "forex":
            job_config.schema = [
                bigquery.SchemaField("forex_data", "STRING"),
                bigquery.SchemaField("_ingest_at", "TIMESTAMP"),
            ]

        job = client.load_table_from_dataframe(combined, table_config["table_id"], job_config=job_config)
        try:
            job.result()
        except Exception as e:
            logger.error(f"BigQuery batch load job failed for {table_name}: {e}")
            raise
