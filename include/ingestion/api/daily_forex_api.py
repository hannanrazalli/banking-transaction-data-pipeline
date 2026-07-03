import os
import json
import requests
import logging
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

def daily_forex(execution_date: str, api_key: str, run_id: str):
    aws_conn_id = os.getenv("AWS_CONN_ID", "aws_default")
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if bucket_name is None:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        s3_key = f"raw/forex/dt={execution_date}/run_{run_id}.json"
        s3_hook.load_string(
            string_data = json.dumps(data),
            key = s3_key,
            bucket_name = bucket_name,
            replace = True
        )
        logger.info(f"Successfully fetched data {execution_date}")
        
    except Exception as e:
        logger.error(f"Failed to fetch data {execution_date}: {e}")
        raise e