import os
import json
import time
import requests
import pandas as pd
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

def daily_forex(execution_date: str, api_key: str, run_id: str):
    s3_hook = s3_hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        s3_key = f"raw/forex/dt={execution_date}/run_{run_id}.json"
        s3_hook.load_string(
            string_data = json.dumps(data),
            key = s3_key,
            bucket_name = bucket_name,
            replace = True
        )
        logging.info(f"Successfully fetch data {execution_date}")

    except Exception as e:
        logger.error(f"Failed to fetch data {execution_date}: {e}")
        raise e
    
    time.sleep(1)