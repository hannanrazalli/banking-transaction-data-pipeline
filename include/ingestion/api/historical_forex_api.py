import os
import json
import time
import requests
import logging
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

def historical_forex(start_date: str, end_date: str, run_id: str):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    dates = pd.date_range(start=start_date, end=end_date)

    for dt in dates:
        date_str = dt.strftime('%Y-%m-%d')
        url = f"https://api.frankfurter.dev/v2/rates?date={date_str}&base=USD"

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            s3_key = f"raw/forex/dt={date_str}/run_{run_id}.json"
            s3_hook.load_string(
                string_data = json.dumps(data),
                key = s3_key,
                bucket_name = bucket_name,
                replace = True
            )
            logger.info(f"Successfully fetch data {date_str}")

        except Exception as e:
            logger.error(f"Failed to fetch data {date_str}: {e}")
            raise e
        
        time.sleep(1)