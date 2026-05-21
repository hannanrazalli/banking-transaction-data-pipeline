import os
import json
import time
import requests
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def fetch_and_stream_historical_forex(start_date: str, end_date: str, run_id: str):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    # Bina senarai tarikh (contoh: 30 hari)
    dates = pd.date_range(start=start_date, end=end_date)
    
    for dt in dates:
        date_str = dt.strftime('%Y-%m-%d')
        # Guna Frankfurter API untuk data sejarah
        url = f"https://api.frankfurter.app/{date_str}?from=USD"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Save setiap hari sebagai 1 fail JSON berasingan dalam folder tarikh masing-masing
            s3_key = f"raw/forex/dt={date_str}/run_{run_id}.json"
            s3_hook.load_string(
                string_data=json.dumps(data),
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            print(f"Berjaya sedut & hantar Forex untuk tarikh: {date_str}")
        except Exception as e:
            print(f"Gagal untuk tarikh {date_str}: {e}")
        
        # Jeda 1 saat supaya API Frankfurter tak anggap kita buat serangan DDoS
        time.sleep(1)