import io
import json
import os
import requests
import logging
import boto3

logger = logging.getLogger(__name__)

def fetch_and_stream_daily_forex(execution_date: str, api_key: str, run_id: str) -> None:
    """
    Tarik data API terus masuk RAM dan simpan ke S3 sebagai fail JSON.
    """
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"
    
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    data['ingestion_date'] = execution_date

    # Tukar JSON dict ke format string bytes di dalam RAM
    json_bytes = json.dumps(data).encode('utf-8')

    s3_client = boto3.client('s3')
    s3_key = f"raw/forex/dt={execution_date}/run_{run_id}.json"
    
    s3_client.put_object(
        Bucket=os.getenv("S3_BUCKET_NAME"),
        Key=s3_key,
        Body=json_bytes
    )
    logger.info(f"⚡ [RAM Stream] Forex harian berjaya di-stream ke S3.")