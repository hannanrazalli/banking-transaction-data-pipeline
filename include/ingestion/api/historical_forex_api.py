import requests
import json
import os
import logging
import boto3

logger = logging.getLogger(__name__)

def fetch_and_stream_historical_forex(date: str, run_id: str) -> None:
    """
    Tarik data sejarah Frankfurter API terus ke RAM dan hantar ke S3.
    """
    url = f"https://api.frankfurter.app/{date}?from=USD"
    
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    
    json_bytes = json.dumps(response.json()).encode('utf-8')

    s3_client = boto3.client('s3')
    s3_key = f"raw/forex/dt={date}/run_historical_{run_id}.json"
    
    s3_client.put_object(Bucket=os.getenv("S3_BUCKET_NAME"), Key=s3_key, Body=json_bytes)
    logger.info(f"⚡ [RAM Stream] Forex sejarah ({date}) berjaya di-stream ke S3.")