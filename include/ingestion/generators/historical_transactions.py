import io
import uuid
import os
import logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import boto3

logger = logging.getLogger(__name__)

def historical_transactions(start_date: str, end_date: str, run_id: str) -> None:
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    days = (end - start).days + 1
    num_records = days * 1000

    random_days = np.random.randint(0, days, num_records)
    df = pd.DataFrame({
        "transaction_id": [str(uuid.uuid4()) for _ in range(num_records)],
        "account_id": np.random.randint(10000, 99999, num_records).astype(np.int32),
        "amount": np.round(np.random.uniform(10.0, 5000.0, num_records), 2).astype(np.float32),
        "currency": np.random.choice(["MYR", "USD", "SGD", "EUR"], num_records),
        "transaction_date": [start + timedelta(days=int(d)) for d in random_days],
        "status": np.random.choice(["COMPLETED", "FAILED"], num_records, p=[0.9, 0.1])
    })

    df["dt"] = df["transaction_date"].dt.strftime('%Y-%m-%d')
    df["transaction_date"] = df["transaction_date"].dt.strftime('%Y-%m-%d %H:%M:%S')

    s3_client = boto3.client('s3')
    bucket_name = os.getenv("S3_BUCKET_NAME")

    for dt, group in df.groupby('dt'):
        clean_group = group.drop(columns=['dt'])
        
        buffer = io.BytesIO()
        clean_group.to_parquet(buffer, index=False, compression='snappy')
        buffer.seek(0)

        s3_key = f"raw/transactions/dt={dt}/run_historical_{run_id}.parquet"
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())

    logger.info("Successfully fetch to S3!")