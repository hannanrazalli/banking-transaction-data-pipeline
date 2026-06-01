import io
import os
import uuid
import logging
import numpy as np
import pandas as pd
import boto3

logger = logging.getLogger(__name__)

def daily_transactions(execution_date: str, run_id: str) -> str:
    num_records = 5000
    df = pd.DataFrame({
        "transaction_id": [str(uuid.uuid4()) for _ in range(num_records)],
        "account_id": np.random.randint(10000, 99999, num_records).astype(np.int32),
        "amount": np.round(np.random.uniform(10.0, 5000.0, num_records), 2).astype(np.float32),
        "currency": np.random.choice(["MYR", "USD", "SGD", "EUR"], num_records),
        "transaction_date": [f"{execution_date} 12:00:00"] * num_records,
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_records, p=[0.8, 0.15, 0.05])
    })

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, compression='snappy')
    parquet_buffer.seek(0)

    s3_client = boto3.client('s3')
    s3_key = f"raw/transactions/dt={execution_date}/run_{run_id}.parquet"
    
    s3_client.put_object(
        Bucket=os.getenv("S3_BUCKET_NAME"),
        Key=s3_key,
        Body=parquet_buffer.getvalue()
    )
    
    logger.info(f"Successfully fetch to S3: {s3_key}")
    return s3_key