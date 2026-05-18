import pandas as pd
import numpy as np
import uuid
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def generate_daily_transactions(execution_date: str) -> str:
    """
    Production Daily Generator: Jenis data ketat & optimasi memori (snappy compression).
    """
    try:
        dt_obj = datetime.strptime(execution_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Format execution_date wajib YYYY-MM-DD: {execution_date}")

    num_records = 5000
    
    df = pd.DataFrame({
        "transaction_id": [str(uuid.uuid4()) for _ in range(num_records)],
        "account_id": np.random.randint(10000, 99999, num_records).astype(np.int32),
        "amount": np.round(np.random.uniform(10.0, 5000.0, num_records), 2).astype(np.float32),
        "currency": np.random.choice(["MYR", "USD", "SGD", "EUR"], num_records),
        "transaction_date": [dt_obj.strftime('%Y-%m-%d %H:%M:%S')] * num_records,
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_records, p=[0.8, 0.15, 0.05])
    })

    # Enforce schemas & data compression efficiency
    df = df.astype({
        "transaction_id": "string",
        "currency": "category",
        "transaction_date": "datetime64[ns]",
        "status": "category"
    })

    file_path = f"/tmp/daily_tx_{dt_obj.strftime('%Y%m%d')}.parquet"
    df.to_parquet(file_path, index=False, compression='snappy')
    logger.info(f"Berjaya menjana data harian di {file_path}")
    return file_path