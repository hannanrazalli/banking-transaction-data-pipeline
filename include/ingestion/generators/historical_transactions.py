import os
import shutil
import logging
import uuid
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

def generate_historical_transactions(start_date: str, end_date: str) -> str:
    """
    Tier 1 Historical Generator: Memecahkan dataset besar kepada folder partition Hive 
    secara lokal sebelum dimuat naik ke S3.
    
    Fix: Menambah parameter max_partitions=2000 untuk mengatasi had lalai (1024) PyArrow
         apabila memproses julat tarikh melebihi 3 tahun.
    """
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Format tarikh wajib YYYY-MM-DD.")

    if start > end:
        raise ValueError("start_date mesti sebelum atau sama dengan end_date.")

    # Kira jumlah hari untuk backfill
    days = (end - start).days + 1
    num_records = days * 1000  # Skala: 1000 transaksi sehari

    # Jana tarikh rawak dalam julat masa tersebut
    random_days = np.random.randint(0, days, num_records)
    dates_list = [start + timedelta(days=int(d)) for d in random_days]

    # Cipta DataFrame dengan skema perbankan yang hampir realistik
    df = pd.DataFrame({
        "transaction_id": [str(uuid.uuid4()) for _ in range(num_records)],
        "account_id": np.random.randint(10000, 99999, num_records).astype(np.int32),
        "amount": np.round(np.random.uniform(10.0, 5000.0, num_records), 2).astype(np.float32),
        "currency": np.random.choice(["MYR", "USD", "SGD", "EUR"], num_records),
        "transaction_date": [d.strftime('%Y-%m-%d %H:%M:%S') for d in dates_list],
        "status": np.random.choice(["COMPLETED", "FAILED"], num_records, p=[0.9, 0.1])
    })

    # Tukar kepada jenis data yang tepat (Optimization & Data Integrity)
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    
    # Cipta kolum fizikal 'dt' untuk proses Hive Partitioning
    df["dt"] = df["transaction_date"].dt.strftime('%Y-%m-%d')
    
    df = df.astype({
        "transaction_id": "string", 
        "currency": "category", 
        "status": "category"
    })

    # Sediakan folder output lokal temporari
    output_dir = "/tmp/historical_raw_dataset"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    # Pro Move: Menulis data mengikut struktur folder /dt=YYYY-MM-DD/
    # Snappy compression memastikan saiz storan di S3 nanti 70% lebih kecil & jimat kos
    df.to_parquet(
        output_dir, 
        partition_cols=['dt'], 
        index=False, 
        compression='snappy',
        max_partitions=2000  # Mengatasi error limit 1024 PyArrow
    )
    
    logger.info(f"Data sejarah berjaya di-partition lokal di {output_dir} (Total Days: {days})")
    return output_dir