import os
import logging
from concurrent.futures import ThreadPoolExecutor  # Tambah ini untuk kelajuan industri
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def upload_to_s3(local_path: str, s3_prefix: str) -> str:
    """
    Tier 1 Loader PRO: Menggunakan Multithreading untuk memuat naik 
    beribu-ribu fail partition secara selari (Parallel Ingestion).
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Laluan tempatan tidak wujud: {local_path}")

    try:
        # JIKA FOLDER (Historical - Banyak Fail)
        if os.path.isdir(local_path):
            logger.info(f"Direktori dikesan. Mengumpul senarai fail untuk muat naik...")
            
            # Kumpul semua laluan fail dulu
            upload_tasks = []
            for root, _, files in os.walk(local_path):
                for file in files:
                    local_file = os.path.join(root, file)
                    rel_path = os.path.relpath(local_file, local_path)
                    s3_key = os.path.join(s3_prefix, rel_path).replace("\\", "/")
                    upload_tasks.append((local_file, s3_key))

            logger.info(f"Memulakan muat naik MULTI-THREADED untuk {len(upload_tasks)} fail serentak!")
            
            # SENIOR MOVE: Guna 10 pekerja (threads) untuk hantar 10 fail serentak
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [
                    executor.submit(s3_client.upload_file, local, bucket_name, s3)
                    for local, s3 in upload_tasks
                ]
                # Pastikan semua thread selesai tanpa ralat
                for future in futures:
                    future.result()

            logger.info(f"⚡ Sukses! {len(upload_tasks)} fail berjaya dimuat naik secara selari.")
            return f"s3://{bucket_name}/{s3_prefix}/"
        
        # JIKA FAIL TUNGGAL (Daily - Satu Fail Sahaja)
        else:
            s3_key = s3_prefix.replace("\\", "/")
            logger.info(f"Memuat naik fail tunggal ke s3://{bucket_name}/{s3_key}")
            s3_client.upload_file(local_path, bucket_name, s3_key)
            return f"s3://{bucket_name}/{s3_key}"

    except ClientError as e:
        logger.error(f"Ralat AWS S3 Client: {e}")
        raise
    except Exception as e:
        logger.error(f"Ralat muat naik S3 tidak dijangka: {e}")
        raise