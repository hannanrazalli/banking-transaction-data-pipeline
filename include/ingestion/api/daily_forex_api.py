import json
import requests
import logging

logger = logging.getLogger(__name__)

def fetch_daily_forex(execution_date: str, api_key: str) -> str:
    """
    Ekstrak data forex harian menggunakan dependency injection untuk api_key.
    """
    if not api_key or api_key == "None":
        raise ValueError("FOREX_API_KEY tidak dijumpai atau tidak sah.")

    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        data['ingestion_date'] = execution_date
        
        file_path = f"/tmp/daily_forex_{execution_date}.json"
        with open(file_path, 'w') as f:
            json.dump(data, f)
            
        return file_path
    except requests.exceptions.RequestException as e:
        logger.error(f"Gagal mengekstrak Daily Forex: {e}")
        raise