import json
import requests
import logging

logger = logging.getLogger(__name__)

def fetch_historical_forex(date: str, api_key: str = None) -> str:
    """
    Menggunakan Frankfurter API (Bebas ralat 403 & tidak memerlukan API key).
    """
    url = f"https://api.frankfurter.app/{date}?from=USD"
    
    try:
        logger.info(f"Mengekstrak data sejarah dari Frankfurter: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        file_path = f"/tmp/historical_forex_{date}.json"
        with open(file_path, 'w') as f:
            json.dump(response.json(), f)
            
        return file_path
    except requests.exceptions.RequestException as e:
        logger.error(f"Gagal mengekstrak Historical Forex: {e}")
        raise