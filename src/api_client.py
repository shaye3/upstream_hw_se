import requests
import logging
from typing import List, Dict, Optional
from datetime import datetime


class APIClient:
    
    def __init__(self, base_url: str = "http://localhost:9900", timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def fetch_vehicle_messages(self, amount: int = 10000) -> List[Dict]:
        url = f"{self.base_url}/upstream/vehicle_messages"
        params = {"amount": amount}
        
        self.logger.info(f"Fetching {amount} vehicle messages from {url}")
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                raise ValueError("Expected list of messages from API")
                
            self.logger.info(f"Successfully fetched {len(data)} messages")
            
            # Add fetch timestamp to each record
            fetch_timestamp = datetime.utcnow().isoformat()
            for record in data:
                record['fetch_timestamp'] = fetch_timestamp
            return data
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch data from API: {e}")
            raise
        except ValueError as e:
            self.logger.error(f"Invalid response data: {e}")
            raise
            
    def health_check(self) -> bool:
        try:
            # Try to fetch a small sample to test connectivity
            self.fetch_vehicle_messages(amount=1)
            return True
        except Exception as e:
            self.logger.warning(f"API health check failed: {e}")
            return False

# if __name__ == "__main__":
#     client = APIClient()
#     print(client.health_check())
#     print(client.fetch_vehicle_messages(amount=10))