import pandas as pd
import duckdb
import logging
from typing import List, Dict
from datetime import datetime
import shutil
from pathlib import Path


class BronzeWriter:
    
    def __init__(self, bronze_path: str = "data/bronze"):
        self.bronze_path = Path(bronze_path)
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def write_raw_data(self, data: List[Dict], batch_id: str = None) -> str:
        if not data:
            raise ValueError("No data provided to write")
            
        try:
            df = pd.DataFrame(data)
            current_time = datetime.utcnow()
            df['ingestion_timestamp'] = current_time.isoformat()
            df['batch_id'] = batch_id or f"batch_{current_time.strftime('%Y%m%d_%H%M%S')}"
            df['message_datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df['partition_date'] = df['message_datetime'].dt.strftime('%Y-%m-%d')
            df['partition_hour'] = df['message_datetime'].dt.strftime('%H')
            
            conn = duckdb.connect()
            conn.register('raw_data', df)
            
            self.logger.info(f"Writing {len(df)} records to Bronze layer")
            
            # If data exists under output folder, remove it before writing
            vehicle_messages_dir = self.bronze_path / "vehicle_messages"
            if vehicle_messages_dir.exists():
                self.logger.info(f"Cleaning output folder before write: {vehicle_messages_dir}")
                shutil.rmtree(vehicle_messages_dir, ignore_errors=True)
            vehicle_messages_dir.mkdir(parents=True, exist_ok=True)
            output_dir = vehicle_messages_dir

            conn.execute(f"""
                COPY (
                    SELECT 
                        vin,
                        manufacturer,
                        year,
                        model,
                        latitude,
                        longitude,
                        timestamp,
                        message_datetime,
                        velocity,
                        frontLeftDoorState as front_left_door_state,
                        wipersState as wipers_state,
                        gearPosition as gear_position,
                        driverSeatbeltState as driver_seatbelt_state,
                        fetch_timestamp,
                        ingestion_timestamp,
                        batch_id,
                        partition_date,
                        partition_hour
                    FROM raw_data
                ) TO '{output_dir}' (FORMAT PARQUET, PARTITION_BY (partition_date, partition_hour))
            """)
            
            conn.close()
            
            self.logger.info(f"Successfully wrote partitioned data under {output_dir}")
            return str(output_dir)
            
        except Exception as e:
            self.logger.error(f"Failed to write bronze data: {e}")
            raise
            
    def list_bronze_files(self) -> List[str]:
        parquet_files = []
        bronze_dir = self.bronze_path / "vehicle_messages"
        
        if bronze_dir.exists():
            for parquet_file in bronze_dir.rglob("*.parquet"):
                parquet_files.append(str(parquet_file))
                
        self.logger.info(f"Found {len(parquet_files)} parquet files in Bronze layer")
        return parquet_files

# if __name__ == "__main__":
#     from api_client import APIClient
#     raw_data = APIClient().fetch_vehicle_messages(amount=10)
#     writer = BronzeWriter()
#     writer.write_raw_data(raw_data)