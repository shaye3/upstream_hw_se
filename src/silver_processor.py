import duckdb
import logging
from typing import Optional
from pathlib import Path
from datetime import datetime
import shutil


class SilverProcessor:
    
    def __init__(self, bronze_path: str = "data/bronze", silver_path: str = "data/silver"):
        self.bronze_path = Path(bronze_path)
        self.silver_path = Path(silver_path)
        self.silver_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def process_to_silver(self) -> str:
        try:
            conn = duckdb.connect()
            bronze_pattern = str(self.bronze_path / "vehicle_messages" / "**" / "*.parquet")
            
            self.logger.info("Starting Silver layer processing")
            
            # Check if bronze data exists
            bronze_check = conn.execute(f"""
                SELECT COUNT(*) as count 
                FROM read_parquet('{bronze_pattern}', filename=true, hive_partitioning=false)
            """).fetchone()
            
            if bronze_check[0] == 0:
                raise ValueError("No data found in Bronze layer")
                
            self.logger.info(f"Found {bronze_check[0]} records in Bronze layer")
            
            # Clean silver output folder before writing
            if self.silver_path.exists():
                self.logger.info(f"Cleaning silver output folder before write: {self.silver_path}")
                shutil.rmtree(self.silver_path, ignore_errors=True)
            self.silver_path.mkdir(parents=True, exist_ok=True)

            processing_timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            output_path = str(self.silver_path / f"vehicle_messages_cleaned_{processing_timestamp}.parquet")
            
            conn.execute(f"""
                COPY (
                    WITH cleaned_data AS (
                        SELECT 
                            vin,
                            TRIM(manufacturer) as manufacturer,
                            year,
                            model,
                            latitude,
                            longitude,
                            timestamp,
                            message_datetime,
                            velocity,
                            front_left_door_state,
                            wipers_state,
                            -- Convert gear positions to integers, handling special cases
                            CASE 
                                WHEN gear_position = 'NEUTRAL' THEN 0
                                WHEN gear_position = 'REVERSE' THEN -1
                                WHEN gear_position IS NULL THEN NULL
                                ELSE NULL
                            END as gear_position_numeric,
                            gear_position as gear_position_original,
                            driver_seatbelt_state,
                            fetch_timestamp,
                            ingestion_timestamp,
                            batch_id,
                            partition_date,
                            partition_hour,
                            '{processing_timestamp}' as silver_processing_timestamp
                        FROM read_parquet('{bronze_pattern}', filename=true, hive_partitioning=true)
                    )
                    SELECT * FROM cleaned_data
                    WHERE vin IS NOT NULL AND TRIM(vin) <> ''
                    ORDER BY message_datetime DESC
                ) TO '{output_path}' (FORMAT PARQUET)
            """)
            
            conn.close()
            
            self.logger.info("Silver processing completed.")
            self.logger.info(f"Silver data written to: {output_path}")
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"Failed to process silver data: {e}")
            raise
            
# if __name__ == "__main__":
#     processor = SilverProcessor(
#         bronze_path="data/bronze",
#         silver_path="data/silver"
#     )
#     processor.process_to_silver()
    
