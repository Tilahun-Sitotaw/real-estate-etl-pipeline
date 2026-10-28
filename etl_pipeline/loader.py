"""
Data Loading Module for Real Estate ETL Pipeline.

Handles high-throughput batch insertion into PostgreSQL using psycopg2 execute_values,
transaction management, incremental vs. full-load handling, and load reporting.
"""

import time
from typing import List, Dict, Any, Optional
from datetime import datetime

from etl_pipeline.logger import get_logger
from etl_pipeline.database import DatabaseManager, DatabaseError

try:
    from psycopg2.extras import execute_values
except ImportError:
    execute_values = None


class LoaderError(Exception):
    """Raised when loading encounters a critical failure."""
    pass


class DataLoader:
    """
    Loads transformed property records into PostgreSQL in optimized batches.
    
    Supports:
    - Batch insert using psycopg2.extras.execute_values for optimal throughput
    - Conflict resolution (incremental DO NOTHING vs. full DO UPDATE)
    - Connection pooling and auto-commit batch boundaries
    - Per-batch and end-of-run loading statistics
    - Dry-run capability for testing without an active database
    """
    
    INSERT_QUERY_INCREMENTAL = """
    INSERT INTO properties (
        transaction_id, price, date_of_transfer, sale_year, sale_month,
        months_since_transaction, postcode, town_city, district, county,
        region, country, property_type, property_type_category, duration_type,
        is_freehold, is_new_build, address_line, square_meters,
        price_per_square_meter, latitude, longitude, data_quality_score,
        dedup_hash, metadata
    ) VALUES %s
    ON CONFLICT (transaction_id) DO NOTHING;
    """
    
    INSERT_QUERY_UPSERT = """
    INSERT INTO properties (
        transaction_id, price, date_of_transfer, sale_year, sale_month,
        months_since_transaction, postcode, town_city, district, county,
        region, country, property_type, property_type_category, duration_type,
        is_freehold, is_new_build, address_line, square_meters,
        price_per_square_meter, latitude, longitude, data_quality_score,
        dedup_hash, metadata
    ) VALUES %s
    ON CONFLICT (transaction_id) DO UPDATE SET
        price = EXCLUDED.price,
        date_of_transfer = EXCLUDED.date_of_transfer,
        sale_year = EXCLUDED.sale_year,
        sale_month = EXCLUDED.sale_month,
        months_since_transaction = EXCLUDED.months_since_transaction,
        postcode = EXCLUDED.postcode,
        town_city = EXCLUDED.town_city,
        district = EXCLUDED.district,
        county = EXCLUDED.county,
        region = EXCLUDED.region,
        country = EXCLUDED.country,
        property_type = EXCLUDED.property_type,
        property_type_category = EXCLUDED.property_type_category,
        duration_type = EXCLUDED.duration_type,
        is_freehold = EXCLUDED.is_freehold,
        is_new_build = EXCLUDED.is_new_build,
        address_line = EXCLUDED.address_line,
        square_meters = EXCLUDED.square_meters,
        price_per_square_meter = EXCLUDED.price_per_square_meter,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        data_quality_score = EXCLUDED.data_quality_score,
        metadata = EXCLUDED.metadata,
        updated_at = CURRENT_TIMESTAMP;
    """
    
    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        batch_size: int = 1000,
        mode: str = 'full_load',
        dry_run: bool = False
    ):
        """
        Initialize the DataLoader.
        
        Args:
            db_manager: DatabaseManager instance for PostgreSQL
            batch_size: Number of records to insert per SQL statement
            mode: 'full_load' (upsert) or 'incremental_load' (skip existing)
            dry_run: If True, simulates loading without writing to PostgreSQL
        """
        self.logger = get_logger()
        self.db_manager = db_manager
        self.batch_size = batch_size
        self.mode = mode
        self.dry_run = dry_run
        
        self.total_submitted = 0
        self.total_inserted = 0
        self.total_errors = 0
        self.total_batches = 0
        self.load_start_time: Optional[datetime] = None
        
    def _prepare_record_tuple(self, record: Dict[str, Any]) -> tuple:
        """Convert a record dictionary into a tuple matching SQL parameter order."""
        return (
            record['transaction_id'],
            record['price'],
            record['date_of_transfer'],
            record['sale_year'],
            record['sale_month'],
            record['months_since_transaction'],
            record['postcode'],
            record['town_city'],
            record['district'],
            record['county'],
            record['region'],
            record['country'],
            record['property_type'],
            record['property_type_category'],
            record['duration_type'],
            record['is_freehold'],
            record['is_new_build'],
            record['address_line'],
            record['square_meters'],
            record['price_per_square_meter'],
            record['latitude'],
            record['longitude'],
            record['data_quality_score'],
            record['dedup_hash'],
            record['metadata']
        )

    def load_chunk(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Load a batch or chunk of cleaned records into PostgreSQL.
        
        Args:
            records: Cleaned property record dictionaries
            
        Returns:
            Dictionary with chunk load statistics
        """
        if self.load_start_time is None:
            self.load_start_time = datetime.now()
            
        if not records:
            return {'records_loaded': 0, 'errors': 0, 'duration_seconds': 0.0}
            
        start_ts = time.time()
        self.total_submitted += len(records)
        chunk_errors = 0
        chunk_inserted = 0
        
        if self.dry_run or not self.db_manager:
            # Simulated load for dry-run or mock environments
            chunk_inserted = len(records)
            self.total_inserted += chunk_inserted
            self.total_batches += 1
            elapsed = time.time() - start_ts
            return {
                'records_loaded': chunk_inserted,
                'errors': 0,
                'duration_seconds': round(elapsed, 4)
            }

        conn = self.db_manager.get_connection()
        query = self.INSERT_QUERY_UPSERT if self.mode == 'full_load' else self.INSERT_QUERY_INCREMENTAL
        
        # Sub-divide chunk into batch sizes for optimal database network throughput
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            batch_tuples = [self._prepare_record_tuple(r) for r in batch]
            
            try:
                with conn.cursor() as cur:
                    if execute_values:
                        execute_values(cur, query, batch_tuples, page_size=len(batch_tuples))
                    else:
                        # Fallback for psycopg3 or environments without psycopg2.extras
                        cur.executemany(query.replace('%s', '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'), batch_tuples)
                conn.commit()
                chunk_inserted += len(batch)
                self.total_batches += 1
            except Exception as e:
                conn.rollback()
                self.logger.warning(f"Batch insertion failed, attempting single fallback inserts: {e}")
                
                # Resilient fallback: attempt individual inserts to isolate bad records
                for single_record in batch:
                    try:
                        single_tuple = [self._prepare_record_tuple(single_record)]
                        with conn.cursor() as cur:
                            execute_values(cur, query, single_tuple)
                        conn.commit()
                        chunk_inserted += 1
                    except Exception as single_err:
                        conn.rollback()
                        chunk_errors += 1
                        self.logger.warning(
                            f"Failed to load record {single_record.get('transaction_id')}: {single_err}"
                        )
                        
        self.total_inserted += chunk_inserted
        self.total_errors += chunk_errors
        elapsed = time.time() - start_ts
        
        return {
            'records_loaded': chunk_inserted,
            'errors': chunk_errors,
            'duration_seconds': round(elapsed, 4)
        }

    def get_load_summary(self) -> Dict[str, Any]:
        """Produce final summary of the loading phase."""
        end_time = datetime.now()
        start = self.load_start_time or end_time
        total_seconds = (end_time - start).total_seconds()
        
        return {
            'mode': self.mode,
            'dry_run': self.dry_run,
            'total_submitted': self.total_submitted,
            'total_inserted_or_updated': self.total_inserted,
            'total_errors': self.total_errors,
            'total_batches_executed': self.total_batches,
            'total_load_time_seconds': round(total_seconds, 2),
            'records_per_second': round(self.total_inserted / total_seconds, 2) if total_seconds > 0 else 0.0,
            'load_start_time': start.isoformat(),
            'load_end_time': end_time.isoformat()
        }
