"""
Data Extraction Module for Real Estate ETL Pipeline.

Handles streaming CSV files in configurable chunks using Pandas iterator (with a
resilient built-in streaming fallback) to support datasets with 2.5M+ records
without excessive memory consumption.
"""

import os
import time
import csv
from pathlib import Path
from typing import Iterator, Dict, List, Any, Tuple, Optional
from datetime import datetime

try:
    import pandas as pd
except (ImportError, Exception):
    pd = None

try:
    import psutil
except ImportError:
    psutil = None

from etl_pipeline.logger import get_logger


class ExtractionError(Exception):
    """Raised when extraction encounters a fatal error."""
    pass


class DataExtractor:
    """
    Extracts property records from CSV files in configurable chunks.
    
    Streams large CSV files (>2.5 million records) using chunked iterators,
    tracks memory and throughput, and standardizes column aliases across both
    UK Land Registry datasets and generalized property valuation files.
    """
    
    COLUMN_ALIASES = {
        'transaction_id': ['transaction_id', 'transaction_unique_identifier', 'property_id', 'id', 'record_id'],
        'price': ['price', 'sale_price', 'transaction_price', 'amount'],
        'date_of_transfer': ['date_of_transfer', 'sale_date', 'date', 'transfer_date', 'transaction_date'],
        'postcode': ['postcode', 'location_code', 'postal_code', 'zip', 'zip_code'],
        'town_city': ['town_city', 'city', 'town', 'locality', 'municipality'],
        'district': ['district', 'local_authority', 'borough', 'county_district'],
        'county': ['county', 'province', 'state'],
        'property_type': ['property_type_full', 'property_type', 'type', 'category'],
        'duration': ['duration_full', 'duration', 'tenure', 'seller_type'],
        'is_new_build': ['is_new_build', 'old_new', 'new_build', 'is_new'],
        'address': ['address', 'street', 'paon', 'full_address'],
        'latitude': ['latitude', 'lat'],
        'longitude': ['longitude', 'lon', 'lng'],
        'square_meters': ['square_meters', 'sqm', 'floor_area', 'living_area']
    }
    
    def __init__(self, file_path: str, chunk_size: int = 50000):
        self.logger = get_logger()
        self.file_path = Path(file_path)
        self.chunk_size = chunk_size
        
        if not self.file_path.exists():
            raise ExtractionError(f"CSV file not found: {self.file_path}")
            
        if chunk_size <= 0:
            raise ExtractionError(f"Invalid chunk_size: {chunk_size}. Must be greater than 0")
            
        self.total_records_read = 0
        self.total_errors = 0
        self.total_chunks = 0
        self.extraction_start_time: Optional[datetime] = None
        
        if psutil:
            try:
                self.process = psutil.Process(os.getpid())
                self.initial_memory_mb = self._get_memory_usage_mb()
            except Exception:
                self.process = None
                self.initial_memory_mb = 0.0
        else:
            self.process = None
            self.initial_memory_mb = 0.0
            
        self.resolved_column_map: Dict[str, str] = {}
        
    def _get_memory_usage_mb(self) -> float:
        if not self.process:
            return 0.0
        try:
            return float(self.process.memory_info().rss / (1024 * 1024))
        except Exception:
            return 0.0
            
    def _detect_and_map_columns(self, header_columns: List[str]) -> Dict[str, str]:
        lower_cols = {str(c).strip().lower(): str(c).strip() for c in header_columns}
        canonical_to_actual = {}
        
        for canonical, aliases in self.COLUMN_ALIASES.items():
            for alias in aliases:
                if alias.lower() in lower_cols:
                    canonical_to_actual[canonical] = lower_cols[alias.lower()]
                    break
                    
        has_id = 'transaction_id' in canonical_to_actual
        has_price = 'price' in canonical_to_actual
        
        if not (has_id and (has_price or 'address' in canonical_to_actual)):
            raise ExtractionError(
                f"CSV is missing required fields. Found columns: {list(header_columns)}"
            )
            
        return canonical_to_actual

    def extract_chunks(self) -> Iterator[Tuple[List[Dict[str, Any]], Dict[str, Any]]]:
        """Extract property records in stream chunks."""
        self.extraction_start_time = datetime.now()
        
        if pd is not None:
            try:
                yield from self._extract_chunks_pandas()
                return
            except Exception as e:
                self.logger.warning(
                    f"Pandas processing encountered an issue ({e}). "
                    "Seamlessly falling back to native high-speed streaming CSV extractor."
                )
                
        yield from self._extract_chunks_native_csv()

    def _extract_chunks_pandas(self) -> Iterator[Tuple[List[Dict[str, Any]], Dict[str, Any]]]:
        csv_iterator = pd.read_csv(
            self.file_path,
            chunksize=self.chunk_size,
            dtype=str,
            on_bad_lines='skip',
            low_memory=False,
            encoding='utf-8',
            encoding_errors='replace'
        )
        
        for chunk_idx, chunk_df in enumerate(csv_iterator, 1):
            chunk_start_ts = time.time()
            
            if chunk_idx == 1:
                self.resolved_column_map = self._detect_and_map_columns(list(chunk_df.columns))
                self.logger.info(f"Initialized extraction with column mappings: {self.resolved_column_map}")
                
            mapped_df = pd.DataFrame(index=chunk_df.index)
            for canonical, actual_col in self.resolved_column_map.items():
                if actual_col in chunk_df.columns:
                    mapped_df[canonical] = chunk_df[actual_col]
                    mapped_df[actual_col] = chunk_df[actual_col]
                    
            id_col = self.resolved_column_map.get('transaction_id')
            price_col = self.resolved_column_map.get('price')
            
            is_valid_mask = pd.Series(True, index=chunk_df.index)
            if id_col and id_col in chunk_df.columns:
                is_valid_mask &= chunk_df[id_col].notna() & (chunk_df[id_col].astype(str).str.strip() != '')
            if price_col and price_col in chunk_df.columns:
                is_valid_mask &= chunk_df[price_col].notna() & (chunk_df[price_col].astype(str).str.strip() != '')
                
            valid_df = mapped_df[is_valid_mask]
            chunk_errors = int((~is_valid_mask).sum())
            chunk_records = valid_df.to_dict(orient='records')
            
            self.total_records_read += len(chunk_records)
            self.total_errors += chunk_errors
            self.total_chunks += 1
            
            chunk_elapsed = time.time() - chunk_start_ts
            current_mem = self._get_memory_usage_mb()
            mem_delta = current_mem - self.initial_memory_mb
            
            stats = {
                'chunk_number': chunk_idx,
                'records_read': len(chunk_df),
                'records_valid': len(chunk_records),
                'records_with_errors': chunk_errors,
                'processing_time_seconds': round(chunk_elapsed, 4),
                'records_per_second': round(len(chunk_df) / chunk_elapsed, 2) if chunk_elapsed > 0 else 0,
                'memory_usage_mb': round(current_mem, 2),
                'memory_delta_mb': round(mem_delta, 2),
                'total_records_processed_so_far': self.total_records_read + self.total_errors
            }
            
            self.logger.info(
                f"Chunk {chunk_idx}: Extracted {len(chunk_records)} valid records ({chunk_errors} skipped), "
                f"RAM: {current_mem:.1f} MB, Speed: {stats['records_per_second']} rec/s"
            )
            yield chunk_records, stats

    def _extract_chunks_native_csv(self) -> Iterator[Tuple[List[Dict[str, Any]], Dict[str, Any]]]:
        """Native streaming CSV extractor that bypasses external C DLLs."""
        with open(self.file_path, mode='r', encoding='utf-8', errors='replace', newline='') as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                return
                
            self.resolved_column_map = self._detect_and_map_columns(header)
            header_indices = {col.strip(): idx for idx, col in enumerate(header)}
            
            chunk_records: List[Dict[str, Any]] = []
            chunk_idx = 1
            chunk_errors = 0
            chunk_start_ts = time.time()
            
            for row in reader:
                if len(row) != len(header):
                    chunk_errors += 1
                    continue
                    
                record = {}
                for canonical, actual_col in self.resolved_column_map.items():
                    col_idx = header_indices.get(actual_col)
                    if col_idx is not None and col_idx < len(row):
                        record[canonical] = row[col_idx]
                        record[actual_col] = row[col_idx]
                        
                tx_id = record.get('transaction_id')
                price = record.get('price')
                if not tx_id or not price:
                    chunk_errors += 1
                    continue
                    
                chunk_records.append(record)
                
                if len(chunk_records) >= self.chunk_size:
                    chunk_elapsed = time.time() - chunk_start_ts
                    self.total_records_read += len(chunk_records)
                    self.total_errors += chunk_errors
                    self.total_chunks += 1
                    current_mem = self._get_memory_usage_mb()
                    mem_delta = current_mem - self.initial_memory_mb
                    
                    stats = {
                        'chunk_number': chunk_idx,
                        'records_read': len(chunk_records) + chunk_errors,
                        'records_valid': len(chunk_records),
                        'records_with_errors': chunk_errors,
                        'processing_time_seconds': round(chunk_elapsed, 4),
                        'records_per_second': round((len(chunk_records) + chunk_errors) / chunk_elapsed, 2) if chunk_elapsed > 0 else 0,
                        'memory_usage_mb': round(current_mem, 2),
                        'memory_delta_mb': round(mem_delta, 2),
                        'total_records_processed_so_far': self.total_records_read + self.total_errors
                    }
                    
                    self.logger.info(
                        f"Chunk {chunk_idx}: Extracted {len(chunk_records)} valid records ({chunk_errors} skipped), "
                        f"RAM: {current_mem:.1f} MB, Speed: {stats['records_per_second']} rec/s"
                    )
                    
                    yield chunk_records, stats
                    chunk_records = []
                    chunk_errors = 0
                    chunk_idx += 1
                    chunk_start_ts = time.time()
                    
            if chunk_records:
                chunk_elapsed = time.time() - chunk_start_ts
                self.total_records_read += len(chunk_records)
                self.total_errors += chunk_errors
                self.total_chunks += 1
                current_mem = self._get_memory_usage_mb()
                mem_delta = current_mem - self.initial_memory_mb
                
                stats = {
                    'chunk_number': chunk_idx,
                    'records_read': len(chunk_records) + chunk_errors,
                    'records_valid': len(chunk_records),
                    'records_with_errors': chunk_errors,
                    'processing_time_seconds': round(chunk_elapsed, 4),
                    'records_per_second': round((len(chunk_records) + chunk_errors) / chunk_elapsed, 2) if chunk_elapsed > 0 else 0,
                    'memory_usage_mb': round(current_mem, 2),
                    'memory_delta_mb': round(mem_delta, 2),
                    'total_records_processed_so_far': self.total_records_read + self.total_errors
                }
                self.logger.info(
                    f"Chunk {chunk_idx}: Extracted {len(chunk_records)} valid records ({chunk_errors} skipped), "
                    f"RAM: {current_mem:.1f} MB"
                )
                yield chunk_records, stats

    def get_extraction_summary(self) -> Dict[str, Any]:
        end_time = datetime.now()
        start = self.extraction_start_time or end_time
        total_seconds = (end_time - start).total_seconds()
        total_read = self.total_records_read + self.total_errors
        
        return {
            'file_path': str(self.file_path),
            'chunk_size': self.chunk_size,
            'total_chunks_processed': self.total_chunks,
            'total_records_read': total_read,
            'total_records_valid': self.total_records_read,
            'total_records_with_errors': self.total_errors,
            'error_rate_percent': round((self.total_errors / total_read * 100), 2) if total_read > 0 else 0.0,
            'total_extraction_time_seconds': round(total_seconds, 2),
            'records_per_second': round(self.total_records_read / total_seconds, 2) if total_seconds > 0 else 0.0,
            'final_memory_usage_mb': round(self._get_memory_usage_mb(), 2),
            'extraction_start_time': start.isoformat(),
            'extraction_end_time': end_time.isoformat()
        }
