"""
Data Transformation Module for Real Estate ETL Pipeline.

Performs vectorized cleaning, validation, standardization, feature enrichment,
outlier detection, and deduplication for large property transaction datasets.
"""

import hashlib
import json
from datetime import datetime
from typing import List, Dict, Any, Tuple, Set, Optional

from etl_pipeline.logger import get_logger
from etl_pipeline.validators import (
    validate_price,
    validate_postcode,
    validate_date,
    standardize_property_type,
    standardize_duration,
    validate_square_meters,
    calculate_data_quality_score,
    clean_string
)


class DataTransformer:
    """
    Transforms raw extracted real estate records into high-quality standardized rows.
    
    Handles:
    - Quality issue detection and logging
    - Postcode and location code canonicalization
    - Missing/zero price filtering and outlier tagging
    - Derived temporal and financial metrics (sale_year, sale_month, months_since_sale, price_per_sqm)
    - Deduplication using SHA-256 cryptographic hashing
    - Data quality scoring (0 to 100)
    - Metadata preservation for compliance and audit
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the transformer with reference config."""
        self.logger = get_logger()
        self.config = config or {}
        
        # Deduplication memory (hashes of records processed during run)
        self.seen_hashes: Set[str] = set()
        
        # Cumulative quality metrics
        self.total_processed = 0
        self.total_retained = 0
        self.total_missing_price = 0
        self.total_invalid_postcode = 0
        self.total_invalid_date = 0
        self.total_missing_location = 0
        self.total_duplicates = 0
        self.total_outliers = 0
        self.total_quality_points = 0
        
    def _compute_record_hash(self, tx_id: str, postcode: str, price: float, date_str: str) -> str:
        """Compute SHA-256 hash for strict deduplication."""
        payload = f"{tx_id}|{postcode}|{price:.2f}|{date_str}".encode('utf-8')
        return hashlib.sha256(payload).hexdigest()

    def transform_chunk(self, raw_records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Clean, validate, and enrich a chunk of records.
        
        Args:
            raw_records: List of dictionaries extracted from CSV
            
        Returns:
            Tuple of (cleaned_records, chunk_quality_stats)
        """
        cleaned_records: List[Dict[str, Any]] = []
        chunk_missing_price = 0
        chunk_invalid_postcode = 0
        chunk_invalid_date = 0
        chunk_missing_loc = 0
        chunk_duplicates = 0
        chunk_outliers = 0
        now_dt = datetime.now()
        
        for raw in raw_records:
            self.total_processed += 1
            
            # 1. Transaction Identifier
            tx_id = (
                clean_string(raw.get('transaction_id')) or
                clean_string(raw.get('property_id')) or
                clean_string(raw.get('transaction_unique_identifier')) or
                clean_string(raw.get('id'))
            )
            if not tx_id:
                # Assign deterministic identifier if missing
                tx_id = f"TXN_{self.total_processed:09d}"
                
            # 2. Price Validation
            raw_price = raw.get('price') if 'price' in raw else raw.get('sale_price')
            price_valid, price_val, price_err = validate_price(raw_price)
            if not price_valid or price_val is None:
                chunk_missing_price += 1
                self.total_missing_price += 1
                continue
                
            is_outlier = (price_err == 'outlier_high_price')
            if is_outlier:
                chunk_outliers += 1
                self.total_outliers += 1
                
            # 3. Postcode Validation
            raw_postcode = raw.get('postcode') if 'postcode' in raw else raw.get('location_code')
            postcode_valid, postcode_val, postcode_err = validate_postcode(raw_postcode)
            if not postcode_valid or postcode_val is None:
                chunk_invalid_postcode += 1
                self.total_invalid_postcode += 1
                continue
                
            # 4. Date Validation & Temporal Derivations
            raw_date = raw.get('date_of_transfer') if 'date_of_transfer' in raw else raw.get('sale_date')
            date_valid, date_str, sale_year, sale_month, date_err = validate_date(raw_date)
            if not date_valid or date_str is None or sale_year is None or sale_month is None:
                chunk_invalid_date += 1
                self.total_invalid_date += 1
                continue
                
            # Temporal feature: months since transaction
            months_diff = (now_dt.year - sale_year) * 12 + (now_dt.month - sale_month)
            months_since_tx = max(0, months_diff)
            
            # 5. Location fields
            town_city = clean_string(raw.get('town_city')) or clean_string(raw.get('city'))
            if not town_city:
                chunk_missing_loc += 1
                self.total_missing_location += 1
                continue
            town_city = town_city.title()
            
            district = clean_string(raw.get('district')) or town_city
            district = district.title()
            county = clean_string(raw.get('county'))
            county = county.title() if county else district
            
            region = clean_string(raw.get('region')) or 'United Kingdom'
            country = clean_string(raw.get('country')) or 'England'
            
            # Address line
            address = (
                clean_string(raw.get('address')) or
                clean_string(raw.get('paon')) or
                clean_string(raw.get('street')) or
                f"{town_city}, {postcode_val}"
            )
            
            # 6. Property Classification and Tenure
            raw_type = raw.get('property_type_full') or raw.get('property_type')
            prop_type_code, prop_type_category = standardize_property_type(raw_type)
            
            raw_duration = raw.get('duration_full') or raw.get('duration') or raw.get('seller_type')
            duration_desc, is_freehold = standardize_duration(raw_duration)
            
            # New build flag
            raw_new = raw.get('is_new_build') or raw.get('old_new')
            is_new_build = str(raw_new).upper().startswith(('Y', 'T', '1')) if raw_new else False
            
            # 7. Coordinates & Measurements
            try:
                lat = float(raw['latitude']) if raw.get('latitude') else None
                lon = float(raw['longitude']) if raw.get('longitude') else None
            except (ValueError, TypeError):
                lat, lon = None, None
                
            sqm_valid, sqm_val = validate_square_meters(raw.get('square_meters'))
            price_per_sqm = round(price_val / sqm_val, 2) if (sqm_val and sqm_val > 0) else None
            
            # 8. Deduplication Check
            record_hash = self._compute_record_hash(tx_id, postcode_val, price_val, date_str)
            if record_hash in self.seen_hashes:
                chunk_duplicates += 1
                self.total_duplicates += 1
                continue
            self.seen_hashes.add(record_hash)
            
            # 9. Quality Scoring
            quality_score = calculate_data_quality_score(
                has_valid_price=True,
                has_valid_postcode=True,
                has_valid_date=True,
                has_town_city=True,
                has_coordinates=(lat is not None and lon is not None),
                is_outlier=is_outlier
            )
            self.total_quality_points += quality_score
            
            # 10. Audit Metadata
            metadata = {
                'raw_property_type': str(raw_type),
                'raw_duration': str(raw_duration),
                'is_outlier': is_outlier,
                'pipeline_version': '1.0.0'
            }
            
            cleaned_record = {
                'transaction_id': tx_id,
                'price': price_val,
                'date_of_transfer': date_str,
                'sale_year': sale_year,
                'sale_month': sale_month,
                'months_since_transaction': months_since_tx,
                'postcode': postcode_val,
                'town_city': town_city,
                'district': district,
                'county': county,
                'region': region,
                'country': country,
                'property_type': prop_type_code,
                'property_type_category': prop_type_category,
                'duration_type': duration_desc,
                'is_freehold': is_freehold,
                'is_new_build': is_new_build,
                'address_line': address,
                'square_meters': sqm_val,
                'price_per_square_meter': price_per_sqm,
                'latitude': lat,
                'longitude': lon,
                'data_quality_score': quality_score,
                'dedup_hash': record_hash,
                'metadata': json.dumps(metadata)
            }
            cleaned_records.append(cleaned_record)
            self.total_retained += 1
            
        chunk_stats = {
            'records_in': len(raw_records),
            'records_out': len(cleaned_records),
            'missing_price': chunk_missing_price,
            'invalid_postcode': chunk_invalid_postcode,
            'invalid_date': chunk_invalid_date,
            'missing_location': chunk_missing_loc,
            'duplicates_removed': chunk_duplicates,
            'outliers_flagged': chunk_outliers
        }
        
        return cleaned_records, chunk_stats

    def get_quality_summary(self) -> Dict[str, Any]:
        """Produce overall data quality statistics for the run."""
        retained = self.total_retained
        processed = self.total_processed
        retention_rate = round((retained / processed * 100), 2) if processed > 0 else 0.0
        avg_score = round(self.total_quality_points / retained, 2) if retained > 0 else 0.0
        
        return {
            'total_processed': processed,
            'total_retained': retained,
            'total_discarded': processed - retained,
            'retention_rate_pct': retention_rate,
            'avg_data_quality_score': avg_score,
            'issues_breakdown': {
                'missing_or_non_positive_price': self.total_missing_price,
                'invalid_or_missing_postcode': self.total_invalid_postcode,
                'invalid_or_missing_date': self.total_invalid_date,
                'missing_location_info': self.total_missing_location,
                'duplicates_excluded': self.total_duplicates,
                'price_outliers_flagged': self.total_outliers
            }
        }
