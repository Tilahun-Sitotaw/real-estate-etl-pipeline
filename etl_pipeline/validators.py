"""
Validation Module for Real Estate ETL Pipeline.

Implements data quality validation rules for property records including:
- Numeric price validation and outlier boundary checks
- Postcode / location code format and canonicalization
- Date format and sensible range validation
- Property classification mapping and tenure identification
- Data quality score calculation (0 to 100 scale)
"""

import re
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List, Union


# Standard UK Postcode regex pattern (handles inward/outward with optional space)
UK_POSTCODE_REGEX = re.compile(r'^[A-Z]{1,2}[0-9][A-Z0-9]?\s?[0-9][A-Z]{2}$', re.IGNORECASE)

# Mapping single-letter UK Land Registry property types to canonical names
PROPERTY_TYPE_MAP = {
    'D': 'Detached',
    'S': 'Semi-Detached',
    'T': 'Terraced',
    'F': 'Flat/Maisonette',
    'O': 'Other'
}

# Mapping single-letter duration/tenure types
DURATION_TYPE_MAP = {
    'F': 'Freehold',
    'L': 'Leasehold',
    'U': 'Unknown'
}


def clean_string(val: Any) -> Optional[str]:
    """Clean and trim whitespace from strings, returning None if empty."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ('nan', 'null', 'none', '') else None


def validate_price(price: Any, min_price: float = 1000.0, max_price: float = 500_000_000.0) -> Tuple[bool, Optional[float], Optional[str]]:
    """
    Validate that price is a positive valid number within realistic bounds.
    
    Returns:
        (is_valid, cleaned_float_price, error_message)
    """
    if price is None:
        return False, None, "missing_price"
    
    try:
        if isinstance(price, str):
            clean_str = price.replace(',', '').replace('$', '').replace('£', '').strip()
            if not clean_str:
                return False, None, "missing_price"
            num_price = float(clean_str)
        else:
            num_price = float(price)
            
        if num_price <= 0:
            return False, None, "non_positive_price"
        if num_price < min_price:
            return False, num_price, "price_below_minimum"
        if num_price > max_price:
            return True, num_price, "outlier_high_price"
            
        return True, round(num_price, 2), None
    except (ValueError, TypeError):
        return False, None, "invalid_numeric_price"


def validate_postcode(postcode: Any) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Validate and format a UK postcode.
    
    Returns:
        (is_valid, standardized_postcode, error_message)
    """
    cleaned = clean_string(postcode)
    if not cleaned:
        return False, None, "missing_postcode"
    
    # Standardize uppercase and normalize spaces
    normalized = re.sub(r'\s+', ' ', cleaned.upper()).strip()
    
    # Check general length and format
    if 3 <= len(normalized) <= 10:
        return True, normalized, None
    
    return False, normalized, "invalid_postcode_format"


def validate_date(date_val: Any) -> Tuple[bool, Optional[str], Optional[int], Optional[int], Optional[str]]:
    """
    Validate date string or timestamp, extracting ISO date string, year, and month.
    
    Returns:
        (is_valid, iso_date_str, year, month, error_message)
    """
    cleaned = clean_string(date_val)
    if not cleaned:
        return False, None, None, None, "missing_date"
    
    # Common date formats in property registries
    formats = [
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%d/%m/%Y',
        '%d/%m/%Y %H:%M',
        '%m/%d/%Y',
        '%Y/%m/%d'
    ]
    
    dt = None
    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned[:19], fmt)
            break
        except (ValueError, TypeError):
            continue
            
    if dt is None:
        # Try generic ISO fromisoformat
        try:
            dt = datetime.fromisoformat(cleaned.replace('Z', '+00:00'))
        except Exception:
            return False, None, None, None, "invalid_date_format"
            
    if dt.year < 1980 or dt.year > 2035:
        return False, None, None, None, f"date_year_out_of_range: {dt.year}"
        
    return True, dt.strftime('%Y-%m-%d'), dt.year, dt.month, None


def standardize_property_type(raw_type: Any, valid_types: Optional[List[str]] = None) -> Tuple[str, str]:
    """
    Normalize property type from single letter or description into canonical category.
    
    Returns:
        (property_type_code, property_type_category)
    """
    cleaned = clean_string(raw_type)
    if not cleaned:
        return 'O', 'Other'
        
    code = cleaned.upper()[0]
    if code in PROPERTY_TYPE_MAP:
        return code, PROPERTY_TYPE_MAP[code]
        
    lower_val = cleaned.lower()
    if 'semi' in lower_val:
        return 'S', 'Semi-Detached'
    elif 'flat' in lower_val or 'maisonette' in lower_val or 'apartment' in lower_val:
        return 'F', 'Flat/Maisonette'
    elif 'terraced' in lower_val or 'terrace' in lower_val:
        return 'T', 'Terraced'
    elif 'detached' in lower_val:
        return 'D', 'Detached'
    elif 'commercial' in lower_val or 'industrial' in lower_val or 'land' in lower_val or 'residential' in lower_val:
        return 'O', cleaned.title()
        
    return 'O', 'Other'


def standardize_duration(duration_val: Any) -> Tuple[str, bool]:
    """
    Standardize tenure type and determine if freehold.
    
    Returns:
        (duration_description, is_freehold_boolean)
    """
    cleaned = clean_string(duration_val)
    if not cleaned:
        return 'Unknown', False
        
    code = cleaned.upper()[0]
    if code == 'F' or 'freehold' in cleaned.lower():
        return 'Freehold', True
    elif code == 'L' or 'leasehold' in cleaned.lower():
        return 'Leasehold', False
        
    return 'Unknown', False


def validate_square_meters(sqm: Any, max_sqm: float = 100000.0) -> Tuple[bool, Optional[float]]:
    """Validate square meters if provided."""
    if sqm is None:
        return True, None
    try:
        val = float(sqm)
        if 0 < val <= max_sqm:
            return True, round(val, 2)
        return False, None
    except (ValueError, TypeError):
        return False, None


def calculate_data_quality_score(
    has_valid_price: bool,
    has_valid_postcode: bool,
    has_valid_date: bool,
    has_town_city: bool,
    has_coordinates: bool,
    is_outlier: bool = False
) -> int:
    """
    Calculate an individual record quality score (0 to 100).
    
    Scoring weights:
    - Valid Price: 30 pts
    - Valid Date: 20 pts
    - Valid Postcode: 20 pts
    - Valid Town/City: 15 pts
    - Valid Coordinates: 10 pts
    - Outlier penalty: -10 pts
    """
    score = 5  # Base metadata score
    if has_valid_price:
        score += 30
    if has_valid_date:
        score += 20
    if has_valid_postcode:
        score += 20
    if has_town_city:
        score += 15
    if has_coordinates:
        score += 10
    if is_outlier:
        score = max(0, score - 10)
        
    return min(100, max(0, score))
