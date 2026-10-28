"""
Unit tests for Real Estate ETL Pipeline Validators Module.
"""

import pytest
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


class TestValidators:
    """Test suite for individual validation and standardization functions."""
    
    def test_clean_string(self):
        assert clean_string("  London  ") == "London"
        assert clean_string("") is None
        assert clean_string("None") is None
        assert clean_string("nan") is None
        assert clean_string(None) is None

    def test_validate_price_valid(self):
        valid, price, err = validate_price("250000")
        assert valid is True
        assert price == 250000.0
        assert err is None
        
        valid, price, err = validate_price(450000.50)
        assert valid is True
        assert price == 450000.50

    def test_validate_price_invalid(self):
        valid, price, err = validate_price(None)
        assert valid is False
        assert err == "missing_price"
        
        valid, price, err = validate_price(-500)
        assert valid is False
        assert err == "non_positive_price"
        
        valid, price, err = validate_price("abc")
        assert valid is False
        assert err == "invalid_numeric_price"

    def test_validate_postcode(self):
        valid, pc, err = validate_postcode("sw1a 1aa")
        assert valid is True
        assert pc == "SW1A 1AA"
        assert err is None
        
        valid, pc, err = validate_postcode("")
        assert valid is False
        assert err == "missing_postcode"

    def test_validate_date(self):
        valid, dt_str, yr, mo, err = validate_date("2021-08-15")
        assert valid is True
        assert dt_str == "2021-08-15"
        assert yr == 2021
        assert mo == 8
        assert err is None
        
        valid, dt_str, yr, mo, err = validate_date("invalid-date")
        assert valid is False

    def test_standardize_property_type(self):
        code, cat = standardize_property_type("D")
        assert code == "D"
        assert cat == "Detached"
        
        code, cat = standardize_property_type("Semi-Detached")
        assert code == "S"
        assert cat == "Semi-Detached"
        
        code, cat = standardize_property_type("F")
        assert code == "F"
        assert cat == "Flat/Maisonette"

    def test_standardize_duration(self):
        desc, is_freehold = standardize_duration("F")
        assert desc == "Freehold"
        assert is_freehold is True
        
        desc, is_freehold = standardize_duration("L")
        assert desc == "Leasehold"
        assert is_freehold is False

    def test_calculate_data_quality_score(self):
        score_full = calculate_data_quality_score(
            has_valid_price=True,
            has_valid_postcode=True,
            has_valid_date=True,
            has_town_city=True,
            has_coordinates=True,
            is_outlier=False
        )
        assert score_full == 100
        
        score_outlier = calculate_data_quality_score(
            has_valid_price=True,
            has_valid_postcode=True,
            has_valid_date=True,
            has_town_city=True,
            has_coordinates=True,
            is_outlier=True
        )
        assert score_outlier == 90
