"""
Unit tests for Real Estate ETL Pipeline DataTransformer Module.
"""

import pytest
from etl_pipeline.transformer import DataTransformer


class TestDataTransformer:
    """Test suite for DataTransformer cleaning, deduplication, and enrichment."""
    
    @pytest.fixture
    def transformer(self):
        return DataTransformer()

    def test_transform_chunk_valid_records(self, transformer):
        raw_records = [
            {
                'transaction_id': '{A1B2C3D4-0001}',
                'price': '320000',
                'date_of_transfer': '2023-05-14',
                'postcode': 'NW1 4NP',
                'town_city': 'London',
                'district': 'Camden',
                'county': 'Greater London',
                'property_type': 'F',
                'duration': 'L',
                'latitude': '51.52',
                'longitude': '-0.14'
            },
            {
                'transaction_id': '{A1B2C3D4-0002}',
                'price': '450000',
                'date_of_transfer': '2022-10-20',
                'postcode': 'M1 1AE',
                'town_city': 'Manchester',
                'district': 'Manchester',
                'county': 'Greater Manchester',
                'property_type': 'S',
                'duration': 'F',
                'latitude': '53.48',
                'longitude': '-2.24'
            }
        ]
        
        cleaned, stats = transformer.transform_chunk(raw_records)
        assert len(cleaned) == 2
        assert stats['records_in'] == 2
        assert stats['records_out'] == 2
        
        rec1 = cleaned[0]
        assert rec1['transaction_id'] == '{A1B2C3D4-0001}'
        assert rec1['price'] == 320000.0
        assert rec1['property_type_category'] == 'Flat/Maisonette'
        assert rec1['duration_type'] == 'Leasehold'
        assert rec1['is_freehold'] is False
        assert rec1['sale_year'] == 2023
        assert rec1['sale_month'] == 5
        assert rec1['data_quality_score'] == 100

    def test_transform_chunk_filters_invalid_records(self, transformer):
        raw_records = [
            # Missing price
            {
                'transaction_id': 'TX_BAD_1',
                'price': None,
                'date_of_transfer': '2023-01-01',
                'postcode': 'SW1A 1AA',
                'town_city': 'London'
            },
            # Negative price
            {
                'transaction_id': 'TX_BAD_2',
                'price': '-100',
                'date_of_transfer': '2023-01-01',
                'postcode': 'SW1A 1AA',
                'town_city': 'London'
            },
            # Invalid date
            {
                'transaction_id': 'TX_BAD_3',
                'price': '300000',
                'date_of_transfer': 'not-a-date',
                'postcode': 'SW1A 1AA',
                'town_city': 'London'
            },
            # Missing city
            {
                'transaction_id': 'TX_BAD_4',
                'price': '300000',
                'date_of_transfer': '2023-01-01',
                'postcode': 'SW1A 1AA',
                'town_city': None
            }
        ]
        
        cleaned, stats = transformer.transform_chunk(raw_records)
        assert len(cleaned) == 0
        assert stats['missing_price'] == 2
        assert stats['invalid_date'] == 1
        assert stats['missing_location'] == 1

    def test_deduplication(self, transformer):
        dup_record = {
            'transaction_id': 'TX_DUP_001',
            'price': '250000',
            'date_of_transfer': '2023-06-01',
            'postcode': 'B1 1AA',
            'town_city': 'Birmingham'
        }
        
        # Feed the same record twice
        cleaned1, stats1 = transformer.transform_chunk([dup_record])
        assert len(cleaned1) == 1
        assert stats1['duplicates_removed'] == 0
        
        cleaned2, stats2 = transformer.transform_chunk([dup_record])
        assert len(cleaned2) == 0
        assert stats2['duplicates_removed'] == 1
