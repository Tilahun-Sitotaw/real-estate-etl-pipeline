"""
Unit tests for the Data Extraction Module

Tests the DataExtractor class for:
- Chunked CSV reading with configurable chunk size
- Required field extraction and validation
- Error handling for malformed rows
- Memory usage tracking
- Generator pattern with proper statistics
- Edge cases and error conditions
"""

import pytest
import pandas as pd
import csv
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from etl_pipeline.extractor import DataExtractor, ExtractionError


class TestDataExtractorInitialization:
    """Test DataExtractor initialization and validation."""
    
    def test_initialization_with_valid_file(self):
        """Test initialization with a valid CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            # Write a minimal valid CSV
            f.write('property_id,address,city,location_code,sale_price,sale_date,square_meters,property_type,seller_type\n')
            f.write('1,123 Main St,NYC,NYC-001,500000,2024-01-01,1500,residential,private\n')
            temp_path = f.name
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            assert extractor.file_path == Path(temp_path)
            assert extractor.chunk_size == 1000
            assert extractor.total_records_read == 0
            assert extractor.total_errors == 0
            assert extractor.total_chunks == 0
        finally:
            os.unlink(temp_path)
    
    def test_initialization_with_nonexistent_file(self):
        """Test initialization with nonexistent file raises ExtractionError."""
        with pytest.raises(ExtractionError, match="CSV file not found"):
            DataExtractor('/nonexistent/path/file.csv', chunk_size=1000)
    
    def test_initialization_with_invalid_chunk_size(self):
        """Test initialization with invalid chunk size raises ExtractionError."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('property_id,address,city,location_code,sale_price,sale_date,square_meters,property_type,seller_type\n')
            temp_path = f.name
        
        try:
            with pytest.raises(ExtractionError, match="Invalid chunk_size"):
                DataExtractor(temp_path, chunk_size=0)
            
            with pytest.raises(ExtractionError, match="Invalid chunk_size"):
                DataExtractor(temp_path, chunk_size=-1)
        finally:
            os.unlink(temp_path)
    
    def test_initialization_with_default_chunk_size(self):
        """Test initialization uses default chunk size of 50000."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('property_id,address,city,location_code,sale_price,sale_date,square_meters,property_type,seller_type\n')
            temp_path = f.name
        
        try:
            extractor = DataExtractor(temp_path)
            assert extractor.chunk_size == 50000
        finally:
            os.unlink(temp_path)


class TestDataExtractorChunkExtraction:
    """Test chunk extraction functionality."""
    
    def _create_csv_file(self, records, include_header=True):
        """Helper to create a temporary CSV file with given records."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        fieldnames = [
            'property_id', 'address', 'city', 'location_code', 'sale_price',
            'sale_date', 'square_meters', 'property_type', 'seller_type'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if include_header:
            writer.writeheader()
        
        writer.writerows(records)
        f.close()
        return f.name
    
    def test_extract_single_valid_record(self):
        """Test extraction of a single valid record."""
        records = [{
            'property_id': 'PROP_00001',
            'address': '123 Main St',
            'city': 'New York',
            'location_code': 'NYC-001',
            'sale_price': '500000.00',
            'sale_date': '2024-01-15',
            'square_meters': '1500.50',
            'property_type': 'residential',
            'seller_type': 'private'
        }]
        
        temp_path = self._create_csv_file(records)
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            chunks = list(extractor.extract_chunks())
            
            assert len(chunks) == 1
            extracted_records, stats = chunks[0]
            
            assert len(extracted_records) == 1
            assert extracted_records[0]['property_id'] == 'PROP_00001'
            assert extracted_records[0]['address'] == '123 Main St'
            assert extracted_records[0]['city'] == 'New York'
            assert stats['chunk_number'] == 1
            assert stats['records_read'] == 1
            assert stats['records_valid'] == 1
            assert stats['records_with_errors'] == 0
        finally:
            os.unlink(temp_path)
    
    def test_extract_multiple_chunks(self):
        """Test extraction with multiple chunks."""
        # Create 150 records to get 2 chunks with chunk_size=100
        records = []
        for i in range(150):
            records.append({
                'property_id': f'PROP_{i:08d}',
                'address': f'{i} Main St',
                'city': 'New York',
                'location_code': 'NYC-001',
                'sale_price': '500000.00',
                'sale_date': '2024-01-15',
                'square_meters': '1500.50',
                'property_type': 'residential',
                'seller_type': 'private'
            })
        
        temp_path = self._create_csv_file(records)
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=100)
            chunks = list(extractor.extract_chunks())
            
            assert len(chunks) == 2
            
            # First chunk
            chunk1_records, stats1 = chunks[0]
            assert stats1['chunk_number'] == 1
            assert stats1['records_read'] == 100
            assert stats1['records_valid'] == 100
            
            # Second chunk
            chunk2_records, stats2 = chunks[1]
            assert stats2['chunk_number'] == 2
            assert stats2['records_read'] == 50
            assert stats2['records_valid'] == 50
            
            # Total extracted
            total_records = chunk1_records + chunk2_records
            assert len(total_records) == 150
        finally:
            os.unlink(temp_path)
    
    def test_extract_records_with_missing_required_field(self):
        """Test that records with missing required fields are skipped."""
        records = [
            {
                'property_id': 'PROP_00001',
                'address': '123 Main St',
                'city': 'New York',
                'location_code': 'NYC-001',
                'sale_price': '500000.00',
                'sale_date': '2024-01-15',
                'square_meters': '1500.50',
                'property_type': 'residential',
                'seller_type': 'private'
            },
            {
                'property_id': 'PROP_00002',
                'address': '456 Oak Ave',
                'city': 'New York',
                'location_code': 'NYC-001',
                'sale_price': '',  # Missing price
                'sale_date': '2024-01-15',
                'square_meters': '2000.00',
                'property_type': 'commercial',
                'seller_type': 'developer'
            }
        ]
        
        temp_path = self._create_csv_file(records)
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            chunks = list(extractor.extract_chunks())
            
            extracted_records, stats = chunks[0]
            
            # Only 1 valid record (second one has missing price)
            assert len(extracted_records) == 1
            assert stats['records_valid'] == 1
            assert stats['records_with_errors'] == 1
            assert extracted_records[0]['property_id'] == 'PROP_00001'
        finally:
            os.unlink(temp_path)
    
    def test_extract_records_missing_all_required_fields_header_check(self):
        """Test extraction fails when CSV is missing required field columns."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        # Write CSV with missing required fields
        f.write('property_id,address\n')
        f.write('1,123 Main St\n')
        f.close()
        temp_path = f.name
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            
            with pytest.raises(ExtractionError, match="CSV is missing required fields"):
                list(extractor.extract_chunks())
        finally:
            os.unlink(temp_path)


class TestDataExtractorMemoryTracking:
    """Test memory usage tracking functionality."""
    
    def _create_csv_file(self, num_records):
        """Helper to create a CSV file with specified number of records."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        fieldnames = [
            'property_id', 'address', 'city', 'location_code', 'sale_price',
            'sale_date', 'square_meters', 'property_type', 'seller_type'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for i in range(num_records):
            writer.writerow({
                'property_id': f'PROP_{i:08d}',
                'address': f'{i} Main St',
                'city': 'New York',
                'location_code': 'NYC-001',
                'sale_price': '500000.00',
                'sale_date': '2024-01-15',
                'square_meters': '1500.50',
                'property_type': 'residential',
                'seller_type': 'private'
            })
        
        f.close()
        return f.name
    
    def test_memory_tracking_per_chunk(self):
        """Test that memory usage is tracked for each chunk."""
        temp_path = self._create_csv_file(250)  # 250 records with chunk_size=100
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=100)
            
            for records, stats in extractor.extract_chunks():
                # Check that chunk stats include memory info
                assert 'memory_usage_mb' in stats
                assert 'memory_delta_mb' in stats
                assert 'processing_time_seconds' in stats
                assert 'records_per_second' in stats
                assert stats['memory_usage_mb'] >= 0
        finally:
            os.unlink(temp_path)
    
    def test_get_memory_usage_mb(self):
        """Test memory usage method."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        f.write('property_id,address,city,location_code,sale_price,sale_date,square_meters,property_type,seller_type\n')
        f.write('1,123 Main St,NYC,NYC-001,500000,2024-01-01,1500,residential,private\n')
        f.close()
        temp_path = f.name
        
        try:
            extractor = DataExtractor(temp_path)
            memory_mb = extractor._get_memory_usage_mb()
            
            # Memory should be a positive number
            assert memory_mb > 0
            assert isinstance(memory_mb, float)
        finally:
            os.unlink(temp_path)


class TestDataExtractorStatistics:
    """Test extraction statistics and summary methods."""
    
    def _create_csv_file(self, valid_records=10, error_records=2):
        """Helper to create a CSV file with mixed valid/error records."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        fieldnames = [
            'property_id', 'address', 'city', 'location_code', 'sale_price',
            'sale_date', 'square_meters', 'property_type', 'seller_type'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        # Write valid records
        for i in range(valid_records):
            writer.writerow({
                'property_id': f'PROP_{i:08d}',
                'address': f'{i} Main St',
                'city': 'New York',
                'location_code': 'NYC-001',
                'sale_price': '500000.00',
                'sale_date': '2024-01-15',
                'square_meters': '1500.50',
                'property_type': 'residential',
                'seller_type': 'private'
            })
        
        # Write error records (missing required field)
        for i in range(error_records):
            writer.writerow({
                'property_id': f'ERROR_{i:08d}',
                'address': '',
                'city': 'New York',
                'location_code': 'NYC-001',
                'sale_price': '',  # Missing price
                'sale_date': '2024-01-15',
                'square_meters': '1500.50',
                'property_type': 'residential',
                'seller_type': 'private'
            })
        
        f.close()
        return f.name
    
    def test_get_extraction_summary(self):
        """Test extraction summary generation."""
        temp_path = self._create_csv_file(valid_records=50, error_records=5)
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=100)
            
            # Run extraction
            list(extractor.extract_chunks())
            
            # Get summary
            summary = extractor.get_extraction_summary()
            
            assert summary['total_records_valid'] == 50
            assert summary['total_records_with_errors'] == 5
            assert summary['total_records_read'] == 55
            assert 0 <= summary['error_rate_percent'] <= 100
            assert summary['total_extraction_time_seconds'] >= 0
            assert 'file_path' in summary
            assert 'chunk_size' in summary
            assert 'total_chunks_processed' in summary
            assert 'records_per_second' in summary
            assert 'final_memory_usage_mb' in summary
        finally:
            os.unlink(temp_path)
    
    def test_extraction_tracking(self):
        """Test that extraction properly tracks records and errors."""
        temp_path = self._create_csv_file(valid_records=30, error_records=3)
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=100)
            
            chunks = list(extractor.extract_chunks())
            assert len(chunks) == 1
            
            records, stats = chunks[0]
            assert extractor.total_records_read == 30
            assert extractor.total_errors == 3
            assert extractor.total_chunks == 1
        finally:
            os.unlink(temp_path)


class TestDataExtractorEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_extract_empty_csv(self):
        """Test extraction of empty CSV (header only)."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        f.write('property_id,address,city,location_code,sale_price,sale_date,square_meters,property_type,seller_type\n')
        f.close()
        temp_path = f.name
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            chunks = list(extractor.extract_chunks())
            
            # Empty CSV should produce no chunks
            assert len(chunks) == 0
        finally:
            os.unlink(temp_path)
    
    def test_extract_with_special_characters_in_fields(self):
        """Test extraction of records with special characters."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8')
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'property_id', 'address', 'city', 'location_code', 'sale_price',
                'sale_date', 'square_meters', 'property_type', 'seller_type'
            ]
        )
        writer.writeheader()
        writer.writerow({
            'property_id': 'PROP_00001',
            'address': '123 "Oak" St, Apt #5 & 6',
            'city': "O'Fallon",
            'location_code': 'NYC-001',
            'sale_price': '500000.00',
            'sale_date': '2024-01-15',
            'square_meters': '1500.50',
            'property_type': 'residential',
            'seller_type': 'private'
        })
        f.close()
        temp_path = f.name
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            chunks = list(extractor.extract_chunks())
            
            extracted_records, stats = chunks[0]
            assert len(extracted_records) == 1
            assert 'Oak' in extracted_records[0]['address']
            assert "O'Fallon" == extracted_records[0]['city']
        finally:
            os.unlink(temp_path)
    
    def test_extract_with_numeric_string_values(self):
        """Test extraction properly handles numeric values as strings."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'property_id', 'address', 'city', 'location_code', 'sale_price',
                'sale_date', 'square_meters', 'property_type', 'seller_type'
            ]
        )
        writer.writeheader()
        writer.writerow({
            'property_id': '12345',  # Numeric property_id
            'address': '123 Main St',
            'city': 'New York',
            'location_code': 'NYC-001',
            'sale_price': '1500000.50',
            'sale_date': '2024-01-15',
            'square_meters': '5000.75',
            'property_type': 'commercial',
            'seller_type': 'developer'
        })
        f.close()
        temp_path = f.name
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            chunks = list(extractor.extract_chunks())
            
            extracted_records, stats = chunks[0]
            record = extracted_records[0]
            
            # Values should be extracted correctly
            assert record['property_id'] == '12345'
            assert record['sale_price'] == '1500000.50'
            assert record['square_meters'] == '5000.75'
        finally:
            os.unlink(temp_path)
    
    def test_chunk_size_larger_than_records(self):
        """Test extraction when chunk_size is larger than number of records."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'property_id', 'address', 'city', 'location_code', 'sale_price',
                'sale_date', 'square_meters', 'property_type', 'seller_type'
            ]
        )
        writer.writeheader()
        
        for i in range(10):
            writer.writerow({
                'property_id': f'PROP_{i:08d}',
                'address': f'{i} Main St',
                'city': 'New York',
                'location_code': 'NYC-001',
                'sale_price': '500000.00',
                'sale_date': '2024-01-15',
                'square_meters': '1500.50',
                'property_type': 'residential',
                'seller_type': 'private'
            })
        f.close()
        temp_path = f.name
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            chunks = list(extractor.extract_chunks())
            
            # Should get 1 chunk with all 10 records
            assert len(chunks) == 1
            records, stats = chunks[0]
            assert len(records) == 10
        finally:
            os.unlink(temp_path)


class TestDataExtractorIntegration:
    """Integration tests for the DataExtractor with realistic scenarios."""
    
    def _create_sample_csv(self, num_records=1000):
        """Create a sample CSV file with realistic data."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'property_id', 'address', 'city', 'location_code', 'sale_price',
                'sale_date', 'square_meters', 'property_type', 'seller_type'
            ]
        )
        writer.writeheader()
        
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
        location_codes = ['NYC-001', 'LAC-001', 'CHI-001', 'HOU-001', 'PHX-001']
        property_types = ['residential', 'commercial', 'industrial', 'land']
        seller_types = ['private', 'developer', 'institutional']
        
        for i in range(num_records):
            city_idx = i % len(cities)
            writer.writerow({
                'property_id': f'PROP_{i:08d}',
                'address': f'{i} Main St',
                'city': cities[city_idx],
                'location_code': location_codes[city_idx],
                'sale_price': str(100000 + i * 1000),
                'sale_date': '2024-01-15',
                'square_meters': str(1000 + i * 10),
                'property_type': property_types[i % len(property_types)],
                'seller_type': seller_types[i % len(seller_types)]
            })
        
        f.close()
        return f.name
    
    def test_extraction_performance_with_large_dataset(self):
        """Test extraction performance with a larger dataset."""
        temp_path = self._create_sample_csv(num_records=5000)
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            
            total_records = 0
            chunk_count = 0
            
            for records, stats in extractor.extract_chunks():
                total_records += len(records)
                chunk_count += 1
                
                # Verify chunk statistics are populated
                assert stats['chunk_number'] > 0
                assert stats['records_read'] > 0
                assert stats['processing_time_seconds'] >= 0
            
            assert total_records == 5000
            assert chunk_count == 5
        finally:
            os.unlink(temp_path)
    
    def test_extraction_with_all_fields_types(self):
        """Test extraction with various field data types."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'property_id', 'address', 'city', 'location_code', 'sale_price',
                'sale_date', 'square_meters', 'property_type', 'seller_type'
            ]
        )
        writer.writeheader()
        
        test_records = [
            {
                'property_id': 'PROP_00001',
                'address': '123 Main St',
                'city': 'New York',
                'location_code': 'NYC-001',
                'sale_price': '500000.00',
                'sale_date': '2024-01-15',
                'square_meters': '1500.50',
                'property_type': 'residential',
                'seller_type': 'private'
            },
            {
                'property_id': '456789',
                'address': '456 Oak Ave',
                'city': 'Los Angeles',
                'location_code': 'LAC-001',
                'sale_price': '2500000',
                'sale_date': '2024-02-20',
                'square_meters': '5000',
                'property_type': 'commercial',
                'seller_type': 'developer'
            }
        ]
        
        for record in test_records:
            writer.writerow(record)
        
        f.close()
        temp_path = f.name
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            chunks = list(extractor.extract_chunks())
            
            records, stats = chunks[0]
            assert len(records) == 2
            
            # Verify first record
            assert records[0]['property_id'] == 'PROP_00001'
            assert records[0]['sale_price'] == '500000.00'
            
            # Verify second record
            assert records[1]['property_id'] == '456789'
            assert records[1]['sale_price'] == '2500000'
        finally:
            os.unlink(temp_path)


class TestDataExtractorErrorHandling:
    """Test error handling and resilience."""
    
    def test_extraction_continues_after_malformed_rows(self):
        """Test that extraction continues processing after encountering malformed rows."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        f.write('property_id,address,city,location_code,sale_price,sale_date,square_meters,property_type,seller_type\n')
        f.write('PROP_00001,123 Main St,NYC,NYC-001,500000,2024-01-01,1500,residential,private\n')
        f.write('PROP_00002,456 Oak Ave,LA,LAC-001,,2024-01-02,2000,commercial,developer\n')  # Missing price
        f.write('PROP_00003,789 Pine Rd,CHI,CHI-001,1000000,2024-01-03,3000,industrial,institutional\n')
        f.close()
        temp_path = f.name
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            chunks = list(extractor.extract_chunks())
            
            records, stats = chunks[0]
            
            # Should have 2 valid records (skipping the one with missing price)
            assert len(records) == 2
            assert stats['records_with_errors'] == 1
            assert records[0]['property_id'] == 'PROP_00001'
            assert records[1]['property_id'] == 'PROP_00003'
        finally:
            os.unlink(temp_path)
    
    def test_extraction_handles_various_missing_field_types(self):
        """Test that extraction properly detects various types of missing fields."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'property_id', 'address', 'city', 'location_code', 'sale_price',
                'sale_date', 'square_meters', 'property_type', 'seller_type'
            ]
        )
        writer.writeheader()
        
        # Missing each required field in different rows
        missing_fields = [
            ('property_id', None),
            ('address', None),
            ('city', None),
            ('location_code', None),
            ('sale_price', None),
            ('sale_date', None),
            ('square_meters', None),
            ('property_type', None),
            ('seller_type', None)
        ]
        
        for field_name, _ in missing_fields:
            row = {
                'property_id': 'PROP_00001',
                'address': '123 Main St',
                'city': 'New York',
                'location_code': 'NYC-001',
                'sale_price': '500000.00',
                'sale_date': '2024-01-15',
                'square_meters': '1500.50',
                'property_type': 'residential',
                'seller_type': 'private'
            }
            row[field_name] = ''  # Remove the field
            writer.writerow(row)
        
        f.close()
        temp_path = f.name
        
        try:
            extractor = DataExtractor(temp_path, chunk_size=1000)
            chunks = list(extractor.extract_chunks())
            
            records, stats = chunks[0]
            
            # All records should be skipped due to missing fields
            assert len(records) == 0
            assert stats['records_with_errors'] == 9
        finally:
            os.unlink(temp_path)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
