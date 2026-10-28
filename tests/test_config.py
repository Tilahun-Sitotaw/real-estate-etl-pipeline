"""
Unit tests for ETL Pipeline configuration module.

Tests configuration loading, validation, and access methods.
"""

import json
import pytest
import tempfile
from pathlib import Path
from etl_pipeline.config import Config, ConfigurationError


class TestConfigLoading:
    """Test configuration file loading and parsing."""
    
    @pytest.fixture
    def valid_config_file(self):
        """Create a temporary valid configuration file."""
        config = {
            "source": {
                "file_path": "./data/test.csv",
                "chunk_size": 50000
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "user": "testuser",
                "password": "testpass",
                "schema": "public"
            },
            "processing": {
                "batch_insert_size": 1000,
                "memory_threshold_mb": 1024,
                "max_square_meters": 100000,
                "price_percentile_low": 1,
                "price_percentile_high": 99
            },
            "reference_data": {
                "location_code_mapping_file": "./data/mapping.json",
                "valid_location_codes_file": "./data/locations.txt",
                "valid_property_types": ["residential", "commercial"],
                "valid_seller_types": ["private", "developer"]
            },
            "output": {
                "log_directory": "./logs",
                "reports_directory": "./reports"
            },
            "execution": {
                "mode": "full_load",
                "backup_on_full_load": True
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config, f)
            temp_file = f.name
        
        yield temp_file
        
        # Cleanup
        Path(temp_file).unlink()
        Path("./logs").exists() and Path("./logs").rmdir() if not list(Path("./logs").iterdir()) else None
        Path("./reports").exists() and Path("./reports").rmdir() if not list(Path("./reports").iterdir()) else None
    
    def test_valid_config_loading(self, valid_config_file):
        """Test that valid configuration loads successfully."""
        config = Config(valid_config_file)
        assert config is not None
        assert config.get_source_file_path() == "./data/test.csv"
    
    def test_missing_config_file(self):
        """Test that missing configuration file raises error."""
        with pytest.raises(ConfigurationError, match="Configuration file not found"):
            Config("./nonexistent/config.json")
    
    def test_invalid_json(self):
        """Test that invalid JSON raises error."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("{ invalid json")
            temp_file = f.name
        
        try:
            with pytest.raises(ConfigurationError, match="Invalid JSON"):
                Config(temp_file)
        finally:
            Path(temp_file).unlink()
    
    def test_missing_required_section(self, valid_config_file):
        """Test that missing required configuration section raises error."""
        config_data = {
            "source": {"file_path": "./data/test.csv", "chunk_size": 50000},
            "database": {"host": "localhost", "port": 5432, "database": "test_db",
                        "user": "testuser", "password": "testpass", "schema": "public"}
            # Missing processing, reference_data, output, execution
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_file = f.name
        
        try:
            with pytest.raises(ConfigurationError, match="Missing required configuration sections"):
                Config(temp_file)
        finally:
            Path(temp_file).unlink()
    
    def test_missing_required_field(self, valid_config_file):
        """Test that missing required field in section raises error."""
        config_data = {
            "source": {
                "file_path": "./data/test.csv"
                # Missing chunk_size
            },
            "database": {"host": "localhost", "port": 5432, "database": "test_db",
                        "user": "testuser", "password": "testpass", "schema": "public"},
            "processing": {"batch_insert_size": 1000, "memory_threshold_mb": 1024,
                          "max_square_meters": 100000},
            "reference_data": {"valid_property_types": ["residential"],
                             "valid_seller_types": ["private"]},
            "output": {"log_directory": "./logs", "reports_directory": "./reports"},
            "execution": {"mode": "full_load"}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_file = f.name
        
        try:
            with pytest.raises(ConfigurationError, match="Missing required fields"):
                Config(temp_file)
        finally:
            Path(temp_file).unlink()


class TestConfigAccessMethods:
    """Test configuration getter methods."""
    
    @pytest.fixture
    def config(self):
        """Create a valid configuration for testing."""
        config_data = {
            "source": {
                "file_path": "./data/test.csv",
                "chunk_size": 50000
            },
            "database": {
                "host": "db.example.com",
                "port": 5432,
                "database": "test_db",
                "user": "testuser",
                "password": "secret123",
                "schema": "public"
            },
            "processing": {
                "batch_insert_size": 2000,
                "memory_threshold_mb": 2048,
                "max_square_meters": 150000,
                "price_percentile_low": 5,
                "price_percentile_high": 95
            },
            "reference_data": {
                "location_code_mapping_file": "./data/mapping.json",
                "valid_location_codes_file": "./data/locations.txt",
                "valid_property_types": ["residential", "commercial", "industrial"],
                "valid_seller_types": ["private", "developer", "institutional"]
            },
            "output": {
                "log_directory": "./logs",
                "reports_directory": "./reports"
            },
            "execution": {
                "mode": "incremental_load",
                "backup_on_full_load": False
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_file = f.name
        
        config = Config(temp_file)
        yield config
        
        Path(temp_file).unlink()
        Path("./logs").exists() and Path("./logs").rmdir() if not list(Path("./logs").iterdir()) else None
        Path("./reports").exists() and Path("./reports").rmdir() if not list(Path("./reports").iterdir()) else None
    
    def test_get_source_file_path(self, config):
        """Test getting source file path."""
        assert config.get_source_file_path() == "./data/test.csv"
    
    def test_get_chunk_size(self, config):
        """Test getting chunk size."""
        assert config.get_chunk_size() == 50000
    
    def test_get_database_credentials(self, config):
        """Test getting database credentials."""
        assert config.get_database_host() == "db.example.com"
        assert config.get_database_port() == 5432
        assert config.get_database_name() == "test_db"
        assert config.get_database_user() == "testuser"
        assert config.get_database_password() == "secret123"
        assert config.get_database_schema() == "public"
    
    def test_get_processing_settings(self, config):
        """Test getting processing settings."""
        assert config.get_batch_insert_size() == 2000
        assert config.get_memory_threshold_mb() == 2048
        assert config.get_max_square_meters() == 150000
        assert config.get_price_percentile_low() == 5
        assert config.get_price_percentile_high() == 95
    
    def test_get_reference_data(self, config):
        """Test getting reference data settings."""
        assert config.get_valid_property_types() == ["residential", "commercial", "industrial"]
        assert config.get_valid_seller_types() == ["private", "developer", "institutional"]
        assert config.get_location_code_mapping_file() == "./data/mapping.json"
        assert config.get_valid_location_codes_file() == "./data/locations.txt"
    
    def test_get_output_directories(self, config):
        """Test getting output directories."""
        assert config.get_log_directory() == "./logs"
        assert config.get_reports_directory() == "./reports"
    
    def test_get_execution_settings(self, config):
        """Test getting execution settings."""
        assert config.get_execution_mode() == "incremental_load"
        assert config.get_backup_on_full_load() is False
    
    def test_get_all_config(self, config):
        """Test getting entire configuration as dictionary."""
        all_config = config.get_all_config()
        assert 'source' in all_config
        assert 'database' in all_config
        assert all_config['source']['file_path'] == "./data/test.csv"
    
    def test_to_dict_redacts_password(self, config):
        """Test that to_dict redacts sensitive database password."""
        config_dict = config.to_dict()
        assert config_dict['database']['password'] == '***REDACTED***'
        assert config_dict['database']['user'] == 'testuser'


class TestConfigDefaultValues:
    """Test configuration default values."""
    
    @pytest.fixture
    def config_with_defaults(self):
        """Create configuration with optional fields using defaults."""
        config_data = {
            "source": {
                "file_path": "./data/test.csv",
                "chunk_size": 50000
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "user": "testuser",
                "password": "testpass",
                "schema": "public"
            },
            "processing": {
                "batch_insert_size": 1000,
                "memory_threshold_mb": 1024,
                "max_square_meters": 100000
                # Missing price_percentile_low and price_percentile_high
            },
            "reference_data": {
                "valid_property_types": ["residential"],
                "valid_seller_types": ["private"]
                # Missing optional file paths
            },
            "output": {
                "log_directory": "./logs",
                "reports_directory": "./reports"
            },
            "execution": {
                "mode": "full_load"
                # Missing backup_on_full_load
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_file = f.name
        
        config = Config(temp_file)
        yield config
        
        Path(temp_file).unlink()
        Path("./logs").exists() and Path("./logs").rmdir() if not list(Path("./logs").iterdir()) else None
        Path("./reports").exists() and Path("./reports").rmdir() if not list(Path("./reports").iterdir()) else None
    
    def test_price_percentile_defaults(self, config_with_defaults):
        """Test that price percentiles use default values when not provided."""
        assert config_with_defaults.get_price_percentile_low() == 1
        assert config_with_defaults.get_price_percentile_high() == 99
    
    def test_optional_file_paths_default_to_none(self, config_with_defaults):
        """Test that optional file paths default to None when not provided."""
        assert config_with_defaults.get_location_code_mapping_file() is None
        assert config_with_defaults.get_valid_location_codes_file() is None
    
    def test_backup_on_full_load_defaults_to_true(self, config_with_defaults):
        """Test that backup_on_full_load defaults to True."""
        assert config_with_defaults.get_backup_on_full_load() is True


class TestConfigDirectoryCreation:
    """Test automatic creation of output directories."""
    
    def test_creates_log_directory(self):
        """Test that log directory is created if it doesn't exist."""
        temp_dir = tempfile.mkdtemp()
        log_dir = Path(temp_dir) / "new_logs"
        reports_dir = Path(temp_dir) / "new_reports"
        
        assert not log_dir.exists()
        assert not reports_dir.exists()
        
        config_data = {
            "source": {"file_path": "./data/test.csv", "chunk_size": 50000},
            "database": {"host": "localhost", "port": 5432, "database": "test_db",
                        "user": "testuser", "password": "testpass", "schema": "public"},
            "processing": {"batch_insert_size": 1000, "memory_threshold_mb": 1024,
                          "max_square_meters": 100000},
            "reference_data": {"valid_property_types": ["residential"],
                             "valid_seller_types": ["private"]},
            "output": {
                "log_directory": str(log_dir),
                "reports_directory": str(reports_dir)
            },
            "execution": {"mode": "full_load"}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, dir=temp_dir) as f:
            json.dump(config_data, f)
            temp_file = f.name
        
        try:
            config = Config(temp_file)
            assert log_dir.exists()
            assert reports_dir.exists()
        finally:
            Path(temp_file).unlink()
            import shutil
            shutil.rmtree(temp_dir)
