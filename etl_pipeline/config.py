"""
Configuration management module for Real Estate ETL Pipeline.

Handles reading, parsing, and validating configuration from JSON files and
environment variables. Provides validated access to all pipeline settings including
data sources, PostgreSQL connection parameters, batch processing thresholds,
and reporting directories.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, List


class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing required settings."""
    pass


class Config:
    """
    Manages ETL pipeline configuration.
    
    Reads configuration from a JSON file and provides validated access to all
    pipeline settings with sane defaults and environment variable overrides.
    """
    
    REQUIRED_SECTIONS = {'source', 'database', 'processing', 'output', 'execution'}
    
    DEFAULT_CONFIG: Dict[str, Any] = {
        'source': {
            'file_path': './Dataset/uk_property_price_paid_geo.csv',
            'chunk_size': 50000
        },
        'database': {
            'host': 'localhost',
            'port': 5432,
            'database': 'real_estate',
            'user': 'postgres',
            'password': 'postgres',
            'schema': 'public'
        },
        'processing': {
            'batch_insert_size': 1000,
            'memory_threshold_mb': 1024,
            'max_square_meters': 100000,
            'price_percentile_low': 1,
            'price_percentile_high': 99
        },
        'reference_data': {
            'valid_property_types': [
                'Detached',
                'Semi-Detached',
                'Terraced',
                'Flat/Maisonette',
                'Other'
            ],
            'valid_seller_types': ['private', 'developer', 'institutional'],
            'valid_regions': [
                'North East', 'North West', 'Yorkshire and The Humber',
                'East Midlands', 'West Midlands', 'East of England',
                'London', 'South East', 'South West', 'Wales',
                'Scotland', 'Northern Ireland'
            ],
            'valid_countries': ['England', 'Wales', 'Scotland', 'Northern Ireland']
        },
        'output': {
            'log_directory': './logs',
            'reports_directory': './reports'
        },
        'execution': {
            'mode': 'full_load',
            'backup_on_full_load': True
        }
    }
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration from a JSON file or use default settings.
        
        Args:
            config_path: Path to the configuration JSON file. If None, checks 'config.json'.
        """
        self.config_path = Path(config_path) if config_path else Path('config.json')
        self._config: Dict[str, Any] = json.loads(json.dumps(self.DEFAULT_CONFIG))
        
        if self.config_path.exists():
            self._load_from_file()
        else:
            # Create directories for default config
            self._create_directories()
    
    def _load_from_file(self) -> None:
        """Load configuration from JSON file and merge with defaults."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                user_config = json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON in configuration file '{self.config_path}': {e}")
        except Exception as e:
            raise ConfigurationError(f"Error reading configuration file '{self.config_path}': {e}")
        
        # Deep merge user config into default config
        for section, values in user_config.items():
            if isinstance(values, dict) and section in self._config:
                self._config[section].update(values)
            else:
                self._config[section] = values
        
        # Environment variable overrides
        self._apply_env_overrides()
        self._create_directories()
    
    def _apply_env_overrides(self) -> None:
        """Apply environment variables if present."""
        if os.getenv('DB_HOST'):
            self._config['database']['host'] = os.getenv('DB_HOST')
        if os.getenv('DB_PORT'):
            self._config['database']['port'] = int(os.getenv('DB_PORT', '5432'))
        if os.getenv('DB_NAME'):
            self._config['database']['database'] = os.getenv('DB_NAME')
        if os.getenv('DB_USER'):
            self._config['database']['user'] = os.getenv('DB_USER')
        if os.getenv('DB_PASSWORD'):
            self._config['database']['password'] = os.getenv('DB_PASSWORD')
    
    def _create_directories(self) -> None:
        """Create output directories if they do not exist."""
        log_dir = Path(self._config.get('output', {}).get('log_directory', './logs'))
        reports_dir = Path(self._config.get('output', {}).get('reports_directory', './reports'))
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            reports_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise ConfigurationError(f"Error creating output directories: {e}")
    
    # Getters
    def get_source_file_path(self) -> str:
        return self._config['source']['file_path']
    
    def get_chunk_size(self) -> int:
        return int(self._config['source'].get('chunk_size', 50000))

    def get_record_limit(self) -> Optional[int]:
        """Get default record limit for pipeline runs (e.g. 2,500,000)."""
        limit = self._config.get('source', {}).get('record_limit')
        return int(limit) if limit is not None else 2500000
    
    def get_database_host(self) -> str:
        return self._config['database']['host']
    
    def get_database_port(self) -> int:
        return int(self._config['database']['port'])
    
    def get_database_name(self) -> str:
        return self._config['database']['database']
    
    def get_database_user(self) -> str:
        return self._config['database']['user']
    
    def get_database_password(self) -> str:
        return self._config['database']['password']
    
    def get_database_schema(self) -> str:
        return self._config['database'].get('schema', 'public')
    
    def get_batch_insert_size(self) -> int:
        return int(self._config['processing'].get('batch_insert_size', 1000))
    
    def get_memory_threshold_mb(self) -> int:
        return int(self._config['processing'].get('memory_threshold_mb', 1024))
    
    def get_max_square_meters(self) -> float:
        return float(self._config['processing'].get('max_square_meters', 100000.0))
    
    def get_price_percentile_low(self) -> int:
        return int(self._config['processing'].get('price_percentile_low', 1))
    
    def get_price_percentile_high(self) -> int:
        return int(self._config['processing'].get('price_percentile_high', 99))
    
    def get_valid_property_types(self) -> List[str]:
        return self._config.get('reference_data', {}).get('valid_property_types', [
            'Detached', 'Semi-Detached', 'Terraced', 'Flat/Maisonette', 'Other'
        ])
    
    def get_valid_seller_types(self) -> List[str]:
        return self._config.get('reference_data', {}).get('valid_seller_types', ['private', 'developer', 'institutional'])
    
    def get_valid_regions(self) -> List[str]:
        return self._config.get('reference_data', {}).get('valid_regions', [])
    
    def get_valid_countries(self) -> List[str]:
        return self._config.get('reference_data', {}).get('valid_countries', [])
    
    def get_location_code_mapping_file(self) -> Optional[str]:
        return self._config.get('reference_data', {}).get('location_code_mapping_file')
    
    def get_valid_location_codes_file(self) -> Optional[str]:
        return self._config.get('reference_data', {}).get('valid_location_codes_file')
    
    def get_log_directory(self) -> str:
        return self._config['output']['log_directory']
    
    def get_reports_directory(self) -> str:
        return self._config['output']['reports_directory']
    
    def get_execution_mode(self) -> str:
        return self._config['execution'].get('mode', 'full_load')
    
    def get_backup_on_full_load(self) -> bool:
        return bool(self._config['execution'].get('backup_on_full_load', True))
    
    def get_all_config(self) -> Dict[str, Any]:
        return json.loads(json.dumps(self._config))
    
    def to_dict(self) -> Dict[str, Any]:
        cfg_copy = json.loads(json.dumps(self._config))
        if 'database' in cfg_copy and 'password' in cfg_copy['database']:
            cfg_copy['database']['password'] = '***REDACTED***'
        return cfg_copy
