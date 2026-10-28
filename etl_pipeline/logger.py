"""
Logging module for ETL Pipeline

This module sets up and manages logging for the entire ETL pipeline.
Provides support for multiple log levels (DEBUG, INFO, WARNING, ERROR) with
both file and console output, including timestamps and contextual information.
"""

import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Optional


class ETLLogger:
    """
    Manages logging for the ETL pipeline.
    
    Configures logging to output to both file and console with:
    - Timestamped log files in a configured directory
    - Multiple log levels (DEBUG, INFO, WARNING, ERROR)
    - Consistent formatting with timestamps and context
    - Configurable console output
    """
    
    # Log level mapping
    LOG_LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
    }
    
    # Log format with timestamp, level, and message
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    
    # Single logger instance (singleton pattern)
    _instance = None
    _logger = None
    
    def __new__(cls):
        """Implement singleton pattern."""
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the logger (called only on first instantiation)."""
        if self._logger is None:
            self._logger = logging.getLogger('etl_pipeline')
            self._logger.setLevel(logging.DEBUG)
    
    def setup(
        self,
        log_directory: str,
        level: str = 'INFO',
        console_output: bool = True,
        console_level: str = 'INFO'
    ) -> str:
        """
        Set up logging to file and optionally console.
        
        Args:
            log_directory: Directory where log files will be stored
            level: Default log level (DEBUG, INFO, WARNING, ERROR)
            console_output: Whether to output to console
            console_level: Log level for console output
            
        Returns:
            Path to the created log file
            
        Raises:
            ValueError: If log level is invalid
            OSError: If log directory cannot be created
        """
        # Validate log level
        if level.upper() not in self.LOG_LEVELS:
            raise ValueError(f"Invalid log level: {level}. Must be one of {list(self.LOG_LEVELS.keys())}")
        
        # Create log directory if it doesn't exist
        log_path = Path(log_directory)
        try:
            log_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise OSError(f"Cannot create log directory '{log_directory}': {e}")
        
        # Generate timestamped log filename
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        log_filename = f'etl-pipeline-{timestamp}.log'
        log_filepath = log_path / log_filename
        
        # Create formatter
        formatter = logging.Formatter(self.LOG_FORMAT, datefmt=self.DATE_FORMAT)
        
        # Clear existing handlers
        self._logger.handlers.clear()
        
        # Set file handler
        try:
            file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
            file_handler.setLevel(self.LOG_LEVELS[level.upper()])
            file_handler.setFormatter(formatter)
            self._logger.addHandler(file_handler)
        except Exception as e:
            raise OSError(f"Cannot create log file '{log_filepath}': {e}")
        
        # Set console handler if requested
        if console_output:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self.LOG_LEVELS[console_level.upper()])
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)
        
        # Log initialization message
        self._logger.info(f"ETL Pipeline logging initialized. Log file: {log_filepath}")
        
        return str(log_filepath)
    
    def debug(self, message: str) -> None:
        """Log a debug message."""
        self._logger.debug(message)
    
    def info(self, message: str) -> None:
        """Log an info message."""
        self._logger.info(message)
    
    def warning(self, message: str) -> None:
        """Log a warning message."""
        self._logger.warning(message)
    
    def error(self, message: str) -> None:
        """Log an error message."""
        self._logger.error(message)
    
    def exception(self, message: str) -> None:
        """Log an error with exception traceback."""
        self._logger.exception(message)
    
    def get_logger(self) -> logging.Logger:
        """Get the underlying logger instance."""
        return self._logger


# Module-level convenience functions
def get_logger() -> ETLLogger:
    """Get or create the singleton ETL logger."""
    return ETLLogger()
