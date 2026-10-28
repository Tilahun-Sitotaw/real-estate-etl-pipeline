"""
Unit tests for ETL Pipeline logging module.

Tests logging setup, file creation, and log level functionality.
"""

import pytest
import tempfile
import logging
from pathlib import Path
from etl_pipeline.logger import ETLLogger, get_logger


class TestLoggerSetup:
    """Test logger initialization and setup."""
    
    def test_logger_singleton(self):
        """Test that ETLLogger implements singleton pattern."""
        logger1 = ETLLogger()
        logger2 = ETLLogger()
        assert logger1 is logger2
    
    def test_get_logger_convenience_function(self):
        """Test get_logger convenience function."""
        logger = get_logger()
        assert isinstance(logger, ETLLogger)
    
    def test_setup_creates_log_directory(self):
        """Test that setup creates log directory if it doesn't exist."""
        temp_dir = tempfile.mkdtemp()
        log_dir = Path(temp_dir) / "logs"
        
        assert not log_dir.exists()
        
        logger = ETLLogger()
        log_file = logger.setup(str(log_dir), console_output=False)
        
        assert log_dir.exists()
        assert Path(log_file).exists()
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_setup_creates_timestamped_log_file(self):
        """Test that setup creates log file with timestamp."""
        temp_dir = tempfile.mkdtemp()
        
        logger = ETLLogger()
        log_file = logger.setup(temp_dir, console_output=False)
        
        # Check that file is created with expected format
        assert Path(log_file).exists()
        assert 'etl-pipeline-' in log_file
        assert log_file.endswith('.log')
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_setup_with_invalid_log_level(self):
        """Test that invalid log level raises ValueError."""
        temp_dir = tempfile.mkdtemp()
        
        logger = ETLLogger()
        with pytest.raises(ValueError, match="Invalid log level"):
            logger.setup(temp_dir, level='INVALID')
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_setup_with_nonexistent_directory_raises_error(self):
        """Test that non-creatable directory raises OSError."""
        # Use a path that cannot be created (e.g., invalid path on Windows)
        invalid_path = "/root/nonexistent/path/that/cannot/be/created/logs"
        
        logger = ETLLogger()
        with pytest.raises(OSError):
            logger.setup(invalid_path, console_output=False)


class TestLoggerLogging:
    """Test logging functionality."""
    
    @pytest.fixture
    def logger_instance(self):
        """Create a logger instance with temporary directory."""
        temp_dir = tempfile.mkdtemp()
        logger = ETLLogger()
        log_file = logger.setup(temp_dir, level='DEBUG', console_output=False)
        yield logger, Path(log_file)
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_debug_logging(self, logger_instance):
        """Test debug level logging."""
        logger, log_file = logger_instance
        logger.debug("Test debug message")
        
        log_content = log_file.read_text()
        assert "Test debug message" in log_content
        assert "DEBUG" in log_content
    
    def test_info_logging(self, logger_instance):
        """Test info level logging."""
        logger, log_file = logger_instance
        logger.info("Test info message")
        
        log_content = log_file.read_text()
        assert "Test info message" in log_content
        assert "INFO" in log_content
    
    def test_warning_logging(self, logger_instance):
        """Test warning level logging."""
        logger, log_file = logger_instance
        logger.warning("Test warning message")
        
        log_content = log_file.read_text()
        assert "Test warning message" in log_content
        assert "WARNING" in log_content
    
    def test_error_logging(self, logger_instance):
        """Test error level logging."""
        logger, log_file = logger_instance
        logger.error("Test error message")
        
        log_content = log_file.read_text()
        assert "Test error message" in log_content
        assert "ERROR" in log_content
    
    def test_exception_logging(self, logger_instance):
        """Test exception logging."""
        logger, log_file = logger_instance
        
        try:
            raise ValueError("Test exception")
        except ValueError:
            logger.exception("An error occurred")
        
        log_content = log_file.read_text()
        assert "An error occurred" in log_content
        assert "ValueError" in log_content
        assert "Test exception" in log_content
    
    def test_timestamp_in_log_output(self, logger_instance):
        """Test that timestamps are included in log output."""
        logger, log_file = logger_instance
        logger.info("Test timestamp message")
        
        log_content = log_file.read_text()
        # Check for timestamp pattern YYYY-MM-DD HH:MM:SS
        import re
        timestamp_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
        assert re.search(timestamp_pattern, log_content)
    
    def test_logger_name_in_output(self, logger_instance):
        """Test that logger name appears in output."""
        logger, log_file = logger_instance
        logger.info("Test logger name")
        
        log_content = log_file.read_text()
        assert "etl_pipeline" in log_content


class TestLoggerLevels:
    """Test different log levels and filtering."""
    
    def test_debug_level_logs_all_messages(self):
        """Test that DEBUG level logs all messages."""
        temp_dir = tempfile.mkdtemp()
        
        logger = ETLLogger()
        log_file = logger.setup(temp_dir, level='DEBUG', console_output=False)
        
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        
        log_content = Path(log_file).read_text()
        assert "Debug message" in log_content
        assert "Info message" in log_content
        assert "Warning message" in log_content
        assert "Error message" in log_content
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_info_level_excludes_debug(self):
        """Test that INFO level does not log DEBUG messages."""
        temp_dir = tempfile.mkdtemp()
        
        logger = ETLLogger()
        log_file = logger.setup(temp_dir, level='INFO', console_output=False)
        
        logger.debug("Debug message")
        logger.info("Info message")
        
        log_content = Path(log_file).read_text()
        assert "Debug message" not in log_content
        assert "Info message" in log_content
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_warning_level_only_warnings_and_errors(self):
        """Test that WARNING level only logs WARNING and ERROR."""
        temp_dir = tempfile.mkdtemp()
        
        logger = ETLLogger()
        log_file = logger.setup(temp_dir, level='WARNING', console_output=False)
        
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        
        log_content = Path(log_file).read_text()
        assert "Info message" not in log_content
        assert "Warning message" in log_content
        assert "Error message" in log_content
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_error_level_only_errors(self):
        """Test that ERROR level only logs ERROR messages."""
        temp_dir = tempfile.mkdtemp()
        
        logger = ETLLogger()
        log_file = logger.setup(temp_dir, level='ERROR', console_output=False)
        
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        
        log_content = Path(log_file).read_text()
        assert "Info message" not in log_content
        assert "Warning message" not in log_content
        assert "Error message" in log_content
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)


class TestLoggerConsoleOutput:
    """Test console output configuration."""
    
    def test_console_output_disabled(self):
        """Test that console output can be disabled."""
        temp_dir = tempfile.mkdtemp()
        
        logger = ETLLogger()
        log_file = logger.setup(temp_dir, console_output=False)
        
        # Check that only file handler is set
        file_handlers = [h for h in logger.get_logger().handlers
                        if isinstance(h, logging.FileHandler)]
        stream_handlers = [h for h in logger.get_logger().handlers
                          if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)]
        
        assert len(file_handlers) >= 1
        assert len(stream_handlers) == 0
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_console_output_enabled(self):
        """Test that console output can be enabled."""
        temp_dir = tempfile.mkdtemp()
        
        logger = ETLLogger()
        log_file = logger.setup(temp_dir, console_output=True)
        
        # Check that both file and stream handlers exist
        file_handlers = [h for h in logger.get_logger().handlers
                        if isinstance(h, logging.FileHandler)]
        console_handlers = [h for h in logger.get_logger().handlers
                           if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)]
        
        assert len(file_handlers) >= 1
        assert len(console_handlers) >= 1
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)


class TestLoggerIntegration:
    """Integration tests for logger functionality."""
    
    def test_multiple_log_messages(self):
        """Test logging multiple messages in sequence."""
        temp_dir = tempfile.mkdtemp()
        
        logger = ETLLogger()
        log_file = logger.setup(temp_dir, console_output=False)
        
        messages = [
            ("info", "Starting ETL pipeline"),
            ("debug", "Reading configuration"),
            ("info", "Processing chunk 1"),
            ("warning", "Duplicate record detected"),
            ("info", "Processing chunk 2"),
            ("error", "Database connection failed"),
        ]
        
        for level, msg in messages:
            getattr(logger, level)(msg)
        
        log_content = Path(log_file).read_text()
        for _, msg in messages:
            assert msg in log_content
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_get_logger_returns_logging_logger(self):
        """Test that get_logger().get_logger() returns logging.Logger."""
        logger = get_logger()
        underlying_logger = logger.get_logger()
        assert isinstance(underlying_logger, logging.Logger)
