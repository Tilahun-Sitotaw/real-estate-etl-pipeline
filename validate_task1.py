"""
Validation script for Task 1: Project Structure and Configuration Management

This script validates that all required components for Task 1 have been created
and are properly structured, without requiring external dependencies.
"""

import os
import json
import sys
from pathlib import Path


def validate_package_structure():
    """Validate that etl_pipeline package exists with __init__.py."""
    print("Validating package structure...")
    
    etl_pipeline_dir = Path("etl_pipeline")
    if not etl_pipeline_dir.exists():
        print("❌ FAILED: etl_pipeline directory does not exist")
        return False
    
    if not etl_pipeline_dir.is_dir():
        print("❌ FAILED: etl_pipeline is not a directory")
        return False
    
    init_file = etl_pipeline_dir / "__init__.py"
    if not init_file.exists():
        print("❌ FAILED: etl_pipeline/__init__.py does not exist")
        return False
    
    print("✓ etl_pipeline package structure is valid")
    return True


def validate_config_module():
    """Validate that config.py exists and has required components."""
    print("Validating config.py module...")
    
    config_file = Path("etl_pipeline/config.py")
    if not config_file.exists():
        print("❌ FAILED: config.py does not exist")
        return False
    
    content = config_file.read_text()
    
    required_classes = ['ConfigurationError', 'Config']
    required_methods = [
        'get_source_file_path',
        'get_chunk_size',
        'get_database_host',
        'get_database_port',
        'get_database_name',
        'get_database_user',
        'get_database_password',
        'get_database_schema',
        'get_batch_insert_size',
        'get_memory_threshold_mb',
        'get_max_square_meters',
        'get_valid_property_types',
        'get_valid_seller_types',
        'get_log_directory',
        'get_reports_directory',
        'get_execution_mode',
    ]
    
    for cls in required_classes:
        if f"class {cls}" not in content:
            print(f"❌ FAILED: Missing class {cls} in config.py")
            return False
    
    for method in required_methods:
        if f"def {method}" not in content:
            print(f"❌ FAILED: Missing method {method} in config.py")
            return False
    
    print("✓ config.py has all required classes and methods")
    return True


def validate_logger_module():
    """Validate that logger.py exists and has required components."""
    print("Validating logger.py module...")
    
    logger_file = Path("etl_pipeline/logger.py")
    if not logger_file.exists():
        print("❌ FAILED: logger.py does not exist")
        return False
    
    content = logger_file.read_text()
    
    required_classes = ['ETLLogger']
    required_methods = ['setup', 'debug', 'info', 'warning', 'error', 'exception']
    
    for cls in required_classes:
        if f"class {cls}" not in content:
            print(f"❌ FAILED: Missing class {cls} in logger.py")
            return False
    
    for method in required_methods:
        if f"def {method}" not in content:
            print(f"❌ FAILED: Missing method {method} in logger.py")
            return False
    
    if "get_logger()" not in content:
        print("❌ FAILED: Missing get_logger() function in logger.py")
        return False
    
    print("✓ logger.py has all required classes and methods")
    return True


def validate_sample_config():
    """Validate that config.json exists and is valid JSON with required fields."""
    print("Validating config.json...")
    
    config_file = Path("config.json")
    if not config_file.exists():
        print("❌ FAILED: config.json does not exist")
        return False
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except json.JSONDecodeError:
        print("❌ FAILED: config.json is not valid JSON")
        return False
    
    required_sections = ['source', 'database', 'processing', 'reference_data', 'output', 'execution']
    for section in required_sections:
        if section not in config:
            print(f"❌ FAILED: Missing section '{section}' in config.json")
            return False
    
    # Validate specific required fields
    required_fields = {
        'source': ['file_path', 'chunk_size'],
        'database': ['host', 'port', 'database', 'user', 'password', 'schema'],
        'processing': ['batch_insert_size', 'memory_threshold_mb', 'max_square_meters'],
        'reference_data': ['valid_property_types', 'valid_seller_types'],
        'output': ['log_directory', 'reports_directory'],
        'execution': ['mode']
    }
    
    for section, fields in required_fields.items():
        for field in fields:
            if field not in config[section]:
                print(f"❌ FAILED: Missing field '{field}' in config.json section '{section}'")
                return False
    
    print("✓ config.json is valid and contains all required settings")
    return True


def validate_requirements_txt():
    """Validate that requirements.txt exists and contains required dependencies."""
    print("Validating requirements.txt...")
    
    req_file = Path("requirements.txt")
    if not req_file.exists():
        print("❌ FAILED: requirements.txt does not exist")
        return False
    
    content = req_file.read_text()
    
    required_packages = ['pandas', 'psycopg2-binary', 'python-dotenv', 'pytest', 'pytest-cov']
    for package in required_packages:
        if package not in content:
            print(f"❌ FAILED: Missing package {package} in requirements.txt")
            return False
    
    print("✓ requirements.txt contains all required dependencies")
    return True


def validate_test_files():
    """Validate that test files exist."""
    print("Validating test files...")
    
    tests_dir = Path("tests")
    if not tests_dir.exists():
        print("❌ FAILED: tests directory does not exist")
        return False
    
    required_test_files = ['__init__.py', 'test_config.py', 'test_logger.py']
    for test_file in required_test_files:
        test_path = tests_dir / test_file
        if not test_path.exists():
            print(f"❌ FAILED: Missing {test_file} in tests directory")
            return False
    
    # Validate test file content
    config_test_content = (tests_dir / 'test_config.py').read_text()
    if 'class TestConfigLoading' not in config_test_content:
        print("❌ FAILED: test_config.py missing TestConfigLoading class")
        return False
    
    logger_test_content = (tests_dir / 'test_logger.py').read_text()
    if 'class TestLoggerSetup' not in logger_test_content:
        print("❌ FAILED: test_logger.py missing TestLoggerSetup class")
        return False
    
    print("✓ Test files are valid")
    return True


def validate_imports():
    """Validate that modules can be imported (at module definition level)."""
    print("Validating module imports...")
    
    try:
        # Check if we can at least parse the Python files
        import ast
        
        config_file = Path("etl_pipeline/config.py")
        logger_file = Path("etl_pipeline/logger.py")
        
        try:
            ast.parse(config_file.read_text())
            print("✓ config.py has valid Python syntax")
        except SyntaxError as e:
            print(f"❌ FAILED: config.py has syntax error: {e}")
            return False
        
        try:
            ast.parse(logger_file.read_text())
            print("✓ logger.py has valid Python syntax")
        except SyntaxError as e:
            print(f"❌ FAILED: logger.py has syntax error: {e}")
            return False
        
        return True
    except Exception as e:
        print(f"❌ FAILED: Error validating imports: {e}")
        return False


def main():
    """Run all validations for Task 1."""
    print("=" * 60)
    print("TASK 1 VALIDATION: Set Up Project Structure and Configuration")
    print("=" * 60)
    print()
    
    validations = [
        validate_package_structure,
        validate_config_module,
        validate_logger_module,
        validate_sample_config,
        validate_requirements_txt,
        validate_test_files,
        validate_imports,
    ]
    
    results = []
    for validation in validations:
        try:
            result = validation()
            results.append(result)
        except Exception as e:
            print(f"❌ FAILED: {validation.__name__}: {e}")
            results.append(False)
        print()
    
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"VALIDATION SUMMARY: {passed}/{total} checks passed")
    print("=" * 60)
    
    if all(results):
        print("✓ All Task 1 requirements validated successfully!")
        return 0
    else:
        print("❌ Some validations failed. Please review the output above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
