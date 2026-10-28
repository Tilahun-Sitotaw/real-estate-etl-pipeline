#!/usr/bin/env python
"""Test runner script for the ETL Pipeline extraction module."""

import sys
import subprocess

# Install dependencies first
print("Installing dependencies...")
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pandas", "psutil", "pytest", "pytest-cov"])

# Run tests with coverage
print("\nRunning tests with coverage...")
result = subprocess.run([
    sys.executable, "-m", "pytest",
    "tests/test_extractor.py",
    "-v",
    "--tb=short",
    "--cov=etl_pipeline.extractor",
    "--cov-report=term-missing"
], cwd=".")

sys.exit(result.returncode)
