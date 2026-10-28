#!/usr/bin/env python3
"""
Convenience Runner Script for Real Estate ETL Pipeline.

Usage:
    python run_pipeline.py
    python run_pipeline.py --config config.json
    python run_pipeline.py --limit 500000
    python run_pipeline.py --dry-run
"""

import sys
from etl_pipeline.main import main

if __name__ == '__main__':
    main()
