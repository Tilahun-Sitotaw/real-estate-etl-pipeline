"""
Main Orchestration Module for Real Estate ETL Pipeline.

Coordinates the end-to-end data pipeline:
Extraction (Streaming CSV in Chunks) -> Transformation (Cleaning & Enrichment) -> Loading (PostgreSQL Batch Insert)
"""

import argparse
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Optional

from etl_pipeline.config import Config, ConfigurationError
from etl_pipeline.logger import ETLLogger, get_logger
from etl_pipeline.extractor import DataExtractor, ExtractionError
from etl_pipeline.transformer import DataTransformer
from etl_pipeline.database import DatabaseManager, DatabaseError
from etl_pipeline.loader import DataLoader, LoaderError
from etl_pipeline.quality_report import QualityReportGenerator


class ETLPipeline:
    """
    Orchestrates the entire ETL workflow from raw CSV extraction to PostgreSQL persistence.
    """
    
    def __init__(self, config_path: Optional[str] = None, dry_run: bool = False, limit: Optional[int] = None):
        """
        Initialize the ETL pipeline orchestrator.
        
        Args:
            config_path: Path to configuration JSON
            dry_run: Run extraction and transformation without persisting to PostgreSQL
            limit: Maximum number of records to process (None for entire file)
        """
        self.config = Config(config_path)
        self.dry_run = dry_run
        # Use explicit limit from CLI or default limit from config (2,500,000 records)
        self.limit = limit if limit is not None else self.config.get_record_limit()
        
        # Initialize logging
        self.logger = ETLLogger()
        self.log_file = self.logger.setup(
            log_directory=self.config.get_log_directory(),
            level='INFO',
            console_output=True
        )
        
        self.db_manager: Optional[DatabaseManager] = None
        if not self.dry_run:
            self.db_manager = DatabaseManager(self.config.get_all_config().get('database', {}))

    def run(self) -> int:
        """
        Execute the complete pipeline.
        
        Returns:
            0 on success, 1 on fatal error
        """
        start_time = time.time()
        self.logger.info("=" * 70)
        self.logger.info("Starting Nationwide Real Estate Property ETL Pipeline")
        self.logger.info(f"Source Dataset: {self.config.get_source_file_path()}")
        self.logger.info(f"Execution Mode: {self.config.get_execution_mode()} (Dry Run: {self.dry_run})")
        if self.limit:
            self.logger.info(f"Record Limit: {self.limit:,} records")
        self.logger.info("=" * 70)

        # 1. Database Schema Initialization (if not dry run)
        if not self.dry_run and self.db_manager:
            try:
                self.logger.info("Connecting to PostgreSQL and initializing schema...")
                if self.db_manager.test_connection():
                    if self.config.get_backup_on_full_load() and self.config.get_execution_mode() == 'full_load':
                        self.db_manager.create_backup_table('properties')
                    self.db_manager.initialize_schema()
                else:
                    self.logger.warning(
                        "PostgreSQL is not reachable at specified host/port. "
                        "Switching to dry-run mode for data cleaning and reporting."
                    )
                    self.dry_run = True
            except Exception as e:
                self.logger.warning(
                    f"Database initialization encountered an error: {e}. "
                    "Continuing in dry-run mode to ensure data transformation and reporting complete."
                )
                self.dry_run = True

        # 2. Initialize Pipeline Components
        chunk_size = self.config.get_chunk_size()
        extractor = DataExtractor(
            file_path=self.config.get_source_file_path(),
            chunk_size=chunk_size
        )
        transformer = DataTransformer(config=self.config.get_all_config())
        loader = DataLoader(
            db_manager=self.db_manager,
            batch_size=self.config.get_batch_insert_size(),
            mode=self.config.get_execution_mode(),
            dry_run=self.dry_run
        )

        # 3. Stream & Process Chunks
        total_processed_records = 0
        try:
            for raw_chunk, chunk_stats in extractor.extract_chunks():
                # Apply optional limit
                if self.limit and total_processed_records >= self.limit:
                    self.logger.info(f"Reached processing limit of {self.limit:,} records.")
                    break
                    
                # Transform
                cleaned_chunk, trans_stats = transformer.transform_chunk(raw_chunk)
                
                # Load
                load_stats = loader.load_chunk(cleaned_chunk)
                
                total_processed_records += len(raw_chunk)
                
                self.logger.info(
                    f"Chunk {chunk_stats['chunk_number']} Completed -> "
                    f"Read: {chunk_stats['records_read']:,} | "
                    f"Cleaned & Retained: {len(cleaned_chunk):,} | "
                    f"Loaded: {load_stats['records_loaded']:,} | "
                    f"Cumulative: {total_processed_records:,}"
                )
                
                if self.limit and total_processed_records >= self.limit:
                    break
                    
        except KeyboardInterrupt:
            self.logger.warning("Pipeline execution interrupted by user.")
        except Exception as e:
            self.logger.exception(f"Fatal error during pipeline processing: {e}")
            return 1
        finally:
            if self.db_manager:
                self.db_manager.close()

        # 4. Generate Quality & Execution Report
        total_pipeline_time = time.time() - start_time
        report_gen = QualityReportGenerator(reports_dir=self.config.get_reports_directory())
        reports = report_gen.generate_consolidated_report(
            extraction_summary=extractor.get_extraction_summary(),
            transformation_summary=transformer.get_quality_summary(),
            load_summary=loader.get_load_summary(),
            pipeline_duration_seconds=total_pipeline_time
        )

        self.logger.info("=" * 70)
        self.logger.info("ETL Pipeline Execution Finished Successfully!")
        self.logger.info(f"Total Execution Time: {total_pipeline_time:.2f} seconds")
        self.logger.info(f"JSON Report Saved: {reports['json_path']}")
        self.logger.info(f"Markdown Report Saved: {reports['md_path']}")
        self.logger.info("=" * 70)
        return 0


def main():
    """Command-line entrypoint."""
    parser = argparse.ArgumentParser(
        description="Nationwide Real Estate Property Assessment and Sales ETL Pipeline"
    )
    parser.add_argument(
        '--config', '-c',
        type=str,
        default='config.json',
        help='Path to the configuration JSON file (default: config.json)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run extraction and transformation without loading to database'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=None,
        help='Limit number of records to process (useful for testing or staging)'
    )
    args = parser.parse_args()

    pipeline = ETLPipeline(
        config_path=args.config,
        dry_run=args.dry_run,
        limit=args.limit
    )
    sys.exit(pipeline.run())


if __name__ == '__main__':
    main()
