"""
ETL Pipeline Orchestrator
-------------------------
Coordinates the Extract, Transform, Load process:
- Orchestrates ETL steps
- Handles logging and error reporting
- Provides CLI interface

Author: Data Engineering Team
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from extract import DataExtractor
from transform import DataTransformer
from load import DatabaseLoader

# Configure logging
LOG_DIR = Path(__file__).parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

log_filename = LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Orchestrates the complete ETL pipeline for student data.
    """
    
    def __init__(
        self,
        data_source: str,
        source_type: str = 'csv',
        connection_string: Optional[str] = None
    ):
        """
        Initialize the ETL Pipeline.
        
        Args:
            data_source: Path to data file or URL
            source_type: Type of source ('csv', 'json', 'url', 'google_sheet')
            connection_string: Database connection string
        """
        self.data_source = data_source
        self.source_type = source_type
        self.connection_string = connection_string
        
        # Initialize components
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = None  # Lazy initialization
        
        # Pipeline state
        self.raw_data = None
        self.transformed_data = None
        self.load_stats = None
        self.pipeline_stats = {
            'start_time': None,
            'end_time': None,
            'extract_count': 0,
            'transform_count': 0,
            'load_stats': None,
            'errors': []
        }
        
        logger.info(f"ETL Pipeline initialized for source: {data_source}")
    
    def extract(self) -> bool:
        """
        Execute the extract phase.
        
        Returns:
            True if successful
        """
        logger.info("=" * 50)
        logger.info("EXTRACT PHASE STARTED")
        logger.info("=" * 50)
        
        try:
            if self.source_type == 'csv':
                self.raw_data = self.extractor.extract_csv(self.data_source)
            elif self.source_type == 'json':
                self.raw_data = self.extractor.extract_json(self.data_source)
            elif self.source_type == 'url':
                self.raw_data = self.extractor.extract_from_url(self.data_source)
            elif self.source_type == 'google_sheet':
                self.raw_data = self.extractor.extract_google_sheet(self.data_source)
            else:
                raise ValueError(f"Unknown source type: {self.source_type}")
            
            self.pipeline_stats['extract_count'] = len(self.raw_data)
            logger.info(f"EXTRACT COMPLETE: {len(self.raw_data)} records extracted")
            return True
            
        except Exception as e:
            logger.error(f"EXTRACT FAILED: {e}")
            self.pipeline_stats['errors'].append({
                'phase': 'extract',
                'error': str(e)
            })
            return False
    
    def transform(self) -> bool:
        """
        Execute the transform phase.
        
        Returns:
            True if successful
        """
        logger.info("=" * 50)
        logger.info("TRANSFORM PHASE STARTED")
        logger.info("=" * 50)
        
        if self.raw_data is None:
            logger.error("No raw data available for transformation")
            return False
        
        try:
            self.transformed_data = self.transformer.transform_students(self.raw_data)
            
            # Get validation report
            validation_report = self.transformer.get_validation_report()
            
            self.pipeline_stats['transform_count'] = len(self.transformed_data)
            self.pipeline_stats['validation_errors'] = validation_report['total_errors']
            
            logger.info(f"TRANSFORM COMPLETE: {len(self.transformed_data)} valid records")
            logger.info(f"Validation errors: {validation_report['total_errors']}")
            
            if validation_report['errors']:
                for error in validation_report['errors'][:10]:  # Log first 10 errors
                    logger.warning(f"Validation error - Row {error['row']}: {error['error']}")
            
            return True
            
        except Exception as e:
            logger.error(f"TRANSFORM FAILED: {e}")
            self.pipeline_stats['errors'].append({
                'phase': 'transform',
                'error': str(e)
            })
            return False
    
    def load(self) -> bool:
        """
        Execute the load phase.
        
        Returns:
            True if successful
        """
        logger.info("=" * 50)
        logger.info("LOAD PHASE STARTED")
        logger.info("=" * 50)
        
        if self.transformed_data is None or len(self.transformed_data) == 0:
            logger.error("No transformed data available for loading")
            return False
        
        try:
            # Initialize loader
            self.loader = DatabaseLoader(self.connection_string)
            
            # Test connection first
            if not self.loader.test_connection():
                raise ConnectionError("Database connection test failed")
            
            # Load data
            self.load_stats = self.loader.load_students_batch(self.transformed_data)
            self.pipeline_stats['load_stats'] = self.load_stats
            
            logger.info(f"LOAD COMPLETE: {self.load_stats['inserted']} inserted, "
                       f"{self.load_stats['updated']} updated, "
                       f"{self.load_stats['failed']} failed")
            
            return self.load_stats['failed'] == 0
            
        except Exception as e:
            logger.error(f"LOAD FAILED: {e}")
            self.pipeline_stats['errors'].append({
                'phase': 'load',
                'error': str(e)
            })
            return False
    
    def run(self) -> Dict:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            Pipeline statistics dictionary
        """
        logger.info("=" * 60)
        logger.info("ETL PIPELINE EXECUTION STARTED")
        logger.info("=" * 60)
        
        self.pipeline_stats['start_time'] = datetime.now()
        
        # Execute phases
        extract_success = self.extract()
        
        if extract_success:
            transform_success = self.transform()
        else:
            transform_success = False
        
        if transform_success:
            load_success = self.load()
        else:
            load_success = False
        
        self.pipeline_stats['end_time'] = datetime.now()
        self.pipeline_stats['duration'] = (
            self.pipeline_stats['end_time'] - self.pipeline_stats['start_time']
        ).total_seconds()
        
        # Final summary
        logger.info("=" * 60)
        logger.info("ETL PIPELINE EXECUTION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Duration: {self.pipeline_stats['duration']:.2f} seconds")
        logger.info(f"Records extracted: {self.pipeline_stats['extract_count']}")
        logger.info(f"Records transformed: {self.pipeline_stats['transform_count']}")
        
        if self.pipeline_stats['load_stats']:
            logger.info(f"Records loaded: {self.pipeline_stats['load_stats']['inserted'] + self.pipeline_stats['load_stats']['updated']}")
        
        if self.pipeline_stats['errors']:
            logger.warning(f"Pipeline completed with {len(self.pipeline_stats['errors'])} errors")
        else:
            logger.info("Pipeline completed successfully with no errors")
        
        return self.pipeline_stats
    
    def run_extract_transform(self) -> Dict:
        """
        Run only extract and transform phases (for testing).
        
        Returns:
            Pipeline statistics
        """
        self.pipeline_stats['start_time'] = datetime.now()
        
        if self.extract():
            self.transform()
        
        self.pipeline_stats['end_time'] = datetime.now()
        self.pipeline_stats['duration'] = (
            self.pipeline_stats['end_time'] - self.pipeline_stats['start_time']
        ).total_seconds()
        
        return self.pipeline_stats


def main():
    """
    CLI entry point for the ETL pipeline.
    """
    parser = argparse.ArgumentParser(
        description='Student Data ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python etl.py --source data/sample_students.csv
  python etl.py --source data/students.json --type json
  python etl.py --source https://example.com/data.csv --type url
  python etl.py --source SHEET_ID --type google_sheet
  python etl.py --source data/sample.csv --dry-run
        """
    )
    
    parser.add_argument(
        '--source', '-s',
        required=True,
        help='Data source (file path, URL, or Google Sheet ID)'
    )
    
    parser.add_argument(
        '--type', '-t',
        choices=['csv', 'json', 'url', 'google_sheet'],
        default='csv',
        help='Type of data source (default: csv)'
    )
    
    parser.add_argument(
        '--connection', '-c',
        help='Database connection string (overrides DATABASE_URL env var)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run extract and transform only (no database load)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create and run pipeline
    pipeline = ETLPipeline(
        data_source=args.source,
        source_type=args.type,
        connection_string=args.connection
    )
    
    if args.dry_run:
        print("\n*** DRY RUN MODE - No data will be loaded to database ***\n")
        stats = pipeline.run_extract_transform()
    else:
        stats = pipeline.run()
    
    # Print summary
    print("\n" + "=" * 50)
    print("PIPELINE SUMMARY")
    print("=" * 50)
    print(f"Duration: {stats['duration']:.2f} seconds")
    print(f"Extracted: {stats['extract_count']} records")
    print(f"Transformed: {stats['transform_count']} records")
    
    if stats.get('load_stats'):
        ls = stats['load_stats']
        print(f"Loaded: {ls['inserted']} inserted, {ls['updated']} updated, {ls['failed']} failed")
    
    if stats['errors']:
        print(f"\nERRORS ({len(stats['errors'])}):")
        for err in stats['errors']:
            print(f"  [{err['phase']}] {err['error']}")
    
    print(f"\nLog file: {log_filename}")
    
    return 0 if not stats['errors'] else 1


if __name__ == "__main__":
    sys.exit(main())
