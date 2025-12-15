"""
Extract Module
--------------
Handles data extraction from various sources:
- CSV files
- JSON files
- Google Sheets (via exported CSV URL)

Author: Data Engineering Team
"""

import pandas as pd
import logging
import json
from pathlib import Path
from typing import Union, Optional
import requests
from io import StringIO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataExtractor:
    """
    Extracts data from various sources into pandas DataFrames.
    Supports CSV, JSON, and remote URLs.
    """
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize the DataExtractor.
        
        Args:
            data_dir: Directory containing local data files
        """
        self.data_dir = Path(data_dir)
        logger.info(f"DataExtractor initialized with data directory: {self.data_dir}")
    
    def extract_csv(
        self, 
        filepath: Union[str, Path], 
        encoding: str = 'utf-8',
        **kwargs
    ) -> pd.DataFrame:
        """
        Extract data from a CSV file.
        
        Args:
            filepath: Path to the CSV file
            encoding: File encoding (default: utf-8)
            **kwargs: Additional arguments passed to pd.read_csv
            
        Returns:
            DataFrame containing the extracted data
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            logger.error(f"CSV file not found: {filepath}")
            raise FileNotFoundError(f"CSV file not found: {filepath}")
        
        logger.info(f"Extracting data from CSV: {filepath}")
        
        try:
            df = pd.read_csv(filepath, encoding=encoding, **kwargs)
            logger.info(f"Successfully extracted {len(df)} rows from {filepath}")
            return df
        except pd.errors.EmptyDataError:
            logger.warning(f"Empty CSV file: {filepath}")
            raise
        except Exception as e:
            logger.error(f"Error extracting CSV: {e}")
            raise
    
    def extract_json(
        self, 
        filepath: Union[str, Path],
        orient: str = 'records',
        **kwargs
    ) -> pd.DataFrame:
        """
        Extract data from a JSON file.
        
        Args:
            filepath: Path to the JSON file
            orient: JSON structure orientation
            **kwargs: Additional arguments passed to pd.read_json
            
        Returns:
            DataFrame containing the extracted data
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            logger.error(f"JSON file not found: {filepath}")
            raise FileNotFoundError(f"JSON file not found: {filepath}")
        
        logger.info(f"Extracting data from JSON: {filepath}")
        
        try:
            df = pd.read_json(filepath, orient=orient, **kwargs)
            logger.info(f"Successfully extracted {len(df)} rows from {filepath}")
            return df
        except ValueError as e:
            logger.error(f"Invalid JSON format: {e}")
            raise
        except Exception as e:
            logger.error(f"Error extracting JSON: {e}")
            raise
    
    def extract_from_url(
        self, 
        url: str,
        file_type: str = 'csv',
        timeout: int = 30,
        **kwargs
    ) -> pd.DataFrame:
        """
        Extract data from a remote URL (e.g., Google Sheets published CSV).
        
        Args:
            url: URL to fetch data from
            file_type: Type of file ('csv' or 'json')
            timeout: Request timeout in seconds
            **kwargs: Additional arguments passed to pandas read function
            
        Returns:
            DataFrame containing the extracted data
        """
        logger.info(f"Extracting data from URL: {url}")
        
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            
            content = StringIO(response.text)
            
            if file_type.lower() == 'csv':
                df = pd.read_csv(content, **kwargs)
            elif file_type.lower() == 'json':
                df = pd.read_json(content, **kwargs)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            logger.info(f"Successfully extracted {len(df)} rows from URL")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from URL: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing data from URL: {e}")
            raise
    
    def extract_google_sheet(
        self,
        sheet_id: str,
        sheet_name: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Extract data from a published Google Sheet.
        
        The Google Sheet must be published to the web as CSV.
        
        Args:
            sheet_id: The Google Sheet ID from the URL
            sheet_name: Optional sheet name (gid parameter)
            **kwargs: Additional arguments passed to pd.read_csv
            
        Returns:
            DataFrame containing the extracted data
        """
        # Construct Google Sheets CSV export URL
        base_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        
        if sheet_name:
            base_url += f"&gid={sheet_name}"
        
        logger.info(f"Extracting data from Google Sheet: {sheet_id}")
        return self.extract_from_url(base_url, file_type='csv', **kwargs)
    
    def extract_dict_list(self, data: list) -> pd.DataFrame:
        """
        Extract data from a list of dictionaries (e.g., from API response).
        
        Args:
            data: List of dictionaries
            
        Returns:
            DataFrame containing the data
        """
        logger.info(f"Extracting data from dictionary list: {len(data)} records")
        
        try:
            df = pd.DataFrame(data)
            logger.info(f"Successfully created DataFrame with {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error creating DataFrame from dict list: {e}")
            raise


def extract_students_csv(filepath: str = "data/sample_students.csv") -> pd.DataFrame:
    """
    Convenience function to extract student data from CSV.
    
    Args:
        filepath: Path to the student CSV file
        
    Returns:
        DataFrame with student data
    """
    extractor = DataExtractor()
    return extractor.extract_csv(filepath)


if __name__ == "__main__":
    # Test extraction
    extractor = DataExtractor()
    
    # Test CSV extraction
    try:
        df = extractor.extract_csv("data/sample_students.csv")
        print(f"Extracted {len(df)} students from CSV")
        print(df.head())
    except FileNotFoundError:
        print("Sample CSV file not found - create it first")
