"""
Transform Module
----------------
Handles data transformation operations:
- Data validation
- Type casting
- Deduplication
- Data cleaning
- Business rule application

Author: Data Engineering Team
"""

import pandas as pd
import numpy as np
import logging
import re
from typing import List, Dict, Optional, Callable
from datetime import datetime, date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transforms extracted data for loading into the database.
    Applies validation, cleaning, and business rules.
    """
    
    # Valid values for categorical fields
    VALID_GENDERS = {'Male', 'Female', 'Other', 'Prefer not to say'}
    VALID_STATUSES = {'Active', 'Inactive', 'Graduated', 'Suspended', 'On Leave'}
    VALID_DEPARTMENTS = {'CS', 'MATH', 'PHY', 'ENG', 'BIO', 'CHEM', 'ECON', 'PSY'}
    
    # Email regex pattern
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    
    def __init__(self):
        """Initialize the DataTransformer."""
        self.validation_errors: List[Dict] = []
        self.transformation_log: List[str] = []
        logger.info("DataTransformer initialized")
    
    def validate_email(self, email: str) -> bool:
        """
        Validate email format.
        
        Args:
            email: Email address to validate
            
        Returns:
            True if valid, False otherwise
        """
        if pd.isna(email):
            return False
        return bool(self.EMAIL_PATTERN.match(str(email).strip()))
    
    def validate_phone(self, phone: str) -> bool:
        """
        Validate phone number format (basic validation).
        
        Args:
            phone: Phone number to validate
            
        Returns:
            True if valid or empty, False if invalid format
        """
        if pd.isna(phone) or str(phone).strip() == '':
            return True  # Phone is optional
        
        # Remove common formatting characters
        cleaned = re.sub(r'[\s\-$$$$\+]', '', str(phone))
        
        # Should be 10-15 digits
        return bool(re.match(r'^\d{10,15}$', cleaned))
    
    def validate_gpa(self, gpa: float) -> bool:
        """
        Validate GPA value.
        
        Args:
            gpa: GPA value to validate
            
        Returns:
            True if valid (0.0-4.0) or NaN, False otherwise
        """
        if pd.isna(gpa):
            return True  # GPA can be null for new students
        
        try:
            gpa_float = float(gpa)
            return 0.0 <= gpa_float <= 4.0
        except (ValueError, TypeError):
            return False
    
    def validate_date(self, date_value: str, min_year: int = 1950, max_year: int = None) -> bool:
        """
        Validate date format and range.
        
        Args:
            date_value: Date string to validate
            min_year: Minimum acceptable year
            max_year: Maximum acceptable year (default: current year)
            
        Returns:
            True if valid or empty, False if invalid
        """
        if pd.isna(date_value) or str(date_value).strip() == '':
            return True
        
        max_year = max_year or datetime.now().year
        
        try:
            if isinstance(date_value, (datetime, date)):
                dt = date_value
            else:
                # Try common date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                    try:
                        dt = datetime.strptime(str(date_value), fmt)
                        break
                    except ValueError:
                        continue
                else:
                    return False
            
            return min_year <= dt.year <= max_year
            
        except Exception:
            return False
    
    def clean_string(self, value: str) -> Optional[str]:
        """
        Clean and normalize string values.
        
        Args:
            value: String to clean
            
        Returns:
            Cleaned string or None if empty
        """
        if pd.isna(value):
            return None
        
        cleaned = str(value).strip()
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        return cleaned if cleaned else None
    
    def normalize_name(self, name: str) -> Optional[str]:
        """
        Normalize name fields (proper case).
        
        Args:
            name: Name to normalize
            
        Returns:
            Normalized name or None
        """
        cleaned = self.clean_string(name)
        if cleaned:
            return cleaned.title()
        return None
    
    def normalize_email(self, email: str) -> Optional[str]:
        """
        Normalize email (lowercase).
        
        Args:
            email: Email to normalize
            
        Returns:
            Normalized email or None
        """
        cleaned = self.clean_string(email)
        if cleaned:
            return cleaned.lower()
        return None
    
    def deduplicate(
        self, 
        df: pd.DataFrame, 
        subset: List[str], 
        keep: str = 'first'
    ) -> pd.DataFrame:
        """
        Remove duplicate records based on specified columns.
        
        Args:
            df: DataFrame to deduplicate
            subset: Columns to check for duplicates
            keep: Which duplicate to keep ('first', 'last', or False)
            
        Returns:
            Deduplicated DataFrame
        """
        initial_count = len(df)
        df_deduped = df.drop_duplicates(subset=subset, keep=keep)
        removed_count = initial_count - len(df_deduped)
        
        if removed_count > 0:
            logger.info(f"Removed {removed_count} duplicate records based on {subset}")
            self.transformation_log.append(
                f"Deduplication: removed {removed_count} records"
            )
        
        return df_deduped
    
    def cast_types(self, df: pd.DataFrame, type_mapping: Dict[str, type]) -> pd.DataFrame:
        """
        Cast columns to specified types.
        
        Args:
            df: DataFrame to transform
            type_mapping: Dictionary of column names to types
            
        Returns:
            DataFrame with casted types
        """
        df = df.copy()
        
        for column, dtype in type_mapping.items():
            if column not in df.columns:
                logger.warning(f"Column {column} not found in DataFrame")
                continue
            
            try:
                if dtype == int:
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                elif dtype == float:
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                elif dtype == str:
                    df[column] = df[column].astype(str).replace('nan', None)
                elif dtype == 'date':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
                else:
                    df[column] = df[column].astype(dtype)
                    
                logger.info(f"Cast column {column} to {dtype}")
                
            except Exception as e:
                logger.error(f"Error casting {column} to {dtype}: {e}")
                self.validation_errors.append({
                    'column': column,
                    'error': f"Type casting failed: {e}"
                })
        
        return df
    
    def transform_students(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all transformations for student data.
        
        Args:
            df: Raw student DataFrame
            
        Returns:
            Transformed and validated DataFrame
        """
        logger.info(f"Starting transformation of {len(df)} student records")
        self.validation_errors = []
        self.transformation_log = []
        
        df = df.copy()
        
        # Step 1: Clean string fields
        logger.info("Step 1: Cleaning string fields")
        if 'first_name' in df.columns:
            df['first_name'] = df['first_name'].apply(self.normalize_name)
        if 'last_name' in df.columns:
            df['last_name'] = df['last_name'].apply(self.normalize_name)
        if 'email' in df.columns:
            df['email'] = df['email'].apply(self.normalize_email)
        
        # Step 2: Validate emails
        logger.info("Step 2: Validating emails")
        if 'email' in df.columns:
            invalid_emails = df[~df['email'].apply(self.validate_email)]
            if len(invalid_emails) > 0:
                for idx, row in invalid_emails.iterrows():
                    self.validation_errors.append({
                        'row': idx,
                        'column': 'email',
                        'value': row.get('email'),
                        'error': 'Invalid email format'
                    })
                logger.warning(f"Found {len(invalid_emails)} invalid emails")
        
        # Step 3: Validate and clean GPA
        logger.info("Step 3: Validating GPA values")
        if 'gpa' in df.columns:
            invalid_gpa = df[~df['gpa'].apply(self.validate_gpa)]
            for idx, row in invalid_gpa.iterrows():
                self.validation_errors.append({
                    'row': idx,
                    'column': 'gpa',
                    'value': row.get('gpa'),
                    'error': 'Invalid GPA (must be 0.0-4.0)'
                })
            # Clip GPA to valid range
            df['gpa'] = pd.to_numeric(df['gpa'], errors='coerce')
            df['gpa'] = df['gpa'].clip(0.0, 4.0)
        
        # Step 4: Validate department codes
        logger.info("Step 4: Validating department codes")
        if 'department_code' in df.columns:
            invalid_depts = df[~df['department_code'].isin(self.VALID_DEPARTMENTS) & 
                              df['department_code'].notna()]
            for idx, row in invalid_depts.iterrows():
                self.validation_errors.append({
                    'row': idx,
                    'column': 'department_code',
                    'value': row.get('department_code'),
                    'error': f'Invalid department code. Valid: {self.VALID_DEPARTMENTS}'
                })
        
        # Step 5: Validate gender values
        logger.info("Step 5: Validating gender values")
        if 'gender' in df.columns:
            # Normalize gender values
            df['gender'] = df['gender'].apply(
                lambda x: x.strip().title() if pd.notna(x) else None
            )
            invalid_gender = df[~df['gender'].isin(self.VALID_GENDERS) & 
                               df['gender'].notna()]
            for idx, row in invalid_gender.iterrows():
                self.validation_errors.append({
                    'row': idx,
                    'column': 'gender',
                    'value': row.get('gender'),
                    'error': f'Invalid gender. Valid: {self.VALID_GENDERS}'
                })
        
        # Step 6: Type casting
        logger.info("Step 6: Casting data types")
        type_mapping = {
            'graduation_year': int,
            'gpa': float,
            'date_of_birth': 'date'
        }
        df = self.cast_types(df, type_mapping)
        
        # Step 7: Deduplication
        logger.info("Step 7: Removing duplicates")
        if 'email' in df.columns:
            df = self.deduplicate(df, subset=['email'], keep='first')
        if 'student_code' in df.columns:
            df = self.deduplicate(df, subset=['student_code'], keep='first')
        
        # Step 8: Remove rows with critical validation errors
        logger.info("Step 8: Filtering invalid records")
        valid_df = df[df['email'].apply(self.validate_email)]
        removed = len(df) - len(valid_df)
        if removed > 0:
            logger.warning(f"Removed {removed} records with invalid emails")
            self.transformation_log.append(f"Removed {removed} invalid records")
        
        logger.info(f"Transformation complete. {len(valid_df)} valid records")
        return valid_df
    
    def transform_courses(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Transforming {len(df)} course records")
        df = df.copy()

        df['course_code'] = df['course_code'].str.upper().str.strip()
        df['course_name'] = df['course_name'].str.strip()

        df['credits'] = pd.to_numeric(df['credits'], errors='coerce').fillna(3)
        df['credits'] = df['credits'].clip(1, 12).astype(int)

        df['department_code'] = df['department_code'].str.upper().str.strip()

        return df

    def transform_enrollments(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Transforming {len(df)} enrollment records")
        df = df.copy()

        df['student_code'] = df['student_code'].str.upper().str.strip()
        df['course_code'] = df['course_code'].str.upper().str.strip()

        return df

    
    def get_validation_report(self) -> Dict:
        """
        Get a summary of validation errors.
        
        Returns:
            Dictionary with validation error summary
        """
        return {
            'total_errors': len(self.validation_errors),
            'errors': self.validation_errors,
            'transformation_log': self.transformation_log
        }


def transform_students_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convenience function to transform student data.
    
    Args:
        df: Raw student DataFrame
        
    Returns:
        Transformed DataFrame
    """
    transformer = DataTransformer()
    return transformer.transform_students(df)


if __name__ == "__main__":
    # Test transformation
    import sys
    sys.path.append('..')
    from extract import extract_students_csv
    
    try:
        # Extract
        df = extract_students_csv()
        print(f"Extracted {len(df)} records")
        
        # Transform
        transformer = DataTransformer()
        transformed_df = transformer.transform_students(df)
        
        print(f"\nTransformed {len(transformed_df)} records")
        print("\nValidation Report:")
        report = transformer.get_validation_report()
        print(f"Total errors: {report['total_errors']}")
        for error in report['errors'][:5]:
            print(f"  - Row {error['row']}: {error['error']}")
        
        print("\nSample transformed data:")
        print(transformed_df.head())
        
    except FileNotFoundError:
        print("Sample data file not found")
