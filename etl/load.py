"""
Load Module
-----------
Handles data loading into PostgreSQL database:
- Connection management
- Batch inserts
- Upsert (idempotent) operations
- Error handling and rollback

Author: Data Engineering Team
"""

import pandas as pd
import logging
import os
from typing import List, Dict, Optional, Tuple
from contextlib import contextmanager
import psycopg2
from psycopg2 import sql, extras
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseLoader:
    """
    Loads transformed data into PostgreSQL database.
    Supports idempotent operations and batch processing.
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize the DatabaseLoader.
        
        Args:
            connection_string: PostgreSQL connection string
                              If not provided, uses DATABASE_URL env variable
        """
        self.connection_string = connection_string or os.getenv('DATABASE_URL')
        
        if not self.connection_string:
            raise ValueError(
                "Database connection string not provided. "
                "Set DATABASE_URL environment variable or pass connection_string."
            )
        
        self.engine = create_engine(self.connection_string)
        self.Session = sessionmaker(bind=self.engine)
        
        logger.info("DatabaseLoader initialized")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        
        Yields:
            psycopg2 connection object
        """
        conn = psycopg2.connect(self.connection_string)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    @contextmanager
    def get_cursor(self, conn):
        """
        Context manager for database cursors.
        
        Args:
            conn: Database connection
            
        Yields:
            psycopg2 cursor object
        """
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
    
    def execute_sql_file(self, filepath: str) -> bool:
        """
        Execute a SQL file against the database.
        
        Args:
            filepath: Path to the SQL file
            
        Returns:
            True if successful
        """
        logger.info(f"Executing SQL file: {filepath}")
        
        with open(filepath, 'r') as f:
            sql_content = f.read()
        
        with self.get_connection() as conn:
            with self.get_cursor(conn) as cursor:
                cursor.execute(sql_content)
                logger.info(f"SQL file executed successfully: {filepath}")
        
        return True
    
    def get_department_id(self, cursor, department_code: str) -> Optional[int]:
        """
        Get department ID from department code.
        
        Args:
            cursor: Database cursor
            department_code: Department code to look up
            
        Returns:
            Department ID or None if not found
        """
        if pd.isna(department_code):
            return None
        
        cursor.execute(
            "SELECT department_id FROM departments WHERE department_code = %s",
            (department_code,)
        )
        result = cursor.fetchone()
        return result[0] if result else None
    
    def upsert_student(
        self, 
        cursor,
        student_data: Dict
    ) -> Tuple[bool, Optional[int], str]:
        """
        Insert or update a student record (idempotent).
        
        Args:
            cursor: Database cursor
            student_data: Dictionary with student data
            
        Returns:
            Tuple of (success, student_id, message)
        """
        try:
            # Get department ID
            department_id = self.get_department_id(
                cursor, 
                student_data.get('department_code')
            )
            
            # Check if student exists (by email or student_code)
            cursor.execute(
                """
                SELECT student_id FROM students 
                WHERE email = %s OR student_code = %s
                """,
                (student_data.get('email'), student_data.get('student_code'))
            )
            existing = cursor.fetchone()
            
            if existing:
                # Update existing student
                cursor.execute(
                    """
                    UPDATE students SET
                        first_name = COALESCE(%s, first_name),
                        last_name = COALESCE(%s, last_name),
                        date_of_birth = COALESCE(%s, date_of_birth),
                        gender = COALESCE(%s, gender),
                        phone = COALESCE(%s, phone),
                        department_id = COALESCE(%s, department_id),
                        graduation_year = COALESCE(%s, graduation_year),
                        gpa = COALESCE(%s, gpa),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE student_id = %s
                    RETURNING student_id
                    """,
                    (
                        student_data.get('first_name'),
                        student_data.get('last_name'),
                        student_data.get('date_of_birth'),
                        student_data.get('gender'),
                        student_data.get('phone'),
                        department_id,
                        student_data.get('graduation_year'),
                        student_data.get('gpa'),
                        existing[0]
                    )
                )
                student_id = cursor.fetchone()[0]
                return True, student_id, "updated"
            else:
                # Insert new student
                cursor.execute(
                    """
                    INSERT INTO students (
                        student_code, first_name, last_name, email,
                        date_of_birth, gender, phone, department_id,
                        graduation_year, gpa, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Active')
                    RETURNING student_id
                    """,
                    (
                        student_data.get('student_code'),
                        student_data.get('first_name'),
                        student_data.get('last_name'),
                        student_data.get('email'),
                        student_data.get('date_of_birth'),
                        student_data.get('gender'),
                        student_data.get('phone'),
                        department_id,
                        student_data.get('graduation_year'),
                        student_data.get('gpa')
                    )
                )
                student_id = cursor.fetchone()[0]
                return True, student_id, "inserted"
                
        except Exception as e:
            logger.error(f"Error upserting student: {e}")
            return False, None, str(e)
        
    def upsert_course(self, cursor, data: dict):
        try:
            # Get department_id from department_code
            cursor.execute(
                "SELECT department_id FROM departments WHERE department_code = %s",
                (data["department_code"],)
            )
            dept = cursor.fetchone()
            if not dept:
                return False, "Department not found"

            cursor.execute("""
                INSERT INTO courses (course_code, course_name, credits, department_id, max_enrollment)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (course_code)
                DO UPDATE SET
                    course_name = EXCLUDED.course_name,
                    credits = EXCLUDED.credits,
                    max_enrollment = EXCLUDED.max_enrollment,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                data["course_code"],
                data["course_name"],
                data["credits"],
                dept[0],
                data.get("max_enrollment", 30)
            ))

            return True, "upserted"

        except Exception as e:
            return False, str(e)
        

    def upsert_enrollment(self, cursor, data: dict):
        try:
            cursor.execute(
                "SELECT student_id FROM students WHERE student_code = %s",
                (data["student_code"],)
            )
            student = cursor.fetchone()
            if not student:
                return False, "Student not found"

            cursor.execute(
                "SELECT course_id FROM courses WHERE course_code = %s",
                (data["course_code"],)
            )
            course = cursor.fetchone()
            if not course:
                return False, "Course not found"

            cursor.execute("""
                INSERT INTO enrollments (student_id, course_id, grade, status)
                VALUES (%s, %s, %s, 'Enrolled')
                ON CONFLICT (student_id, course_id)
                DO UPDATE SET
                    grade = EXCLUDED.grade,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                student[0],
                course[0],
                data.get("grade")
            ))

            return True, "upserted"

        except Exception as e:
            return False, str(e)

    def load_courses(self, df):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for _, row in df.iterrows():
                self.upsert_course(cursor, row.to_dict())
            conn.commit()

    def load_enrollments(self, df):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for _, row in df.iterrows():
                self.upsert_enrollment(cursor, row.to_dict())
            conn.commit()


    def load_students_batch(
        self, 
        df: pd.DataFrame,
        batch_size: int = 100
    ) -> Dict:
        """
        Load students data in batches.
        
        Args:
            df: DataFrame with student data
            batch_size: Number of records per batch
            
        Returns:
            Dictionary with load statistics
        """
        logger.info(f"Loading {len(df)} students in batches of {batch_size}")
        
        stats = {
            'total': len(df),
            'inserted': 0,
            'updated': 0,
            'failed': 0,
            'errors': []
        }
        
        with self.get_connection() as conn:
            with self.get_cursor(conn) as cursor:
                for idx, row in df.iterrows():
                    student_data = row.to_dict()
                    
                    success, student_id, message = self.upsert_student(
                        cursor, 
                        student_data
                    )
                    
                    if success:
                        if message == 'inserted':
                            stats['inserted'] += 1
                        else:
                            stats['updated'] += 1
                    else:
                        stats['failed'] += 1
                        stats['errors'].append({
                            'row': idx,
                            'email': student_data.get('email'),
                            'error': message
                        })
                    
                    # Commit in batches
                    if (idx + 1) % batch_size == 0:
                        conn.commit()
                        logger.info(f"Committed batch up to row {idx + 1}")
        
        logger.info(
            f"Load complete. Inserted: {stats['inserted']}, "
            f"Updated: {stats['updated']}, Failed: {stats['failed']}"
        )
        
        return stats
    
    def bulk_insert_students(self, df: pd.DataFrame) -> Dict:
        """
        Bulk insert students using copy_from for maximum performance.
        Note: This does not handle duplicates - use for initial load only.
        
        Args:
            df: DataFrame with student data
            
        Returns:
            Dictionary with load statistics
        """
        logger.info(f"Bulk inserting {len(df)} students")
        
        # Prepare data
        columns = [
            'student_code', 'first_name', 'last_name', 'email',
            'date_of_birth', 'gender', 'phone', 'graduation_year', 'gpa'
        ]
        
        from io import StringIO
        
        # Create CSV-like string buffer
        buffer = StringIO()
        df[columns].to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        with self.get_connection() as conn:
            with self.get_cursor(conn) as cursor:
                try:
                    cursor.copy_from(
                        buffer,
                        'students',
                        columns=columns,
                        sep='\t',
                        null='\\N'
                    )
                    return {'success': True, 'rows': len(df)}
                except Exception as e:
                    logger.error(f"Bulk insert failed: {e}")
                    return {'success': False, 'error': str(e)}
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection successful
        """
        try:
            with self.get_connection() as conn:
                with self.get_cursor(conn) as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    logger.info("Database connection test successful")
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False


def load_students_to_db(df: pd.DataFrame, connection_string: str = None) -> Dict:
    """
    Convenience function to load students to database.
    
    Args:
        df: Transformed student DataFrame
        connection_string: Optional database connection string
        
    Returns:
        Load statistics dictionary
    """
    loader = DatabaseLoader(connection_string)
    return loader.load_students_batch(df)


if __name__ == "__main__":
    # Test connection
    try:
        loader = DatabaseLoader()
        if loader.test_connection():
            print("Database connection successful!")
        else:
            print("Database connection failed!")
    except ValueError as e:
        print(f"Configuration error: {e}")
