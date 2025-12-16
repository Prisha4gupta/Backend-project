"""
FastAPI Application
-------------------
REST API for student registration and management.

Endpoints:
- POST /register-student: Register a new student
- GET /students: List all students
- GET /students/{student_code}: Get student details
- GET /health: Health check

Author: Data Engineering Team
"""

import os
import logging
from typing import Optional, List
from datetime import date
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field, field_validator
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection string
DATABASE_URL = os.getenv('DATABASE_URL')


# ============================================
# Pydantic Models
# ============================================

class StudentCreate(BaseModel):
    """Request model for creating a student."""
    student_code: str = Field(..., min_length=1, max_length=20, description="Unique student identifier")
    first_name: str = Field(..., min_length=1, max_length=50, description="Student's first name")
    last_name: str = Field(..., min_length=1, max_length=50, description="Student's last name")
    email: EmailStr = Field(..., description="Student's email address")
    date_of_birth: Optional[date] = Field(None, description="Date of birth (YYYY-MM-DD)")
    gender: Optional[str] = Field(None, description="Gender (Male/Female/Other/Prefer not to say)")
    phone: Optional[str] = Field(None, max_length=20, description="Phone number")
    department_code: Optional[str] = Field(None, max_length=10, description="Department code (e.g., CS, MATH)")
    graduation_year: Optional[int] = Field(None, ge=2000, le=2100, description="Expected graduation year")
    gpa: Optional[float] = Field(None, ge=0.0, le=4.0, description="Current GPA")
    
    @field_validator('gender')
    @classmethod
    def validate_gender(cls, v):
        if v is not None:
            valid_genders = {'Male', 'Female', 'Other', 'Prefer not to say'}
            if v not in valid_genders:
                raise ValueError(f'Gender must be one of: {valid_genders}')
        return v
    
    @field_validator('department_code')
    @classmethod
    def validate_department(cls, v):
        if v is not None:
            valid_depts = {'CS', 'MATH', 'PHY', 'ENG', 'BIO', 'CHEM', 'ECON', 'PSY'}
            if v.upper() not in valid_depts:
                raise ValueError(f'Department code must be one of: {valid_depts}')
            return v.upper()
        return v
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "student_code": "STU100",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "john.doe@university.edu",
                    "date_of_birth": "2002-05-15",
                    "gender": "Male",
                    "phone": "+1-555-0100",
                    "department_code": "CS",
                    "graduation_year": 2027,
                    "gpa": 3.5
                }
            ]
        }
    }


class StudentResponse(BaseModel):
    """Response model for student data."""
    student_id: int
    student_code: str
    first_name: str
    last_name: str
    email: str
    date_of_birth: Optional[date] = None
    gender: Optional[str] = None
    phone: Optional[str] = None
    department_code: Optional[str] = None
    department_name: Optional[str] = None
    enrollment_date: Optional[date] = None
    graduation_year: Optional[int] = None
    gpa: Optional[float] = None
    status: str


class RegistrationResponse(BaseModel):
    """Response model for registration endpoint."""
    success: bool
    message: str
    student_id: Optional[int] = None
    student_code: Optional[str] = None


class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str
    database: str
    version: str


class ErrorResponse(BaseModel):
    """Response model for errors."""
    success: bool = False
    message: str
    error_code: Optional[str] = None


# ============================================
# Database Connection
# ============================================

def get_db_connection():
    """
    Create a database connection.
    
    Yields:
        Database connection object
        
    Raises:
        HTTPException: If connection fails
    """
    if not DATABASE_URL:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database connection not configured"
        )
    
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        yield conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection failed"
        )
    finally:
        if 'conn' in locals():
            conn.close()


# ============================================
# Application Lifecycle
# ============================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger.info("Starting Student Registration API")
    
    # Test database connection on startup
    if DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.close()
            logger.info("Database connection verified")
        except Exception as e:
            logger.warning(f"Database connection check failed: {e}")
    else:
        logger.warning("DATABASE_URL not set")
    
    yield
    
    logger.info("Shutting down Student Registration API")


# ============================================
# FastAPI Application
# ============================================

app = FastAPI(
    title="Student Registration API",
    description="REST API for student registration and management",
    version="1.0.0",
    lifespan=lifespan,
    responses={
        500: {"model": ErrorResponse, "description": "Internal Server Error"}
    }
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# API Endpoints
# ============================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint.
    
    Returns the API status and database connectivity.
    """
    db_status = "disconnected"
    
    if DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            db_status = "connected"
        except Exception as e:
            logger.error(f"Health check - DB error: {e}")
            db_status = f"error: {str(e)[:50]}"
    
    return HealthResponse(
        status="healthy",
        database=db_status,
        version="1.0.0"
    )


@app.post(
    "/register-student",
    response_model=RegistrationResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Students"],
    responses={
        201: {"description": "Student registered successfully"},
        400: {"model": ErrorResponse, "description": "Invalid input data"},
        409: {"model": ErrorResponse, "description": "Student already exists"},
        503: {"model": ErrorResponse, "description": "Database unavailable"}
    }
)
async def register_student(
    student: StudentCreate,
    conn=Depends(get_db_connection)
):
    """
    Register a new student.
    
    Creates a new student record in the database with the provided information.
    Email and student_code must be unique.
    """
    logger.info(f"Registering student: {student.student_code} - {student.email}")
    
    try:
        cursor = conn.cursor()
        
        # Check for existing email
        cursor.execute(
            "SELECT student_id FROM students WHERE email = %s",
            (student.email,)
        )
        if cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A student with this email already exists"
            )
        
        # Check for existing student code
        cursor.execute(
            "SELECT student_id FROM students WHERE student_code = %s",
            (student.student_code,)
        )
        if cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A student with this student code already exists"
            )
        
        # Get department ID if provided
        department_id = None
        if student.department_code:
            cursor.execute(
                "SELECT department_id FROM departments WHERE department_code = %s",
                (student.department_code,)
            )
            dept_result = cursor.fetchone()
            if not dept_result:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid department code: {student.department_code}"
                )
            department_id = dept_result['department_id']
        
        # Insert student
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
                student.student_code,
                student.first_name.strip().title(),
                student.last_name.strip().title(),
                student.email.lower(),
                student.date_of_birth,
                student.gender,
                student.phone,
                department_id,
                student.graduation_year,
                student.gpa
            )
        )
        
        result = cursor.fetchone()
        student_id = result['student_id']
        
        conn.commit()
        
        logger.info(f"Student registered successfully: ID {student_id}")
        
        return RegistrationResponse(
            success=True,
            message="Student registered successfully",
            student_id=student_id,
            student_code=student.student_code
        )
        
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Registration error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {str(e)}"
        )
    finally:
        cursor.close()


@app.get(
    "/students",
    response_model=List[StudentResponse],
    tags=["Students"]
)
async def list_students(
    status_filter: Optional[str] = None,
    department: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    conn=Depends(get_db_connection)
):
    """
    List all students with optional filtering.
    
    - **status_filter**: Filter by status (Active, Inactive, Graduated, etc.)
    - **department**: Filter by department code
    - **limit**: Maximum number of results (default 100)
    - **offset**: Number of results to skip
    """
    try:
        cursor = conn.cursor()
        
        query = """
            SELECT 
                s.student_id,
                s.student_code,
                s.first_name,
                s.last_name,
                s.email,
                s.date_of_birth,
                s.gender,
                s.phone,
                d.department_code,
                d.department_name,
                s.enrollment_date,
                s.graduation_year,
                s.gpa,
                s.status
            FROM students s
            LEFT JOIN departments d ON s.department_id = d.department_id
            WHERE 1=1
        """
        params = []
        
        if status_filter:
            query += " AND s.status = %s"
            params.append(status_filter)
        
        if department:
            query += " AND d.department_code = %s"
            params.append(department.upper())
        
        query += " ORDER BY s.student_id LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        students = cursor.fetchall()
        
        return [StudentResponse(**student) for student in students]
        
    except Exception as e:
        logger.error(f"Error listing students: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve students"
        )
    finally:
        cursor.close()


@app.get(
    "/students/{student_code}",
    response_model=StudentResponse,
    tags=["Students"],
    responses={
        404: {"model": ErrorResponse, "description": "Student not found"}
    }
)
async def get_student(
    student_code: str,
    conn=Depends(get_db_connection)
):
    """
    Get a specific student by their student code.
    """
    try:
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT 
                s.student_id,
                s.student_code,
                s.first_name,
                s.last_name,
                s.email,
                s.date_of_birth,
                s.gender,
                s.phone,
                d.department_code,
                d.department_name,
                s.enrollment_date,
                s.graduation_year,
                s.gpa,
                s.status
            FROM students s
            LEFT JOIN departments d ON s.department_id = d.department_id
            WHERE s.student_code = %s
            """,
            (student_code,)
        )
        
        student = cursor.fetchone()
        
        if not student:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Student with code {student_code} not found"
            )
        
        return StudentResponse(**student)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting student: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve student"
        )
    finally:
        cursor.close()


# ============================================
# Run with Uvicorn
# ============================================

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="info"
    )
