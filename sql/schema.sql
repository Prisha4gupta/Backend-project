-- ============================================
-- DATABASE SCHEMA: Student Management System
-- Normalized to 3NF with proper constraints
-- Compatible with NeonDB (PostgreSQL)
-- ============================================

-- Drop existing tables (in correct order due to FK constraints)
DROP TABLE IF EXISTS enrollments CASCADE;
DROP TABLE IF EXISTS courses CASCADE;
DROP TABLE IF EXISTS students CASCADE;
DROP TABLE IF EXISTS departments CASCADE;

-- ============================================
-- DEPARTMENTS TABLE
-- Stores academic department information
-- ============================================
CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    department_code VARCHAR(10) NOT NULL UNIQUE,
    department_name VARCHAR(100) NOT NULL,
    head_of_department VARCHAR(100),
    established_year INTEGER CHECK (established_year >= 1800 AND established_year <= EXTRACT(YEAR FROM CURRENT_DATE)),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for department lookups
CREATE INDEX idx_departments_code ON departments(department_code);

-- ============================================
-- STUDENTS TABLE
-- Stores student personal and academic info
-- ============================================
CREATE TABLE students (
    student_id SERIAL PRIMARY KEY,
    student_code VARCHAR(20) NOT NULL UNIQUE,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    date_of_birth DATE,
    gender VARCHAR(10) CHECK (gender IN ('Male', 'Female', 'Other', 'Prefer not to say')),
    phone VARCHAR(20),
    address TEXT,
    department_id INTEGER REFERENCES departments(department_id) ON DELETE SET NULL,
    enrollment_date DATE DEFAULT CURRENT_DATE,
    graduation_year INTEGER CHECK (graduation_year >= 2000 AND graduation_year <= 2100),
    gpa DECIMAL(3, 2) CHECK (gpa >= 0.00 AND gpa <= 4.00),
    status VARCHAR(20) DEFAULT 'Active' CHECK (status IN ('Active', 'Inactive', 'Graduated', 'Suspended', 'On Leave')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX idx_students_email ON students(email);
CREATE INDEX idx_students_department ON students(department_id);
CREATE INDEX idx_students_status ON students(status);
CREATE INDEX idx_students_code ON students(student_code);

-- ============================================
-- COURSES TABLE
-- Stores course information
-- ============================================
CREATE TABLE courses (
    course_id SERIAL PRIMARY KEY,
    course_code VARCHAR(20) NOT NULL UNIQUE,
    course_name VARCHAR(150) NOT NULL,
    description TEXT,
    credits INTEGER NOT NULL CHECK (credits >= 1 AND credits <= 12),
    department_id INTEGER REFERENCES departments(department_id) ON DELETE CASCADE,
    instructor VARCHAR(100),
    max_enrollment INTEGER DEFAULT 50 CHECK (max_enrollment > 0),
    semester VARCHAR(20) CHECK (semester IN ('Fall', 'Spring', 'Summer', 'Winter')),
    academic_year INTEGER CHECK (academic_year >= 2000 AND academic_year <= 2100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for course queries
CREATE INDEX idx_courses_department ON courses(department_id);
CREATE INDEX idx_courses_code ON courses(course_code);
CREATE INDEX idx_courses_active ON courses(is_active);

-- ============================================
-- ENROLLMENTS TABLE
-- Junction table for student-course relationship
-- ============================================
CREATE TABLE enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    student_id INTEGER NOT NULL REFERENCES students(student_id) ON DELETE CASCADE,
    course_id INTEGER NOT NULL REFERENCES courses(course_id) ON DELETE CASCADE,
    enrollment_date DATE DEFAULT CURRENT_DATE,
    grade VARCHAR(2) CHECK (grade IN ('A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'D-', 'F', 'W', 'I', 'P', NULL)),
    grade_points DECIMAL(3, 2) CHECK (grade_points >= 0.00 AND grade_points <= 4.00),
    status VARCHAR(20) DEFAULT 'Enrolled' CHECK (status IN ('Enrolled', 'Completed', 'Dropped', 'Withdrawn', 'Failed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Prevent duplicate enrollments
    UNIQUE(student_id, course_id)
);

-- Indexes for enrollment queries
CREATE INDEX idx_enrollments_student ON enrollments(student_id);
CREATE INDEX idx_enrollments_course ON enrollments(course_id);
CREATE INDEX idx_enrollments_status ON enrollments(status);

-- ============================================
-- TRIGGER FUNCTION: Update timestamp
-- ============================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to all tables
CREATE TRIGGER update_departments_updated_at
    BEFORE UPDATE ON departments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_students_updated_at
    BEFORE UPDATE ON students
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_courses_updated_at
    BEFORE UPDATE ON courses
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_enrollments_updated_at
    BEFORE UPDATE ON enrollments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================
COMMENT ON TABLE departments IS 'Academic departments within the institution';
COMMENT ON TABLE students IS 'Student personal and academic information';
COMMENT ON TABLE courses IS 'Course catalog with scheduling information';
COMMENT ON TABLE enrollments IS 'Student course enrollments and grades';
