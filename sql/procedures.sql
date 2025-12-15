-- ============================================
-- STORED PROCEDURES AND FUNCTIONS
-- Business logic encapsulated in database
-- ============================================

-- ============================================
-- FUNCTION: Register New Student
-- Inserts a new student with validation
-- ============================================
CREATE OR REPLACE FUNCTION fn_register_student(
    p_student_code VARCHAR(20),
    p_first_name VARCHAR(50),
    p_last_name VARCHAR(50),
    p_email VARCHAR(100),
    p_date_of_birth DATE DEFAULT NULL,
    p_gender VARCHAR(10) DEFAULT NULL,
    p_phone VARCHAR(20) DEFAULT NULL,
    p_department_code VARCHAR(10) DEFAULT NULL,
    p_graduation_year INTEGER DEFAULT NULL
)
RETURNS TABLE(
    success BOOLEAN,
    message TEXT,
    student_id INTEGER
) AS $$
DECLARE
    v_department_id INTEGER;
    v_new_student_id INTEGER;
BEGIN
    -- Validate email format (basic check)
    IF p_email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN
        RETURN QUERY SELECT FALSE, 'Invalid email format', NULL::INTEGER;
        RETURN;
    END IF;
    
    -- Check for existing email
    IF EXISTS (SELECT 1 FROM students WHERE email = p_email) THEN
        RETURN QUERY SELECT FALSE, 'Email already registered', NULL::INTEGER;
        RETURN;
    END IF;
    
    -- Check for existing student code
    IF EXISTS (SELECT 1 FROM students WHERE student_code = p_student_code) THEN
        RETURN QUERY SELECT FALSE, 'Student code already exists', NULL::INTEGER;
        RETURN;
    END IF;
    
    -- Get department ID if provided
    IF p_department_code IS NOT NULL THEN
        SELECT department_id INTO v_department_id 
        FROM departments 
        WHERE department_code = p_department_code;
        
        IF v_department_id IS NULL THEN
            RETURN QUERY SELECT FALSE, 'Invalid department code', NULL::INTEGER;
            RETURN;
        END IF;
    END IF;
    
    -- Insert student
    INSERT INTO students (
        student_code, first_name, last_name, email,
        date_of_birth, gender, phone, department_id, graduation_year
    )
    VALUES (
        p_student_code, p_first_name, p_last_name, p_email,
        p_date_of_birth, p_gender, p_phone, v_department_id, p_graduation_year
    )
    RETURNING students.student_id INTO v_new_student_id;
    
    RETURN QUERY SELECT TRUE, 'Student registered successfully', v_new_student_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- FUNCTION: Enroll Student in Course
-- Handles enrollment with capacity checks
-- ============================================
CREATE OR REPLACE FUNCTION fn_enroll_student(
    p_student_code VARCHAR(20),
    p_course_code VARCHAR(20)
)
RETURNS TABLE(
    success BOOLEAN,
    message TEXT,
    enrollment_id INTEGER
) AS $$
DECLARE
    v_student_id INTEGER;
    v_course_id INTEGER;
    v_current_enrollment INTEGER;
    v_max_enrollment INTEGER;
    v_new_enrollment_id INTEGER;
    v_course_active BOOLEAN;
BEGIN
    -- Get student ID
    SELECT student_id INTO v_student_id 
    FROM students 
    WHERE student_code = p_student_code AND status = 'Active';
    
    IF v_student_id IS NULL THEN
        RETURN QUERY SELECT FALSE, 'Student not found or inactive', NULL::INTEGER;
        RETURN;
    END IF;
    
    -- Get course details
    SELECT course_id, max_enrollment, is_active 
    INTO v_course_id, v_max_enrollment, v_course_active
    FROM courses 
    WHERE course_code = p_course_code;
    
    IF v_course_id IS NULL THEN
        RETURN QUERY SELECT FALSE, 'Course not found', NULL::INTEGER;
        RETURN;
    END IF;
    
    IF NOT v_course_active THEN
        RETURN QUERY SELECT FALSE, 'Course is not active', NULL::INTEGER;
        RETURN;
    END IF;
    
    -- Check if already enrolled
    IF EXISTS (
        SELECT 1 FROM enrollments 
        WHERE student_id = v_student_id 
        AND course_id = v_course_id 
        AND status IN ('Enrolled', 'Completed')
    ) THEN
        RETURN QUERY SELECT FALSE, 'Student already enrolled or completed this course', NULL::INTEGER;
        RETURN;
    END IF;
    
    -- Check capacity
    SELECT COUNT(*) INTO v_current_enrollment
    FROM enrollments 
    WHERE course_id = v_course_id AND status = 'Enrolled';
    
    IF v_current_enrollment >= v_max_enrollment THEN
        RETURN QUERY SELECT FALSE, 'Course is at maximum capacity', NULL::INTEGER;
        RETURN;
    END IF;
    
    -- Create enrollment
    INSERT INTO enrollments (student_id, course_id, status)
    VALUES (v_student_id, v_course_id, 'Enrolled')
    RETURNING enrollments.enrollment_id INTO v_new_enrollment_id;
    
    RETURN QUERY SELECT TRUE, 'Enrollment successful', v_new_enrollment_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- FUNCTION: Update Student Grade
-- Updates grade with GPA recalculation
-- ============================================
CREATE OR REPLACE FUNCTION fn_update_grade(
    p_student_code VARCHAR(20),
    p_course_code VARCHAR(20),
    p_grade VARCHAR(2)
)
RETURNS TABLE(
    success BOOLEAN,
    message TEXT
) AS $$
DECLARE
    v_student_id INTEGER;
    v_course_id INTEGER;
    v_grade_points DECIMAL(3,2);
    v_new_gpa DECIMAL(3,2);
BEGIN
    -- Get student ID
    SELECT student_id INTO v_student_id 
    FROM students 
    WHERE student_code = p_student_code;
    
    IF v_student_id IS NULL THEN
        RETURN QUERY SELECT FALSE, 'Student not found';
        RETURN;
    END IF;
    
    -- Get course ID
    SELECT course_id INTO v_course_id 
    FROM courses 
    WHERE course_code = p_course_code;
    
    IF v_course_id IS NULL THEN
        RETURN QUERY SELECT FALSE, 'Course not found';
        RETURN;
    END IF;
    
    -- Calculate grade points
    v_grade_points := CASE p_grade
        WHEN 'A+' THEN 4.00
        WHEN 'A' THEN 4.00
        WHEN 'A-' THEN 3.70
        WHEN 'B+' THEN 3.30
        WHEN 'B' THEN 3.00
        WHEN 'B-' THEN 2.70
        WHEN 'C+' THEN 2.30
        WHEN 'C' THEN 2.00
        WHEN 'C-' THEN 1.70
        WHEN 'D+' THEN 1.30
        WHEN 'D' THEN 1.00
        WHEN 'D-' THEN 0.70
        WHEN 'F' THEN 0.00
        ELSE NULL
    END;
    
    -- Update enrollment
    UPDATE enrollments 
    SET grade = p_grade, 
        grade_points = v_grade_points,
        status = 'Completed'
    WHERE student_id = v_student_id 
    AND course_id = v_course_id;
    
    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, 'Enrollment not found';
        RETURN;
    END IF;
    
    -- Recalculate student GPA
    SELECT ROUND(AVG(grade_points), 2) INTO v_new_gpa
    FROM enrollments
    WHERE student_id = v_student_id 
    AND status = 'Completed'
    AND grade_points IS NOT NULL;
    
    UPDATE students SET gpa = v_new_gpa WHERE student_id = v_student_id;
    
    RETURN QUERY SELECT TRUE, CONCAT('Grade updated. New GPA: ', v_new_gpa);
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- FUNCTION: Get Student Transcript
-- Returns complete academic record
-- ============================================
CREATE OR REPLACE FUNCTION fn_get_transcript(p_student_code VARCHAR(20))
RETURNS TABLE(
    course_code VARCHAR(20),
    course_name VARCHAR(150),
    credits INTEGER,
    semester VARCHAR(20),
    academic_year INTEGER,
    grade VARCHAR(2),
    grade_points DECIMAL(3,2),
    status VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.course_code,
        c.course_name,
        c.credits,
        c.semester,
        c.academic_year,
        e.grade,
        e.grade_points,
        e.status
    FROM students s
    JOIN enrollments e ON s.student_id = e.student_id
    JOIN courses c ON e.course_id = c.course_id
    WHERE s.student_code = p_student_code
    ORDER BY c.academic_year DESC, c.semester;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- FUNCTION: Department Statistics
-- Returns comprehensive department metrics
-- ============================================
CREATE OR REPLACE FUNCTION fn_department_stats(p_department_code VARCHAR(10))
RETURNS TABLE(
    metric_name TEXT,
    metric_value TEXT
) AS $$
DECLARE
    v_dept_id INTEGER;
BEGIN
    SELECT department_id INTO v_dept_id 
    FROM departments 
    WHERE department_code = p_department_code;
    
    IF v_dept_id IS NULL THEN
        RETURN QUERY SELECT 'Error'::TEXT, 'Department not found'::TEXT;
        RETURN;
    END IF;
    
    RETURN QUERY
    SELECT 'Total Students'::TEXT, COUNT(*)::TEXT 
    FROM students WHERE department_id = v_dept_id;
    
    RETURN QUERY
    SELECT 'Active Students'::TEXT, COUNT(*)::TEXT 
    FROM students WHERE department_id = v_dept_id AND status = 'Active';
    
    RETURN QUERY
    SELECT 'Average GPA'::TEXT, ROUND(AVG(gpa), 2)::TEXT 
    FROM students WHERE department_id = v_dept_id;
    
    RETURN QUERY
    SELECT 'Total Courses'::TEXT, COUNT(*)::TEXT 
    FROM courses WHERE department_id = v_dept_id;
    
    RETURN QUERY
    SELECT 'Active Courses'::TEXT, COUNT(*)::TEXT 
    FROM courses WHERE department_id = v_dept_id AND is_active = TRUE;
END;
$$ LANGUAGE plpgsql;
