-- ============================================
-- DATABASE VIEWS
-- Pre-built views for common data access patterns
-- ============================================

-- ============================================
-- VIEW: Student Dashboard
-- Comprehensive student information with department
-- ============================================
CREATE OR REPLACE VIEW vw_student_dashboard AS
SELECT 
    s.student_id,
    s.student_code,
    CONCAT(s.first_name, ' ', s.last_name) AS full_name,
    s.email,
    s.phone,
    s.date_of_birth,
    EXTRACT(YEAR FROM AGE(s.date_of_birth))::INTEGER AS age,
    s.gender,
    d.department_code,
    d.department_name,
    s.enrollment_date,
    s.graduation_year,
    s.gpa,
    s.status,
    COUNT(e.enrollment_id) FILTER (WHERE e.status = 'Enrolled') AS current_courses,
    COUNT(e.enrollment_id) FILTER (WHERE e.status = 'Completed') AS completed_courses,
    COALESCE(SUM(c.credits) FILTER (WHERE e.status = 'Completed'), 0) AS total_credits_earned
FROM students s
LEFT JOIN departments d ON s.department_id = d.department_id
LEFT JOIN enrollments e ON s.student_id = e.student_id
LEFT JOIN courses c ON e.course_id = c.course_id
GROUP BY s.student_id, s.student_code, s.first_name, s.last_name, s.email, 
         s.phone, s.date_of_birth, s.gender, d.department_code, d.department_name,
         s.enrollment_date, s.graduation_year, s.gpa, s.status;

-- ============================================
-- VIEW: Course Catalog
-- Complete course information with enrollment stats
-- ============================================
CREATE OR REPLACE VIEW vw_course_catalog AS
SELECT 
    c.course_id,
    c.course_code,
    c.course_name,
    c.description,
    c.credits,
    d.department_code,
    d.department_name,
    c.instructor,
    c.semester,
    c.academic_year,
    c.max_enrollment,
    COUNT(e.enrollment_id) FILTER (WHERE e.status = 'Enrolled') AS current_enrollment,
    c.max_enrollment - COUNT(e.enrollment_id) FILTER (WHERE e.status = 'Enrolled') AS available_seats,
    c.is_active,
    ROUND(AVG(e.grade_points) FILTER (WHERE e.status = 'Completed'), 2) AS avg_grade
FROM courses c
LEFT JOIN departments d ON c.department_id = d.department_id
LEFT JOIN enrollments e ON c.course_id = e.course_id
GROUP BY c.course_id, c.course_code, c.course_name, c.description, c.credits,
         d.department_code, d.department_name, c.instructor, c.semester,
         c.academic_year, c.max_enrollment, c.is_active;

-- ============================================
-- VIEW: Enrollment Details
-- Full enrollment information with student and course
-- ============================================
CREATE OR REPLACE VIEW vw_enrollment_details AS
SELECT 
    e.enrollment_id,
    s.student_code,
    CONCAT(s.first_name, ' ', s.last_name) AS student_name,
    s.email AS student_email,
    c.course_code,
    c.course_name,
    c.credits,
    d.department_name,
    e.enrollment_date,
    e.grade,
    e.grade_points,
    e.status AS enrollment_status,
    c.semester,
    c.academic_year
FROM enrollments e
JOIN students s ON e.student_id = s.student_id
JOIN courses c ON e.course_id = c.course_id
LEFT JOIN departments d ON c.department_id = d.department_id;

-- ============================================
-- VIEW: Department Summary
-- Aggregate statistics per department
-- ============================================
CREATE OR REPLACE VIEW vw_department_summary AS
SELECT 
    d.department_id,
    d.department_code,
    d.department_name,
    d.head_of_department,
    COUNT(DISTINCT s.student_id) AS total_students,
    COUNT(DISTINCT s.student_id) FILTER (WHERE s.status = 'Active') AS active_students,
    COUNT(DISTINCT c.course_id) AS total_courses,
    COUNT(DISTINCT c.course_id) FILTER (WHERE c.is_active = TRUE) AS active_courses,
    ROUND(AVG(s.gpa), 2) AS average_student_gpa,
    SUM(c.credits) FILTER (WHERE c.is_active = TRUE) AS total_active_credits
FROM departments d
LEFT JOIN students s ON d.department_id = s.department_id
LEFT JOIN courses c ON d.department_id = c.department_id
GROUP BY d.department_id, d.department_code, d.department_name, d.head_of_department;

-- ============================================
-- VIEW: Academic Performance Report
-- Student academic performance metrics
-- ============================================
CREATE OR REPLACE VIEW vw_academic_performance AS
SELECT 
    s.student_id,
    s.student_code,
    CONCAT(s.first_name, ' ', s.last_name) AS full_name,
    d.department_name,
    s.gpa AS cumulative_gpa,
    COUNT(e.enrollment_id) AS total_courses_taken,
    COUNT(e.enrollment_id) FILTER (WHERE e.status = 'Completed') AS courses_completed,
    COUNT(e.enrollment_id) FILTER (WHERE e.status = 'Enrolled') AS courses_in_progress,
    COALESCE(SUM(c.credits) FILTER (WHERE e.status = 'Completed'), 0) AS credits_earned,
    COALESCE(SUM(c.credits) FILTER (WHERE e.status = 'Enrolled'), 0) AS credits_in_progress,
    ROUND(AVG(e.grade_points) FILTER (WHERE e.status = 'Completed'), 2) AS semester_gpa,
    CASE 
        WHEN s.gpa >= 3.9 THEN 'Summa Cum Laude'
        WHEN s.gpa >= 3.7 THEN 'Magna Cum Laude'
        WHEN s.gpa >= 3.5 THEN 'Cum Laude'
        WHEN s.gpa >= 2.0 THEN 'Good Standing'
        ELSE 'Academic Probation'
    END AS academic_standing
FROM students s
LEFT JOIN departments d ON s.department_id = d.department_id
LEFT JOIN enrollments e ON s.student_id = e.student_id
LEFT JOIN courses c ON e.course_id = c.course_id
WHERE s.status = 'Active'
GROUP BY s.student_id, s.student_code, s.first_name, s.last_name, d.department_name, s.gpa;
