-- ============================================
-- ANALYTICAL QUERIES
-- Common queries for reporting and analysis
-- ============================================

-- ============================================
-- 1. Student Statistics by Department
-- ============================================
SELECT 
    d.department_name,
    COUNT(s.student_id) AS total_students,
    ROUND(AVG(s.gpa), 2) AS average_gpa,
    COUNT(CASE WHEN s.status = 'Active' THEN 1 END) AS active_students,
    COUNT(CASE WHEN s.status = 'Graduated' THEN 1 END) AS graduated_students
FROM departments d
LEFT JOIN students s ON d.department_id = s.department_id
GROUP BY d.department_id, d.department_name
ORDER BY total_students DESC;

-- ============================================
-- 2. Course Enrollment Summary
-- ============================================
SELECT 
    c.course_code,
    c.course_name,
    d.department_name,
    c.max_enrollment,
    COUNT(e.enrollment_id) AS current_enrollment,
    c.max_enrollment - COUNT(e.enrollment_id) AS available_spots,
    ROUND(COUNT(e.enrollment_id)::DECIMAL / c.max_enrollment * 100, 1) AS fill_rate_percent
FROM courses c
LEFT JOIN departments d ON c.department_id = d.department_id
LEFT JOIN enrollments e ON c.course_id = e.course_id AND e.status = 'Enrolled'
WHERE c.is_active = TRUE
GROUP BY c.course_id, c.course_code, c.course_name, d.department_name, c.max_enrollment
ORDER BY fill_rate_percent DESC;

-- ============================================
-- 3. Grade Distribution Analysis
-- ============================================
SELECT 
    c.course_code,
    c.course_name,
    COUNT(e.enrollment_id) AS total_graded,
    ROUND(AVG(e.grade_points), 2) AS average_grade_points,
    COUNT(CASE WHEN e.grade IN ('A+', 'A', 'A-') THEN 1 END) AS a_grades,
    COUNT(CASE WHEN e.grade IN ('B+', 'B', 'B-') THEN 1 END) AS b_grades,
    COUNT(CASE WHEN e.grade IN ('C+', 'C', 'C-') THEN 1 END) AS c_grades,
    COUNT(CASE WHEN e.grade IN ('D+', 'D', 'D-', 'F') THEN 1 END) AS below_c_grades
FROM courses c
JOIN enrollments e ON c.course_id = e.course_id
WHERE e.grade IS NOT NULL AND e.status = 'Completed'
GROUP BY c.course_id, c.course_code, c.course_name
ORDER BY average_grade_points DESC;

-- ============================================
-- 4. Top Performing Students
-- ============================================
SELECT 
    s.student_code,
    CONCAT(s.first_name, ' ', s.last_name) AS full_name,
    d.department_name,
    s.gpa,
    COUNT(e.enrollment_id) AS courses_completed,
    ROUND(AVG(e.grade_points), 2) AS avg_course_grade
FROM students s
JOIN departments d ON s.department_id = d.department_id
LEFT JOIN enrollments e ON s.student_id = e.student_id AND e.status = 'Completed'
WHERE s.status = 'Active'
GROUP BY s.student_id, s.student_code, s.first_name, s.last_name, d.department_name, s.gpa
HAVING s.gpa >= 3.5
ORDER BY s.gpa DESC, avg_course_grade DESC
LIMIT 20;

-- ============================================
-- 5. Department Course Load
-- ============================================
SELECT 
    d.department_code,
    d.department_name,
    COUNT(DISTINCT c.course_id) AS total_courses,
    SUM(c.credits) AS total_credits_offered,
    COUNT(DISTINCT e.student_id) AS unique_students_enrolled,
    SUM(CASE WHEN c.is_active THEN 1 ELSE 0 END) AS active_courses
FROM departments d
LEFT JOIN courses c ON d.department_id = c.department_id
LEFT JOIN enrollments e ON c.course_id = e.course_id
GROUP BY d.department_id, d.department_code, d.department_name
ORDER BY total_courses DESC;

-- ============================================
-- 6. Students At Risk (Low GPA)
-- ============================================
SELECT 
    s.student_code,
    CONCAT(s.first_name, ' ', s.last_name) AS full_name,
    s.email,
    d.department_name,
    s.gpa,
    s.enrollment_date,
    COUNT(e.enrollment_id) AS current_enrollments
FROM students s
LEFT JOIN departments d ON s.department_id = d.department_id
LEFT JOIN enrollments e ON s.student_id = e.student_id AND e.status = 'Enrolled'
WHERE s.status = 'Active' AND s.gpa < 2.0
GROUP BY s.student_id, s.student_code, s.first_name, s.last_name, s.email, d.department_name, s.gpa, s.enrollment_date
ORDER BY s.gpa ASC;

-- ============================================
-- 7. Enrollment Trends by Semester
-- ============================================
SELECT 
    c.semester,
    c.academic_year,
    COUNT(DISTINCT e.enrollment_id) AS total_enrollments,
    COUNT(DISTINCT e.student_id) AS unique_students,
    COUNT(DISTINCT c.course_id) AS courses_offered
FROM courses c
LEFT JOIN enrollments e ON c.course_id = e.course_id
WHERE c.academic_year >= EXTRACT(YEAR FROM CURRENT_DATE) - 2
GROUP BY c.semester, c.academic_year
ORDER BY c.academic_year DESC, 
    CASE c.semester 
        WHEN 'Spring' THEN 1 
        WHEN 'Summer' THEN 2 
        WHEN 'Fall' THEN 3 
        WHEN 'Winter' THEN 4 
    END;

-- ============================================
-- 8. Course Completion Rate
-- ============================================
SELECT 
    c.course_code,
    c.course_name,
    COUNT(e.enrollment_id) AS total_enrollments,
    COUNT(CASE WHEN e.status = 'Completed' THEN 1 END) AS completed,
    COUNT(CASE WHEN e.status = 'Dropped' THEN 1 END) AS dropped,
    COUNT(CASE WHEN e.status = 'Withdrawn' THEN 1 END) AS withdrawn,
    ROUND(
        COUNT(CASE WHEN e.status = 'Completed' THEN 1 END)::DECIMAL / 
        NULLIF(COUNT(e.enrollment_id), 0) * 100, 
    1) AS completion_rate_percent
FROM courses c
LEFT JOIN enrollments e ON c.course_id = e.course_id
GROUP BY c.course_id, c.course_code, c.course_name
HAVING COUNT(e.enrollment_id) > 0
ORDER BY completion_rate_percent DESC;
