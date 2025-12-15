-- ============================================
-- SEED DATA: Initial data for testing
-- ============================================

-- Insert Departments
INSERT INTO departments (department_code, department_name, head_of_department, established_year)
VALUES 
    ('CS', 'Computer Science', 'Dr. Alan Turing', 1985),
    ('MATH', 'Mathematics', 'Dr. Emmy Noether', 1960),
    ('PHY', 'Physics', 'Dr. Richard Feynman', 1955),
    ('ENG', 'English Literature', 'Dr. Jane Austen', 1970),
    ('BIO', 'Biology', 'Dr. Charles Darwin', 1965),
    ('CHEM', 'Chemistry', 'Dr. Marie Curie', 1958),
    ('ECON', 'Economics', 'Dr. Adam Smith', 1980),
    ('PSY', 'Psychology', 'Dr. Carl Jung', 1975)
ON CONFLICT (department_code) DO NOTHING;

-- Insert Courses
INSERT INTO courses (course_code, course_name, description, credits, department_id, instructor, max_enrollment, semester, academic_year, is_active)
VALUES 
    ('CS101', 'Introduction to Programming', 'Fundamentals of programming using Python', 3, 1, 'Prof. Guido van Rossum', 100, 'Fall', 2024, TRUE),
    ('CS201', 'Data Structures', 'Advanced data structures and algorithms', 4, 1, 'Prof. Donald Knuth', 60, 'Spring', 2024, TRUE),
    ('CS301', 'Database Systems', 'Relational databases and SQL', 3, 1, 'Prof. Edgar Codd', 50, 'Fall', 2024, TRUE),
    ('CS401', 'Machine Learning', 'Introduction to ML algorithms', 4, 1, 'Prof. Andrew Ng', 40, 'Spring', 2024, TRUE),
    ('MATH101', 'Calculus I', 'Differential calculus fundamentals', 4, 2, 'Prof. Isaac Newton', 80, 'Fall', 2024, TRUE),
    ('MATH201', 'Linear Algebra', 'Vectors, matrices, and linear transformations', 3, 2, 'Prof. Gilbert Strang', 60, 'Spring', 2024, TRUE),
    ('PHY101', 'Physics I', 'Classical mechanics and thermodynamics', 4, 3, 'Prof. Albert Einstein', 70, 'Fall', 2024, TRUE),
    ('PHY201', 'Quantum Mechanics', 'Introduction to quantum physics', 4, 3, 'Prof. Niels Bohr', 30, 'Spring', 2024, TRUE),
    ('ENG101', 'English Composition', 'Academic writing fundamentals', 3, 4, 'Prof. William Strunk', 40, 'Fall', 2024, TRUE),
    ('BIO101', 'Biology Fundamentals', 'Introduction to life sciences', 4, 5, 'Prof. Gregor Mendel', 50, 'Fall', 2024, TRUE)
ON CONFLICT (course_code) DO NOTHING;

-- Insert Sample Students
INSERT INTO students (student_code, first_name, last_name, email, date_of_birth, gender, phone, department_id, enrollment_date, graduation_year, gpa, status)
VALUES 
    ('STU001', 'John', 'Doe', 'john.doe@university.edu', '2002-05-15', 'Male', '+1-555-0101', 1, '2023-09-01', 2027, 3.75, 'Active'),
    ('STU002', 'Jane', 'Smith', 'jane.smith@university.edu', '2001-08-22', 'Female', '+1-555-0102', 1, '2022-09-01', 2026, 3.90, 'Active'),
    ('STU003', 'Michael', 'Johnson', 'michael.j@university.edu', '2003-01-10', 'Male', '+1-555-0103', 2, '2024-01-15', 2028, 3.50, 'Active'),
    ('STU004', 'Emily', 'Williams', 'emily.w@university.edu', '2002-11-30', 'Female', '+1-555-0104', 3, '2023-09-01', 2027, 3.85, 'Active'),
    ('STU005', 'David', 'Brown', 'david.brown@university.edu', '2001-03-25', 'Male', '+1-555-0105', 1, '2022-09-01', 2026, 2.95, 'Active')
ON CONFLICT (student_code) DO NOTHING;

-- Insert Sample Enrollments
INSERT INTO enrollments (student_id, course_id, enrollment_date, grade, grade_points, status)
VALUES 
    (1, 1, '2023-09-01', 'A', 4.00, 'Completed'),
    (1, 2, '2024-01-15', 'A-', 3.70, 'Completed'),
    (1, 3, '2024-09-01', NULL, NULL, 'Enrolled'),
    (2, 1, '2022-09-01', 'A+', 4.00, 'Completed'),
    (2, 2, '2023-01-15', 'A', 4.00, 'Completed'),
    (2, 4, '2024-01-15', 'B+', 3.30, 'Completed'),
    (3, 5, '2024-01-15', NULL, NULL, 'Enrolled'),
    (3, 6, '2024-01-15', NULL, NULL, 'Enrolled'),
    (4, 7, '2023-09-01', 'A-', 3.70, 'Completed'),
    (4, 8, '2024-01-15', NULL, NULL, 'Enrolled'),
    (5, 1, '2022-09-01', 'B', 3.00, 'Completed'),
    (5, 2, '2023-01-15', 'C+', 2.30, 'Completed')
ON CONFLICT (student_id, course_id) DO NOTHING;
