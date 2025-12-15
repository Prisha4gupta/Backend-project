# Data Engineering Assignment: Student Management ETL Pipeline

A production-quality backend project that migrates Google Sheets data into a PostgreSQL (NeonDB-compatible) database using an ETL pipeline with a REST API for student registration.

## 🏗️ Project Structure

\`\`\`
data-engineering-assignment/
├── etl/                      # ETL Pipeline
│   ├── extract.py           # Data extraction from CSV/JSON/URLs
│   ├── transform.py         # Data validation & transformation
│   ├── load.py              # Database loading with upsert support
│   ├── etl.py               # Pipeline orchestrator & CLI
│   └── logs/                # ETL execution logs
├── api/
│   └── main.py              # FastAPI REST API
├── sql/
│   ├── schema.sql           # Database schema (3NF normalized)
│   ├── seed.sql             # Initial seed data
│   ├── queries.sql          # Analytical queries
│   ├── views.sql            # Database views
│   └── procedures.sql       # Stored procedures
├── scripts/
│   └── test_connection.py   # Database connection tester
├── data/
│   └── sample_students.csv  # Sample student data
├── app_script/
│   └── sheet_to_db.gs       # Google Apps Script for Sheets integration
├── requirements.txt         # Python dependencies
└── README.md               # This file
\`\`\`

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL database (NeonDB recommended)
- pip (Python package manager)

### 1. Setup Environment

\`\`\`bash
# Clone/download the project
cd data-engineering-assignment

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
\`\`\`

### 2. Configure Database

Create a `.env` file in the project root:

\`\`\`env
DATABASE_URL=postgresql://username:password@hostname:5432/database_name
\`\`\`

For NeonDB, the connection string looks like:
\`\`\`env
DATABASE_URL=postgresql://user:pass@ep-xxx.region.aws.neon.tech/neondb?sslmode=require
\`\`\`

### 3. Test Database Connection

\`\`\`bash
python scripts/test_connection.py
\`\`\`

### 4. Initialize Database Schema

Execute the SQL files in order:

\`\`\`bash
# Using psql
psql $DATABASE_URL -f sql/schema.sql
psql $DATABASE_URL -f sql/seed.sql
psql $DATABASE_URL -f sql/views.sql
psql $DATABASE_URL -f sql/procedures.sql
\`\`\`

Or use a database client (pgAdmin, DBeaver, etc.) to run the scripts.

### 5. Run ETL Pipeline

\`\`\`bash
# Run full ETL with sample data
python etl/etl.py --source data/sample_students.csv

# Dry run (extract & transform only)
python etl/etl.py --source data/sample_students.csv --dry-run

# With verbose logging
python etl/etl.py --source data/sample_students.csv --verbose
\`\`\`

### 6. Start API Server

\`\`\`bash
cd api
uvicorn main:app --reload --port 8000
\`\`\`

API will be available at `http://localhost:8000`

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## 📊 Database Design

### Entity Relationship

\`\`\`
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ departments │     │  students   │     │   courses   │
├─────────────┤     ├─────────────┤     ├─────────────┤
│ PK: dept_id │◄────│ FK: dept_id │     │ PK: course_id│
│ dept_code   │     │ PK: stud_id │     │ FK: dept_id │
│ dept_name   │     │ stud_code   │     │ course_code │
└─────────────┘     │ email (UQ)  │     └──────┬──────┘
                    └──────┬──────┘            │
                           │                   │
                           │  ┌─────────────┐  │
                           └──┤ enrollments ├──┘
                              ├─────────────┤
                              │ FK: stud_id │
                              │ FK: course_id│
                              │ grade       │
                              └─────────────┘
\`\`\`

### Tables

| Table | Description |
|-------|-------------|
| `departments` | Academic departments |
| `students` | Student personal & academic info |
| `courses` | Course catalog |
| `enrollments` | Student-course registrations |

### Constraints & Indexes

- Primary keys on all tables
- Foreign keys with proper ON DELETE actions
- UNIQUE constraints on `email`, `student_code`, `course_code`
- CHECK constraints for `gpa`, `status`, `gender`
- Indexes on frequently queried columns

## 🔄 ETL Pipeline

### How It Works

\`\`\`
┌──────────┐     ┌───────────┐     ┌────────┐
│ EXTRACT  │────►│ TRANSFORM │────►│  LOAD  │
└──────────┘     └───────────┘     └────────┘
     │                │                 │
     ▼                ▼                 ▼
 CSV/JSON/URL    Validation        PostgreSQL
                 Deduplication     Idempotent
                 Type Casting      Upserts
\`\`\`

### Extract Phase
- Supports CSV, JSON, URLs, Google Sheets
- Handles encoding issues
- Logging for traceability

### Transform Phase
- Email format validation
- GPA range validation (0.0-4.0)
- Department code validation
- Name normalization (title case)
- Deduplication by email/student_code
- Type casting with error handling

### Load Phase
- Idempotent upserts (INSERT or UPDATE)
- Batch processing for performance
- Transaction management
- Detailed error reporting

### CLI Usage

\`\`\`bash
python etl/etl.py --help

# Options:
#   --source, -s   Data source (file path or URL)
#   --type, -t     Source type: csv, json, url, google_sheet
#   --connection   Database connection string (optional)
#   --dry-run      Run without loading to database
#   --verbose      Enable debug logging
\`\`\`

## 🌐 REST API

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/register-student` | Register a new student |
| GET | `/students` | List all students |
| GET | `/students/{code}` | Get student by code |

### Register Student

\`\`\`bash
curl -X POST http://localhost:8000/register-student \
  -H "Content-Type: application/json" \
  -d '{
    "student_code": "STU100",
    "first_name": "John",
    "last_name": "Doe",
    "email": "john.doe@university.edu",
    "department_code": "CS",
    "graduation_year": 2027
  }'
\`\`\`

### Response

\`\`\`json
{
  "success": true,
  "message": "Student registered successfully",
  "student_id": 123,
  "student_code": "STU100"
}
\`\`\`

### Validation

The API validates:
- Email format and uniqueness
- Student code format and uniqueness
- Department code existence
- GPA range (0.0-4.0)
- Gender values

## 📋 Google Sheets Integration

### Setup

1. Open your Google Sheet
2. Go to **Extensions > Apps Script**
3. Paste the contents of `app_script/sheet_to_db.gs`
4. Update `API_ENDPOINT` with your API URL
5. Run `setupTrigger()` to enable auto-sync

### Sheet Format

| A | B | C | D | E | F | G | H | I | J | K |
|---|---|---|---|---|---|---|---|---|---|---|
| student_code | first_name | last_name | email | date_of_birth | gender | phone | department_code | graduation_year | gpa | status |

### Features

- **onEdit trigger**: Auto-syncs when you edit a row
- **Email validation**: Highlights invalid emails in red
- **Batch sync**: Sync all rows from the menu
- **Visual feedback**: Color-coded row status

### Menu Options

After setup, you'll see a **🎓 Student Sync** menu:
- Setup Auto-Sync Trigger
- Validate All Rows
- Sync All Rows
- Clear Status

## 🔍 SQL Queries & Analytics

### Pre-built Queries

The `sql/queries.sql` file includes:
- Student statistics by department
- Course enrollment summary
- Grade distribution analysis
- Top performing students
- Students at risk (low GPA)
- Enrollment trends by semester

### Views

| View | Description |
|------|-------------|
| `vw_student_dashboard` | Complete student info with dept |
| `vw_course_catalog` | Courses with enrollment stats |
| `vw_enrollment_details` | Full enrollment records |
| `vw_department_summary` | Department metrics |
| `vw_academic_performance` | Student performance report |

### Stored Procedures

| Function | Description |
|----------|-------------|
| `fn_register_student()` | Register with validation |
| `fn_enroll_student()` | Enroll with capacity check |
| `fn_update_grade()` | Update grade & recalc GPA |
| `fn_get_transcript()` | Get student transcript |
| `fn_department_stats()` | Department statistics |

## 🧪 Testing

### Test Database Connection

\`\`\`bash
python scripts/test_connection.py
\`\`\`

### Test ETL (Dry Run)

\`\`\`bash
python etl/etl.py --source data/sample_students.csv --dry-run
\`\`\`

### Test API

\`\`\`bash
# Start server
uvicorn api.main:app --reload

# Test health endpoint
curl http://localhost:8000/health

# Test registration
curl -X POST http://localhost:8000/register-student \
  -H "Content-Type: application/json" \
  -d '{"student_code":"TEST001","first_name":"Test","last_name":"User","email":"test@example.edu"}'
\`\`\`

## 📝 Logging

ETL logs are stored in `etl/logs/` with timestamps:
\`\`\`
etl/logs/etl_20241215_143022.log
\`\`\`

Each log includes:
- Timestamp
- Phase (extract/transform/load)
- Record counts
- Validation errors
- Performance metrics

## 🔒 Security Notes

1. **Never commit `.env`** - Contains database credentials
2. **Use SSL** - NeonDB requires `sslmode=require`
3. **Validate all inputs** - Both ETL and API validate data
4. **Use parameterized queries** - Prevents SQL injection

## 📚 Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| psycopg2-binary | 2.9.9 | PostgreSQL driver |
| sqlalchemy | 2.0.23 | ORM & connection pooling |
| pandas | 2.1.3 | Data manipulation |
| fastapi | 0.104.1 | REST API framework |
| uvicorn | 0.24.0 | ASGI server |
| pydantic | 2.5.2 | Data validation |
| python-dotenv | 1.0.0 | Environment config |

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## 📄 License

MIT License - Feel free to use and modify.
