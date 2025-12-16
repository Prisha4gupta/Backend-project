# Student Data Engineering & Automation Pipeline

This project demonstrates a full-cycle **Data Engineering migration** workflow. It automates the extraction of student data from **Google Sheets**, validates it via a **FastAPI** backend, and loads it into a **PostgreSQL (NeonDB)** warehouse.
It includes a robust **ETL pipeline** (Extract, Transform, Load) for relational data (Students, Courses, Enrollments), automated email notifications for data errors, and a showcase of processing messy public datasets.

---

## 🚀 Features

* **Automated Sync:** Real-time data sync from Google Sheets to NeonDB via Google Apps Script triggers.
* **Data Validation:** Strict validation for GPA, Email, and Foreign Key constraints before insertion.
* **Relational ETL:** A Python-based pipeline handling dependencies between `Departments` -> `Courses` -> `Students` -> `Enrollments`.
* **Interactive Dashboard:** Real-time feedback in Google Sheets (Rows turn Green/Red based on status).
* **Public Dataset Processing:** Capable of ingesting and cleaning messy raw CSV data.
---

## 🛠️ Tech Stack
* **Database:** PostgreSQL (NeonDB Serverless)
* **Backend:** Python (FastAPI) hosted on **Render**
* **ETL Engine:** Python (Pandas, SQLAlchemy, Psycopg2)
* **Automation:** Google Apps Script (JavaScript)
* **Documentation:** Swagger UI

---
## 🏗️ Project Structure

```text
Backend-project
├── api
│   └── main.py                 # FastAPI backend for real-time registration
├── app_script
│   └── sheet_to_db.gs          # Google Apps Script for Sheet automation
├── data
│   ├── courses.csv             # Source data for courses
│   ├── enrollments.csv         # Source data for enrollments
│   └── sample_students.csv     # Source data for students
├── etl
│   ├── __init__.py
│   ├── pipeline.py             # Main pipeline controller
│   ├── extract.py              # Data extraction logic
│   ├── transform.py            # Cleaning & validation logic (Pandas)
│   ├── load.py                 # Database loading logic (SQLAlchemy)
│   ├── run_courses_etl.py      # Script to load Courses
│   ├── run_enrollments_etl.py  # Script to load Enrollments
│   └── public_dataset_etl.py   # Script for Public Dataset 
├── sql
│   ├── schema.sql              # Database Table Definitions
│   ├── seed.sql                # Initial data seeds
│   ├── queries.sql             # Analytical queries
│   └── procedures.sql          # Stored Procedures (Registration logic)
├── .env                        # Environment variables (Database URL)
├── requirements.txt            # Python dependencies
└── README.md                   # Project Documentation
```

---


## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL database (NeonDB recommended)
- pip (Python package manager)

---

### 1. Run Locally
```bash
# Setup
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Test DB
python scripts/test_connection.py

# Run ETL
python etl/etl.py --source data/sample_students.csv
python -m etl.run_courses_etl
python -m etl.run_enrollments_etl

# Start API
cd api
uvicorn main:app --reload

```
---


## 🌐 API Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET    | `/health` | Health check |
| POST   | `/register-student` | Register student |
| GET    | `/students` | List students |
| GET    | `/students/{code}` | Student details |
---

## 🗄️ Database Highlights

- Normalized relational schema
- Foreign key constraints
- SQL views for dashboards
- Stored procedures for enrollment & analytics
  
---

## 🔒 Security & Best Practices

- .env for secrets (not committed)
- SSL-enabled DB connections
- Input validation at ETL & API level
- Idempotent DB writes
