# Student Management ETL Pipeline

A production-ready data engineering backend that ingests student data from CSV / Google Sheets into PostgreSQL (NeonDB) using an ETL pipeline, with a FastAPI REST API for student registration and analytics.

---

## 🏗️ Project Structure

```text
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
```

---

## 🚀 Features

- ETL Pipeline (Extract → Transform → Load)
- PostgreSQL (NeonDB) with normalized schema (3NF)
- FastAPI REST API for student registration
- SQL Views & Stored Procedures for analytics
- Batch processing & idempotent upserts
- Connection & pipeline testing
- Optional Google Sheets integration

---

## ⚙️ Tech Stack

- Python 3.10+
- FastAPI
- PostgreSQL (NeonDB)
- SQLAlchemy + psycopg2
- Pandas
- Google Apps Script (optional)
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
