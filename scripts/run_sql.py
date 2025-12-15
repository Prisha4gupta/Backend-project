import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env")

engine = create_engine(DATABASE_URL)

def run_sql_file(path):
    print(f"\nRunning {path}...")
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()

    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

    print(f"✅ Finished {path}")

if __name__ == "__main__":
    run_sql_file("sql/schema.sql")
    run_sql_file("sql/seed.sql")
    run_sql_file("sql/views.sql")
    run_sql_file("sql/procedures.sql")

    print("\n🎉 All SQL files executed successfully!")
