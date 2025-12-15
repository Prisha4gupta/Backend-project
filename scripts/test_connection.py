"""
Database Connection Test Script
-------------------------------
Tests PostgreSQL/NeonDB connectivity and displays database info.

Usage:
    python test_connection.py

Author: Data Engineering Team
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def test_connection():
    """
    Test database connection and display information.
    """
    import psycopg2
    from psycopg2 import sql
    
    connection_string = os.getenv('DATABASE_URL')
    
    if not connection_string:
        print("❌ ERROR: DATABASE_URL environment variable not set")
        print("\nTo set it, create a .env file with:")
        print("DATABASE_URL=postgresql://user:password@host:port/database")
        return False
    
    # Mask password in output
    masked_url = connection_string
    if '@' in connection_string:
        parts = connection_string.split('@')
        if ':' in parts[0]:
            prefix = parts[0].rsplit(':', 1)[0]
            masked_url = f"{prefix}:****@{parts[1]}"
    
    print("=" * 60)
    print("DATABASE CONNECTION TEST")
    print("=" * 60)
    print(f"\nConnection string: {masked_url}")
    
    try:
        print("\n📡 Connecting to database...")
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        
        print("✅ Connection successful!")
        
        # Get database version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"\n📊 Database Version:\n   {version[:80]}...")
        
        # Get current database
        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()[0]
        print(f"\n📁 Current Database: {db_name}")
        
        # Get current user
        cursor.execute("SELECT current_user;")
        user = cursor.fetchone()[0]
        print(f"👤 Current User: {user}")
        
        # List tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        
        print(f"\n📋 Tables in public schema ({len(tables)}):")
        if tables:
            for table in tables:
                # Get row count
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier(table[0])
                    )
                )
                count = cursor.fetchone()[0]
                print(f"   • {table[0]}: {count} rows")
        else:
            print("   (No tables found - run schema.sql to create tables)")
        
        # List views
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """)
        views = cursor.fetchall()
        
        if views:
            print(f"\n👁️  Views ({len(views)}):")
            for view in views:
                print(f"   • {view[0]}")
        
        # List functions
        cursor.execute("""
            SELECT routine_name 
            FROM information_schema.routines 
            WHERE routine_schema = 'public' 
            AND routine_type = 'FUNCTION'
            ORDER BY routine_name;
        """)
        functions = cursor.fetchall()
        
        if functions:
            print(f"\n⚙️  Functions ({len(functions)}):")
            for func in functions:
                print(f"   • {func[0]}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ All connection tests passed!")
        print("=" * 60)
        
        return True
        
    except psycopg2.OperationalError as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nCommon issues:")
        print("  • Check if the database server is running")
        print("  • Verify the connection string is correct")
        print("  • Ensure your IP is whitelisted (for cloud databases)")
        return False
        
    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        return False
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return False


def test_sqlalchemy():
    """
    Test SQLAlchemy connection.
    """
    from sqlalchemy import create_engine, text
    
    connection_string = os.getenv('DATABASE_URL')
    
    if not connection_string:
        return False
    
    print("\n" + "=" * 60)
    print("SQLALCHEMY CONNECTION TEST")
    print("=" * 60)
    
    try:
        print("\n📡 Creating SQLAlchemy engine...")
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            row = result.fetchone()
            
            if row[0] == 1:
                print("✅ SQLAlchemy connection successful!")
                return True
            
    except Exception as e:
        print(f"❌ SQLAlchemy error: {e}")
        return False


if __name__ == "__main__":
    print("\n🔧 Starting database connection tests...\n")
    
    # Test basic connection
    psycopg_ok = test_connection()
    
    # Test SQLAlchemy
    if psycopg_ok:
        sqlalchemy_ok = test_sqlalchemy()
    else:
        sqlalchemy_ok = False
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"psycopg2:    {'✅ PASS' if psycopg_ok else '❌ FAIL'}")
    print(f"SQLAlchemy:  {'✅ PASS' if sqlalchemy_ok else '❌ FAIL'}")
    
    sys.exit(0 if psycopg_ok and sqlalchemy_ok else 1)
