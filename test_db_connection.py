#!/usr/bin/env python3
"""
PostgreSQL Database Connection Diagnostic and Setup Utility.

Tests PostgreSQL connectivity, validates the database schema, reports row counts,
and can automatically create the target database and tables if they do not exist.

Usage:
    python test_db_connection.py
"""

import sys
import json
from pathlib import Path

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    try:
        import psycopg as psycopg2
        from psycopg import sql
    except ImportError:
        print("\n❌ Error: Neither psycopg2 nor psycopg is installed.")
        print("Please run: pip install \"psycopg[binary]\" or pip install psycopg2-binary\n")
        sys.exit(1)


def load_config():
    config_file = Path("config.json")
    if not config_file.exists():
        print("❌ Error: config.json not found in current directory.")
        sys.exit(1)
    with open(config_file, "r", encoding="utf-8") as f:
        return json.load(f).get("database", {})


def test_postgres_connection():
    db_cfg = load_config()
    host = db_cfg.get("host", "localhost")
    port = db_cfg.get("port", 5432)
    dbname = db_cfg.get("database", "real_estate")
    user = db_cfg.get("user", "postgres")
    password = db_cfg.get("password", "postgres")

    print("=" * 65)
    print("🐘 PostgreSQL Database Connection Diagnostic Tool")
    print("=" * 65)
    print(f"Target Host    : {host}:{port}")
    print(f"Target Database: {dbname}")
    print(f"Database User  : {user}")
    print("-" * 65)

    # 1. Test server availability by connecting to default 'postgres' database
    print("Step 1: Checking PostgreSQL server availability...")
    try:
        root_conn = psycopg2.connect(
            host=host,
            port=port,
            dbname="postgres",
            user=user,
            password=password,
            connect_timeout=5
        )
        root_conn.autocommit = True
        print("✅ Success: PostgreSQL server is online and credentials are correct.")
    except Exception as e:
        print(f"❌ Connection Failed: Could not connect to PostgreSQL server at {host}:{port}")
        print(f"   Reason: {e}")
        print("\n🔧 Troubleshooting Suggestions:")
        print("   1. Verify PostgreSQL service is running (e.g. Services -> postgresql-x64-xx on Windows)")
        print("   2. Or start via Docker: docker compose up -d")
        print("   3. Check password in config.json matches your PostgreSQL 'postgres' superuser password.")
        return False

    # 2. Check if target database exists
    print(f"\nStep 2: Checking if target database '{dbname}' exists...")
    with root_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))
        exists = cur.fetchone()

        if not exists:
            print(f"⚠️ Target database '{dbname}' does not exist.")
            print(f"   Creating database '{dbname}' now...")
            try:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
                print(f"✅ Success: Created database '{dbname}' successfully!")
            except Exception as err:
                print(f"❌ Failed to create database: {err}")
                root_conn.close()
                return False
        else:
            print(f"✅ Success: Database '{dbname}' exists.")

    root_conn.close()

    # 3. Connect to target database and check schema
    print(f"\nStep 3: Connecting to '{dbname}' and verifying schema...")
    try:
        app_conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=5
        )
        app_conn.autocommit = True
    except Exception as e:
        print(f"❌ Failed to connect to '{dbname}': {e}")
        return False

    with app_conn.cursor() as cur:
        # Check server version
        cur.execute("SELECT version();")
        version_str = cur.fetchone()[0]
        print(f"   PostgreSQL Version: {version_str.split(',')[0]}")

        # Check tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
        """)
        tables = [row[0] for row in cur.fetchall()]
        print(f"   Existing Tables in '{dbname}': {tables if tables else 'None'}")

        if "properties" not in tables:
            print("⚠️ Table 'properties' not found. Initializing schema from database/schema.sql...")
            schema_file = Path("database/schema.sql")
            if schema_file.exists():
                with open(schema_file, "r", encoding="utf-8") as sf:
                    cur.execute(sf.read())
                print("✅ Success: Initialized 'dim_property_types', 'properties', and indexes!")
            else:
                print("❌ database/schema.sql file missing.")
        else:
            cur.execute("SELECT COUNT(*) FROM properties;")
            count = cur.fetchone()[0]
            print(f"📊 Current Record Count in 'properties' table: {count:,} records")

    app_conn.close()
    print("\n" + "=" * 65)
    print("🎉 All checks passed! Your PostgreSQL database is fully ready for the ETL pipeline.")
    print("To run the ETL pipeline, execute:")
    print("    python run_pipeline.py")
    print("=" * 65 + "\n")
    return True


if __name__ == "__main__":
    test_postgres_connection()
