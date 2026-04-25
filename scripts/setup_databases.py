"""
setup_databases.py — Database Initialization for Docker
E-commerce Data Pipeline with Automated ETL

Creates both the source (OLTP) and warehouse (OLAP) databases
in the Dockerized PostgreSQL instance, then initializes their
schemas and seeds the source with synthetic data.

Run this before the ETL pipeline on first startup.
"""

import os
import sys
import time
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from generate_large_data import generate_large_data

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Connection parameters (from environment or defaults)
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5433")
PG_USER = os.environ.get("PG_USER", "etl_user")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "etl_password")

SOURCE_DB = os.environ.get("SOURCE_DB_NAME", "ecommerce_db")
WAREHOUSE_DB = os.environ.get("DW_NAME", "ecommerce_dw")

SOURCE_SCHEMA_FILE = os.path.join(BASE_DIR, "warehouse_schema", "source_schema.sql")
WAREHOUSE_SCHEMA_FILE = os.path.join(BASE_DIR, "warehouse_schema", "warehouse_docker.sql")

ENABLE_LARGE_DATA = os.environ.get("ENABLE_LARGE_DATA", "true").lower() in {
    "1", "true", "yes", "y",
}
LARGE_DATA_CUSTOMERS = int(os.environ.get("LARGE_DATA_CUSTOMERS", "2000"))
LARGE_DATA_PRODUCTS = int(os.environ.get("LARGE_DATA_PRODUCTS", "500"))
LARGE_DATA_ORDERS = int(os.environ.get("LARGE_DATA_ORDERS", "15000"))
LARGE_DATA_LOOKBACK_DAYS = int(os.environ.get("LARGE_DATA_LOOKBACK_DAYS", "365"))


def wait_for_postgres(max_retries=30, delay=2):
    """Wait for PostgreSQL to become available."""
    print(f"Waiting for PostgreSQL at {PG_HOST}:{PG_PORT}...")
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                user=PG_USER, password=PG_PASSWORD,
                dbname="postgres",
            )
            conn.close()
            print(f"  PostgreSQL is ready (attempt {attempt})")
            return True
        except psycopg2.OperationalError:
            if attempt < max_retries:
                print(f"  Attempt {attempt}/{max_retries} — retrying in {delay}s...")
                time.sleep(delay)
            else:
                print("  ERROR: PostgreSQL did not become available.")
                return False


def create_database(db_name):
    """Create a database if it does not already exist."""
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname="postgres",
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    if cur.fetchone():
        print(f"  Database '{db_name}' already exists — skipping creation.")
    else:
        cur.execute(f'CREATE DATABASE "{db_name}"')
        print(f"  Database '{db_name}' created.")

    cur.close()
    conn.close()


def run_sql_file(db_name, filepath):
    """Execute a SQL file against the specified database."""
    if not os.path.exists(filepath):
        print(f"  WARNING: SQL file not found: {filepath}")
        return False

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname=db_name,
    )
    cur = conn.cursor()

    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()

    try:
        cur.execute(sql)
        conn.commit()
        print(f"  Schema loaded into '{db_name}' from {os.path.basename(filepath)}")
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
        print(f"  Tables already exist in '{db_name}' — skipping schema load.")
    except Exception as e:
        conn.rollback()
        print(f"  WARNING: Error loading schema into '{db_name}': {e}")
        # Try statement-by-statement execution as fallback
        print(f"  Retrying statement-by-statement...")
        conn2 = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname=db_name,
        )
        cur2 = conn2.cursor()
        success_count = 0
        error_count = 0
        for statement in sql.split(";"):
            stmt = statement.strip()
            if not stmt or stmt.startswith("--"):
                continue
            try:
                cur2.execute(stmt)
                conn2.commit()
                success_count += 1
            except Exception:
                conn2.rollback()
                error_count += 1
        print(f"  Statement-by-statement: {success_count} succeeded, {error_count} skipped")
        cur2.close()
        conn2.close()

    cur.close()
    conn.close()
    return True


def verify_setup():
    """Verify that both databases have tables."""
    for db_name in [SOURCE_DB, WAREHOUSE_DB]:
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                user=PG_USER, password=PG_PASSWORD,
                dbname=db_name,
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cur.fetchall()]
            print(f"  {db_name}: {len(tables)} tables — {', '.join(tables)}")
            cur.close()
            conn.close()
        except Exception as e:
            print(f"  WARNING: Could not verify {db_name}: {e}")


def maybe_generate_large_data():
    """Generate high-volume synthetic source data when enabled."""
    if not ENABLE_LARGE_DATA:
        print("  Large data generation disabled (ENABLE_LARGE_DATA=false).")
        return

    print(
        "  Target volumes: "
        f"customers={LARGE_DATA_CUSTOMERS}, "
        f"products={LARGE_DATA_PRODUCTS}, "
        f"orders={LARGE_DATA_ORDERS}"
    )

    summary = generate_large_data(
        target_customers=LARGE_DATA_CUSTOMERS,
        target_products=LARGE_DATA_PRODUCTS,
        target_orders=LARGE_DATA_ORDERS,
        lookback_days=LARGE_DATA_LOOKBACK_DAYS,
    )

    inserted = summary["inserted"]
    final_counts = summary["final_counts"]

    print(
        "  Inserted: "
        f"customers={inserted['customers']}, "
        f"products={inserted['products']}, "
        f"orders={inserted['orders']}, "
        f"order_items={inserted['order_items']}, "
        f"payments={inserted['payments']}"
    )
    print(
        "  Final source counts: "
        f"customers={final_counts['customers']}, "
        f"products={final_counts['products']}, "
        f"orders={final_counts['orders']}, "
        f"order_items={final_counts['order_items']}, "
        f"payments={final_counts['payments']}"
    )


def main():
    print("=" * 60)
    print("  DATABASE SETUP")
    print("=" * 60)

    # Step 1: Wait for PostgreSQL
    if not wait_for_postgres():
        sys.exit(1)

    # Step 2: Create databases
    print("\nCreating databases...")
    create_database(SOURCE_DB)
    create_database(WAREHOUSE_DB)

    # Step 3: Initialize source database (OLTP schema)
    print(f"\nInitializing source database ({SOURCE_DB})...")
    run_sql_file(SOURCE_DB, SOURCE_SCHEMA_FILE)

    # Step 3b: Seed source with synthetic data
    print("\nGenerating realistic synthetic data...")
    maybe_generate_large_data()

    # Step 4: Initialize warehouse database (star schema)
    print(f"\nInitializing warehouse database ({WAREHOUSE_DB})...")
    run_sql_file(WAREHOUSE_DB, WAREHOUSE_SCHEMA_FILE)

    # Step 5: Verify
    print("\nVerifying setup...")
    verify_setup()

    print("\n" + "=" * 60)
    print("  DATABASE SETUP COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
