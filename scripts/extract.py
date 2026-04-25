"""
extract.py — Data Extraction Module
E-commerce Data Pipeline with Automated ETL

Extracts raw data from the source OLTP database and saves as
timestamped CSV files for downstream transformation.
Uses metadata tracking for incremental extraction support.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import logging

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
LOG_PATH = os.path.join(BASE_DIR, "logs")

os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_PATH, "extract.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Database connection from environment variables
SOURCE_DRIVER = os.environ.get("SOURCE_DB_DRIVER", "postgresql")
SOURCE_HOST = os.environ.get("SOURCE_DB_HOST", "localhost")
SOURCE_PORT = os.environ.get("SOURCE_DB_PORT", "5433")
SOURCE_USER = os.environ.get("SOURCE_DB_USER", "etl_user")
SOURCE_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD", "etl_password")
SOURCE_DB = os.environ.get("SOURCE_DB_NAME", "ecommerce_db")

# Tables to extract
TABLES = ["orders", "order_items", "customers", "products", "payments"]


def get_source_engine():
    """Create SQLAlchemy engine for the source OLTP database."""
    conn_str = (
        f"{SOURCE_DRIVER}://{SOURCE_USER}:{SOURCE_PASSWORD}"
        f"@{SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DB}"
    )
    return create_engine(conn_str, pool_pre_ping=True)


def get_last_watermark(engine, table_name):
    """Get the last extraction watermark from ETL metadata."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT last_run FROM etl_metadata "
                "WHERE table_name = :table AND status = 'success' "
                "ORDER BY last_run DESC LIMIT 1"
            ), {"table": table_name})
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        logger.warning(f"Could not read watermark for {table_name}: {e}")
        return None


def update_watermark(engine, table_name, rows_extracted, status="success"):
    """Update the ETL metadata watermark after extraction."""
    try:
        with engine.begin() as conn:
            conn.execute(text(
                """
                UPDATE etl_metadata SET
                    last_run = CURRENT_TIMESTAMP,
                    rows_extracted = :rows,
                    status = :status,
                    updated_at = CURRENT_TIMESTAMP
                WHERE table_name = :table
                """
            ), {"rows": rows_extracted, "status": status, "table": table_name})
    except Exception as e:
        logger.warning(f"Could not update watermark for {table_name}: {e}")


def extract_table(engine, table_name, incremental=True):
    """
    Extract a table from the source database to CSV.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table to extract
        incremental: If True, only extract new/updated records.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(RAW_DATA_PATH, f"{table_name}_{timestamp}.csv")

    # Build query
    if incremental:
        watermark = get_last_watermark(engine, table_name)
        if watermark:
            if "updated_at" in _get_columns(engine, table_name):
                query = f"SELECT * FROM {table_name} WHERE updated_at > '{watermark}'"
            else:
                query = f"SELECT * FROM {table_name}"
            logger.info(f"Incremental extract for {table_name} since {watermark}")
        else:
            query = f"SELECT * FROM {table_name}"
            logger.info(f"Full extract for {table_name} (no watermark)")
    else:
        query = f"SELECT * FROM {table_name}"
        logger.info(f"Full extract for {table_name}")

    # Execute extraction
    df = pd.read_sql(query, engine)
    df.to_csv(output_file, index=False, encoding="utf-8")

    # Update metadata
    update_watermark(engine, table_name, len(df))

    logger.info(f"Extracted {table_name}: {len(df)} rows → {output_file}")
    return output_file, len(df)


def _get_columns(engine, table_name):
    """Get column names for a table."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = :table AND table_schema = 'public'"
            ), {"table": table_name})
            return [row[0] for row in result.fetchall()]
    except Exception:
        return []


def run_extraction(incremental=True):
    """
    Run the full extraction pipeline.

    Returns: list of (filepath, row_count) tuples
    """
    logger.info("=" * 60)
    logger.info("EXTRACTION PIPELINE STARTED")
    logger.info("=" * 60)

    start_time = datetime.now()
    engine = get_source_engine()
    results = []

    for table_name in TABLES:
        try:
            filepath, row_count = extract_table(engine, table_name, incremental)
            results.append({"table": table_name, "file": filepath, "rows": row_count, "status": "success"})
        except Exception as e:
            logger.error(f"Extraction failed for {table_name}: {e}")
            results.append({"table": table_name, "file": None, "rows": 0, "status": "failed"})

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"EXTRACTION COMPLETE | Time: {elapsed:.2f}s")

    # Print summary
    print("\n── Extraction Summary ──")
    print(f"{'Table':<20} {'Rows':<10} {'Status':<10}")
    print("─" * 40)
    for r in results:
        print(f"{r['table']:<20} {r['rows']:<10} {r['status']:<10}")
    print(f"\nTotal time: {elapsed:.2f} seconds")

    return results


if __name__ == "__main__":
    results = run_extraction()
