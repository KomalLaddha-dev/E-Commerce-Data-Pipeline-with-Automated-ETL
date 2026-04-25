"""
load.py — Data Loading Module
E-commerce Data Pipeline with Automated ETL

Loads processed dimension and fact tables into the data warehouse.
Supports full reload (dimensions) and append (facts).
Handles materialized view refreshes.
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
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, "data", "processed")
LOG_PATH = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_PATH, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_PATH, "load.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Warehouse connection from environment variables
DW_DRIVER = os.environ.get("DW_DRIVER", "postgresql")
DW_HOST = os.environ.get("DW_HOST", "localhost")
DW_PORT = os.environ.get("DW_PORT", "5433")
DW_USER = os.environ.get("DW_USER", "etl_user")
DW_PASSWORD = os.environ.get("DW_PASSWORD", "etl_password")
DW_NAME = os.environ.get("DW_NAME", "ecommerce_dw")


def get_warehouse_engine():
    """Create SQLAlchemy engine for the data warehouse."""
    conn_str = (
        f"{DW_DRIVER}://{DW_USER}:{DW_PASSWORD}"
        f"@{DW_HOST}:{DW_PORT}/{DW_NAME}"
    )
    return create_engine(conn_str, pool_pre_ping=True)


def get_table_columns(engine, table_name):
    """Retrieve the column names of a warehouse table to filter CSV data."""
    query = text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = :tbl "
        "ORDER BY ordinal_position"
    )
    with engine.connect() as conn:
        result = conn.execute(query, {"tbl": table_name})
        return [row[0] for row in result]


def filter_columns(df, engine, table_name):
    """Keep only the DataFrame columns that exist in the target table."""
    db_columns = get_table_columns(engine, table_name)
    if not db_columns:
        logger.warning(f"Could not retrieve columns for {table_name} — loading all CSV columns")
        return df
    valid = [c for c in df.columns if c in db_columns]
    skipped = [c for c in df.columns if c not in db_columns]
    if skipped:
        logger.info(f"  {table_name}: dropping extra CSV columns not in schema: {skipped}")
    return df[valid]


def load_dimension(engine, filepath, table_name, key_column=None):
    """
    Load a dimension table with full reload strategy.
    Clears existing rows and inserts fresh data.
    """
    df = pd.read_csv(filepath)

    # Filter to only columns that exist in the warehouse table
    df = filter_columns(df, engine, table_name)

    with engine.begin() as conn:
        # Clear and reload (dimensions are small)
        conn.execute(text(f"DELETE FROM {table_name}"))
        logger.info(f"Cleared {table_name} for full reload")

    # Use pandas to insert
    df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")

    logger.info(f"Loaded {table_name}: {len(df)} rows")
    return len(df)


def load_fact(engine, filepath, table_name):
    """
    Load a fact table with truncate-and-append strategy.
    Suitable for batch ETL where we rebuild facts each run.
    """
    df = pd.read_csv(filepath)

    # Filter to only columns that exist in the warehouse table
    df = filter_columns(df, engine, table_name)

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
        logger.info(f"Truncated {table_name}")

    # Insert in chunks to avoid memory issues
    chunk_size = 5000
    total = 0
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        chunk.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
        total += len(chunk)
        logger.info(f"  {table_name}: loaded chunk {start} to {start + len(chunk)}")

    logger.info(f"Loaded {table_name}: {total} rows")
    return total


def refresh_materialized_views(engine):
    """Refresh all materialized views for fast dashboard queries."""
    views = ["mv_daily_sales", "mv_product_performance", "mv_customer_summary"]
    refreshed = []

    for view in views:
        try:
            with engine.begin() as conn:
                # Use CONCURRENTLY if possible (requires unique index)
                try:
                    conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"))
                except Exception:
                    conn.execute(text(f"REFRESH MATERIALIZED VIEW {view}"))
                refreshed.append(view)
                logger.info(f"Refreshed: {view}")
        except Exception as e:
            logger.warning(f"Could not refresh {view}: {e}")

    return refreshed


def log_audit(engine, batch_id, table_name, rows, status, error=None):
    """Write an audit log entry to the warehouse."""
    try:
        with engine.begin() as conn:
            conn.execute(text(
                """
                INSERT INTO etl_audit_log (batch_id, table_name, rows_processed,
                                           status, started_at, completed_at, error_message)
                VALUES (:batch, :table, :rows, :status, NOW(), NOW(), :err)
                """
            ), {"batch": batch_id, "table": table_name,
                "rows": rows, "status": status, "err": error})
    except Exception:
        pass  # Don't fail the pipeline for audit log issues


def run_loading():
    """
    Execute the full loading pipeline.

    Order:
    1. Dimension tables first (no FK dependencies between dims)
    2. Fact table (depends on all dimensions)
    3. Refresh materialized views
    """
    logger.info("=" * 60)
    logger.info("LOADING PIPELINE STARTED")
    logger.info("=" * 60)

    start_time = datetime.now()
    batch_id = start_time.strftime("BATCH_%Y%m%d_%H%M%S")
    engine = get_warehouse_engine()

    results = {}

    # ── Load Dimensions (order: date, payment_method, location, customer, product) ──
    dim_load_order = [
        ("dim_date.csv", "dim_date"),
        ("dim_payment_method.csv", "dim_payment_method"),
        ("dim_location.csv", "dim_location"),
        ("dim_customer.csv", "dim_customer"),
        ("dim_product.csv", "dim_product"),
    ]

    for filename, table_name in dim_load_order:
        filepath = os.path.join(PROCESSED_DATA_PATH, filename)
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath} — skipping {table_name}")
            results[table_name] = 0
            continue

        try:
            rows = load_dimension(engine, filepath, table_name)
            results[table_name] = rows
            log_audit(engine, batch_id, table_name, rows, "success")
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            results[table_name] = 0
            log_audit(engine, batch_id, table_name, 0, "failed", str(e))

    # ── Load Fact Table ──
    fact_file = os.path.join(PROCESSED_DATA_PATH, "fact_order_items.csv")
    if os.path.exists(fact_file):
        try:
            rows = load_fact(engine, fact_file, "fact_order_items")
            results["fact_order_items"] = rows
            log_audit(engine, batch_id, "fact_order_items", rows, "success")
        except Exception as e:
            logger.error(f"Failed to load fact_order_items: {e}")
            results["fact_order_items"] = 0
            log_audit(engine, batch_id, "fact_order_items", 0, "failed", str(e))
    else:
        logger.warning(f"Fact file not found: {fact_file}")
        results["fact_order_items"] = 0

    # ── Refresh Materialized Views ──
    logger.info("Refreshing materialized views...")
    refreshed = refresh_materialized_views(engine)

    # ── Summary ──
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"LOADING COMPLETE | Time: {elapsed:.2f}s | Batch: {batch_id}")

    print("\n── Loading Summary ──")
    print(f"{'Table':<25} {'Rows':<10}")
    print("─" * 35)
    for table, count in results.items():
        print(f"{table:<25} {count:<10}")
    print(f"\nMaterialized views refreshed: {', '.join(refreshed) if refreshed else 'None'}")
    print(f"Total time: {elapsed:.2f} seconds")

    return results


if __name__ == "__main__":
    results = run_loading()
