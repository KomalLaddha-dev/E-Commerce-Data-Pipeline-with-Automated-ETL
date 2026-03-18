"""
load.py — Data Loading Module
E-commerce Data Pipeline with Automated ETL

Loads transformed data from the processed staging area into the
target data warehouse (PostgreSQL / Snowflake / BigQuery).
Supports full-reload for dimensions and upsert for fact tables.
"""

import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os
from datetime import datetime

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
DW_HOST = os.environ.get("DW_HOST", "localhost")
DW_PORT = os.environ.get("DW_PORT", "5432")
DW_USER = os.environ.get("DW_USER", "warehouse_user")
DW_PASSWORD = os.environ.get("DW_PASSWORD", "warehouse_pass")
DW_NAME = os.environ.get("DW_NAME", "ecommerce_dw")
DW_DRIVER = os.environ.get("DW_DRIVER", "postgresql")

WAREHOUSE_CONNECTION = f"{DW_DRIVER}://{DW_USER}:{DW_PASSWORD}@{DW_HOST}:{DW_PORT}/{DW_NAME}"

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


# ──────────────────────────────────────────────
# Database Connection
# ──────────────────────────────────────────────
def get_warehouse_engine():
    """Create and return a data warehouse SQLAlchemy engine."""
    try:
        engine = create_engine(WAREHOUSE_CONNECTION, echo=False, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Warehouse connection established successfully.")
        return engine
    except Exception as e:
        logger.error(f"Warehouse connection failed: {e}")
        raise


# ──────────────────────────────────────────────
# Loading Strategies
# ──────────────────────────────────────────────
def load_dimension_full_reload(engine, df, table_name):
    """
    Load a dimension table using FULL RELOAD strategy.

    This truncates the existing table and loads fresh data.
    Best for small dimension tables that change infrequently.

    Parameters:
        engine:     SQLAlchemy engine for the warehouse
        df:         DataFrame with the dimension data
        table_name: Target table name in the warehouse
    """
    try:
        with engine.begin() as conn:
            # Truncate existing data (ignore error if table is empty/new)
            try:
                conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                logger.info(f"Truncated {table_name}")
            except Exception as trunc_err:
                logger.warning(f"Truncate skipped for {table_name}: {trunc_err}")

        # Filter DataFrame columns to only those that exist in the target table
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{table_name}' AND table_schema = 'public'"
            ))
            table_columns = {row[0] for row in result}

        if table_columns:
            # Only keep columns that exist in both DataFrame and target table
            common_cols = [c for c in df.columns if c in table_columns]
            df = df[common_cols]

        # Load new data
        df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
        logger.info(f"Loaded {len(df)} rows into {table_name} (full reload)")

    except Exception as e:
        logger.error(f"Failed to load {table_name}: {e}")
        raise


def load_fact_upsert(engine, df, table_name, conflict_column="order_id"):
    """
    Load a fact table using UPSERT (INSERT ... ON CONFLICT) strategy.

    New records are inserted; existing records are updated.
    Uses a staging table for atomic operations.

    Parameters:
        engine:          SQLAlchemy engine for the warehouse
        df:              DataFrame with the fact data
        table_name:      Target fact table name
        conflict_column: Column used to detect duplicates
    """
    staging_table = f"staging_{table_name}"

    try:
        # Step 1: Load data into staging table
        df.to_sql(staging_table, engine, if_exists="replace", index=False, method="multi")
        logger.info(f"Staged {len(df)} rows into {staging_table}")

        # Step 2: Upsert from staging into target
        # Get column names (excluding auto-generated ones)
        columns = [c for c in df.columns if c not in ["sale_id"]]
        update_columns = [c for c in columns if c != conflict_column]

        update_clause = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in update_columns
        )

        upsert_sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM {staging_table}
            ON CONFLICT ({conflict_column})
            DO UPDATE SET {update_clause};
        """

        with engine.begin() as conn:
            result = conn.execute(text(upsert_sql))
            logger.info(f"Upserted into {table_name}: {result.rowcount} rows affected")

            # Step 3: Clean up staging table
            conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
            logger.info(f"Dropped staging table {staging_table}")

    except Exception as e:
        logger.error(f"Failed to upsert into {table_name}: {e}")
        # Attempt cleanup on failure
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
        except Exception:
            pass
        raise


# ──────────────────────────────────────────────
# Data Quality Checks (Post-Load)
# ──────────────────────────────────────────────
def run_post_load_checks(engine):
    """
    Run data quality validations after loading.

    Checks:
    1. Fact table is not empty
    2. No null foreign keys in fact table
    3. No negative amounts
    4. Referential integrity between fact and dimensions
    """
    checks_passed = True
    checks = []

    with engine.connect() as conn:
        # Check 1: Fact table row count
        count = conn.execute(text("SELECT COUNT(*) FROM sales_fact")).scalar()
        passed = count > 0
        checks.append(("sales_fact not empty", passed, f"{count} rows"))
        if not passed:
            checks_passed = False

        # Check 2: No null customer IDs
        null_customers = conn.execute(
            text("SELECT COUNT(*) FROM sales_fact WHERE customer_id IS NULL")
        ).scalar()
        passed = null_customers == 0
        checks.append(("No null customer_ids", passed, f"{null_customers} nulls"))
        if not passed:
            logger.warning(f"Found {null_customers} null customer_ids in sales_fact")

        # Check 3: No negative net amounts
        negatives = conn.execute(
            text("SELECT COUNT(*) FROM sales_fact WHERE net_amount < 0")
        ).scalar()
        passed = negatives == 0
        checks.append(("No negative amounts", passed, f"{negatives} negatives"))
        if not passed:
            checks_passed = False

        # Check 4: All date_keys exist in date_dim
        orphan_dates = conn.execute(text("""
            SELECT COUNT(*) FROM sales_fact f
            LEFT JOIN date_dim d ON f.date_key = d.date_key
            WHERE d.date_key IS NULL
        """)).scalar()
        passed = orphan_dates == 0
        checks.append(("Date referential integrity", passed, f"{orphan_dates} orphans"))
        if not passed:
            checks_passed = False

    # Print results
    print("\n── Post-Load Quality Checks ──")
    for check_name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {check_name}: {detail}")
        logger.info(f"Quality Check [{status}] {check_name}: {detail}")

    if not checks_passed:
        logger.error("One or more quality checks FAILED!")
        raise ValueError("Post-load data quality checks failed. See logs for details.")

    logger.info("All post-load quality checks PASSED.")
    return checks_passed


# ──────────────────────────────────────────────
# Refresh Materialized Views
# ──────────────────────────────────────────────
def refresh_materialized_views(engine):
    """Refresh materialized views after data load."""
    views = [
        "mv_daily_sales_summary",
        "mv_product_performance",
    ]
    for view in views:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"))
            logger.info(f"Refreshed materialized view: {view}")
        except Exception as e:
            logger.warning(f"Could not refresh {view}: {e}")


# ──────────────────────────────────────────────
# Main Loading Pipeline
# ──────────────────────────────────────────────
def run_loading():
    """
    Execute the full loading pipeline.

    Steps:
    1. Connect to data warehouse
    2. Load dimension tables (full reload)
    3. Load fact table (upsert)
    4. Run post-load quality checks
    5. Refresh materialized views

    Returns:
        dict with load summary
    """
    logger.info("=" * 60)
    logger.info("LOADING PIPELINE STARTED")
    logger.info("=" * 60)

    start_time = datetime.now()
    engine = get_warehouse_engine()
    load_summary = []

    # ── Load Dimension Tables (Full Reload) ──
    dimensions = {
        "date_dim": "date_dim.csv",
        "customer_dim": "customer_dim.csv",
        "product_dim": "product_dim.csv",
        "payment_dim": "payment_dim.csv",
    }

    for table_name, filename in dimensions.items():
        filepath = os.path.join(PROCESSED_DATA_PATH, filename)
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath}. Skipping {table_name}.")
            continue

        df = pd.read_csv(filepath)
        load_dimension_full_reload(engine, df, table_name)
        load_summary.append({"table": table_name, "rows": len(df), "strategy": "full_reload"})

    # ── Load Fact Table (Upsert) ──
    fact_filepath = os.path.join(PROCESSED_DATA_PATH, "sales_fact.csv")
    if os.path.exists(fact_filepath):
        sales_df = pd.read_csv(fact_filepath)
        load_fact_upsert(engine, sales_df, "sales_fact", conflict_column="order_id")
        load_summary.append({"table": "sales_fact", "rows": len(sales_df), "strategy": "upsert"})
    else:
        logger.warning(f"File not found: {fact_filepath}. Skipping sales_fact.")

    # ── Post-Load Quality Checks ──
    run_post_load_checks(engine)

    # ── Refresh Materialized Views ──
    refresh_materialized_views(engine)

    # ── Summary ──
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"LOADING COMPLETE | Time: {elapsed:.2f}s")

    print("\n── Loading Summary ──")
    print(f"{'Table':<20} {'Rows':<10} {'Strategy':<15}")
    print("─" * 45)
    for item in load_summary:
        print(f"{item['table']:<20} {item['rows']:<10} {item['strategy']:<15}")
    print(f"\nTotal time: {elapsed:.2f} seconds")

    return load_summary


if __name__ == "__main__":
    run_loading()
