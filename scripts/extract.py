"""
extract.py — Data Extraction Module
E-commerce Data Pipeline with Automated ETL

Connects to the source MySQL/PostgreSQL database and extracts
raw transactional data into CSV files in the staging area.
Supports both full and incremental extraction strategies.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
import os
import json
import yaml

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
DB_HOST = os.environ.get("SOURCE_DB_HOST", "localhost")
DB_PORT = os.environ.get("SOURCE_DB_PORT", "3306")
DB_USER = os.environ.get("SOURCE_DB_USER", "root")
DB_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD", "password")
DB_NAME = os.environ.get("SOURCE_DB_NAME", "ecommerce_db")
DB_DRIVER = os.environ.get("SOURCE_DB_DRIVER", "mysql+pymysql")

DB_CONNECTION = f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
LOG_PATH = os.path.join(BASE_DIR, "logs")
METADATA_PATH = os.path.join(BASE_DIR, "config", "etl_state.json")
PIPELINE_CONFIG_PATH = os.path.join(BASE_DIR, "config", "pipeline_config.yaml")

os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_PATH, "extract.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_TABLES_CONFIG = [
    {
        "table": "orders",
        "incremental_col": "order_date",
        "description": "Customer orders (header records)",
    },
    {
        "table": "order_items",
        "incremental_col": None,
        "description": "Order line items",
    },
    {
        "table": "customers",
        "incremental_col": "registration_date",
        "description": "Customer profiles",
    },
    {
        "table": "products",
        "incremental_col": "updated_at",
        "description": "Product catalog",
    },
    {
        "table": "payments",
        "incremental_col": "payment_date",
        "description": "Payment transactions",
    },
]


# ──────────────────────────────────────────────
# Metadata Functions (Track Last Run)
# ──────────────────────────────────────────────
def load_metadata():
    """Load ETL state metadata from JSON file."""
    if os.path.exists(METADATA_PATH):
        with open(METADATA_PATH, "r") as f:
            return json.load(f)
    return {}


def save_metadata(metadata):
    """Persist ETL state metadata to JSON file."""
    os.makedirs(os.path.dirname(METADATA_PATH), exist_ok=True)
    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2, default=str)


def load_extraction_config():
    """
    Load extraction settings from pipeline_config.yaml.

    Returns:
        tuple: (default_lookback_days, tables_config)
    """
    default_lookback_days = 1

    if not os.path.exists(PIPELINE_CONFIG_PATH):
        logger.warning("pipeline_config.yaml not found. Using default extraction settings.")
        return default_lookback_days, DEFAULT_TABLES_CONFIG

    try:
        with open(PIPELINE_CONFIG_PATH, "r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file) or {}

        extraction = config.get("extraction", {})
        default_lookback_days = int(extraction.get("default_lookback_days", 1))

        configured_tables = []
        for table in extraction.get("tables", []):
            table_name = table.get("name")
            if not table_name:
                continue
            configured_tables.append(
                {
                    "table": table_name,
                    "incremental_col": table.get("incremental_column"),
                    "description": table_name,
                }
            )

        if configured_tables:
            return default_lookback_days, configured_tables

        return default_lookback_days, DEFAULT_TABLES_CONFIG

    except Exception as err:
        logger.warning(f"Could not parse pipeline_config.yaml: {err}. Using defaults.")
        return default_lookback_days, DEFAULT_TABLES_CONFIG


# ──────────────────────────────────────────────
# Extraction Functions
# ──────────────────────────────────────────────
def get_db_engine():
    """Create and return a SQLAlchemy database engine."""
    try:
        engine = create_engine(DB_CONNECTION, echo=False, pool_pre_ping=True)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection established successfully.")
        return engine
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise


def extract_table(engine, table_name, incremental_col=None, last_run=None):
    """
    Extract data from a single source table.

    Parameters:
        engine:          SQLAlchemy engine connected to source database
        table_name:      Name of the table to extract
        incremental_col: Column to use for incremental extraction (timestamp column)
        last_run:        Datetime of the last successful extraction

    Returns:
        tuple: (pandas DataFrame containing extracted records, strategy_used)
    """
    try:
        if incremental_col and last_run:
            query = text(
                f"SELECT * FROM {table_name} "
                f"WHERE {incremental_col} >= :last_run "
                f"ORDER BY {incremental_col}"
            )
            df = pd.read_sql(query, engine, params={"last_run": last_run})
            logger.info(
                f"Incremental extract: {table_name} | "
                f"Column: {incremental_col} | Since: {last_run} | "
                f"Rows: {len(df)}"
            )

            # Automatic bootstrap fallback: if incremental returns nothing,
            # run a one-time full extract to avoid downstream file-not-found failures.
            if df.empty:
                full_query = f"SELECT * FROM {table_name}"
                full_df = pd.read_sql(full_query, engine)
                if not full_df.empty:
                    logger.warning(
                        f"Incremental extract empty for {table_name}. "
                        f"Falling back to full extract ({len(full_df)} rows)."
                    )
                    return full_df, "full_fallback"

            return df, "incremental"
        else:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, engine)
            logger.info(f"Full extract: {table_name} | Rows: {len(df)}")
            return df, "full"

    except Exception as e:
        logger.error(f"Extraction failed for {table_name}: {e}")
        raise


def save_raw_data(df, table_name):
    """
    Save extracted data to CSV in the raw staging area.

    Files are named with timestamps to maintain extraction history:
        orders_20251201_103000.csv
    """
    if df.empty:
        logger.warning(f"No data to save for {table_name}. Skipping.")
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{table_name}_{timestamp}.csv"
    filepath = os.path.join(RAW_DATA_PATH, filename)

    df.to_csv(filepath, index=False, encoding="utf-8")
    logger.info(f"Saved: {filepath} | Rows: {len(df)} | Size: {os.path.getsize(filepath)} bytes")
    return filepath


# ──────────────────────────────────────────────
# Main Extraction Pipeline
# ──────────────────────────────────────────────

def run_extraction():
    """
    Execute the full extraction pipeline.

    Connects to the source database, extracts all configured tables
    using the appropriate strategy (full or incremental), and saves
    the raw data to the staging area.

    Returns:
        list of file paths for all extracted files
    """
    logger.info("=" * 60)
    logger.info("EXTRACTION PIPELINE STARTED")
    logger.info("=" * 60)

    start_time = datetime.now()
    engine = get_db_engine()
    metadata = load_metadata()

    default_lookback_days, tables_config = load_extraction_config()
    default_last_run = (datetime.now() - timedelta(days=default_lookback_days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    extracted_files = []
    extraction_summary = []

    for config in tables_config:
        table_name = config["table"]
        incr_col = config["incremental_col"]

        # Get last run timestamp from metadata
        last_run = metadata.get(table_name, {}).get("last_run", default_last_run)
        if isinstance(last_run, str):
            try:
                last_run = datetime.strptime(last_run, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                last_run = pd.to_datetime(last_run, errors="coerce")
                if pd.isna(last_run):
                    last_run = datetime.strptime(default_last_run, "%Y-%m-%d %H:%M:%S")

        if isinstance(last_run, pd.Timestamp):
            last_run = last_run.to_pydatetime()

        # Extract data
        df, strategy_used = extract_table(
            engine,
            table_name,
            incr_col,
            last_run if incr_col else None,
        )

        # Save to staging
        filepath = save_raw_data(df, table_name)
        if filepath:
            extracted_files.append(filepath)

        # Update metadata
        metadata[table_name] = {
            "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "rows_extracted": len(df),
            "file": filepath,
        }

        extraction_summary.append({
            "table": table_name,
            "rows": len(df),
            "strategy": strategy_used,
        })

    # Save metadata for next run
    save_metadata(metadata)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"EXTRACTION COMPLETE | Files: {len(extracted_files)} | Time: {elapsed:.2f}s")

    # Print summary
    print("\n── Extraction Summary ──")
    print(f"{'Table':<20} {'Rows':<10} {'Strategy':<15}")
    print("─" * 45)
    for item in extraction_summary:
        print(f"{item['table']:<20} {item['rows']:<10} {item['strategy']:<15}")
    print(f"\nTotal time: {elapsed:.2f} seconds")

    return extracted_files


if __name__ == "__main__":
    run_extraction()
