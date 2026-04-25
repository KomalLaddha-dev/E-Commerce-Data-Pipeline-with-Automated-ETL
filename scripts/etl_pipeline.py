"""
etl_pipeline.py — Pipeline Orchestrator
E-commerce Data Pipeline with Automated ETL

Coordinates the full ETL pipeline:
  1. Extract raw data from source OLTP
  2. Transform and build star schema tables
  3. Load into the data warehouse
"""

import sys
import os
import logging
from datetime import datetime

# Ensure scripts directory is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from extract import run_extraction
from transform import run_transformation
from load import run_loading

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_PATH = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_PATH, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_PATH, "pipeline.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """
    Execute the full ETL pipeline.

    Steps:
    1. Extract: Pull data from source OLTP → staging CSVs
    2. Transform: Clean, enrich, build star schema → processed CSVs
    3. Load: Push to data warehouse → refresh materialized views
    """
    pipeline_start = datetime.now()
    batch_id = pipeline_start.strftime("BATCH_%Y%m%d_%H%M%S")

    print("=" * 60)
    print("  E-COMMERCE ETL PIPELINE")
    print(f"  Batch: {batch_id}")
    print("=" * 60)
    logger.info(f"PIPELINE STARTED — Batch: {batch_id}")

    try:
        # ── Step 1: Extract ──
        print("\n[1/3] EXTRACTION PHASE")
        print("─" * 40)
        extract_results = run_extraction(incremental=False)

        extracted_tables = [r for r in extract_results if r["status"] == "success"]
        if not extracted_tables:
            raise RuntimeError("No tables were successfully extracted!")

        total_extracted = sum(r["rows"] for r in extracted_tables)
        print(f"\n✓ Extracted {total_extracted} total rows from {len(extracted_tables)} tables")

        # ── Step 2: Transform ──
        print("\n[2/3] TRANSFORMATION PHASE")
        print("─" * 40)
        transform_results = run_transformation()
        print(f"\n✓ Built {len(transform_results)} output tables")

        # ── Step 3: Load ──
        print("\n[3/3] LOADING PHASE")
        print("─" * 40)
        load_results = run_loading()

        total_loaded = sum(load_results.values())
        print(f"\n✓ Loaded {total_loaded} total rows into {len(load_results)} tables")

    except Exception as e:
        elapsed = (datetime.now() - pipeline_start).total_seconds()
        logger.error(f"PIPELINE FAILED after {elapsed:.2f}s: {e}")
        print(f"\n✗ PIPELINE FAILED: {e}")
        sys.exit(1)

    elapsed = (datetime.now() - pipeline_start).total_seconds()
    logger.info(f"PIPELINE COMPLETE — Time: {elapsed:.2f}s")

    print("\n" + "=" * 60)
    print(f"  PIPELINE COMPLETE — {elapsed:.2f} seconds")
    print("=" * 60)


if __name__ == "__main__":
    run_pipeline()
