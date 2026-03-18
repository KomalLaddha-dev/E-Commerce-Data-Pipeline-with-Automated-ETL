"""
etl_pipeline.py — End-to-End ETL Pipeline Runner
E-commerce Data Pipeline with Automated ETL

Orchestrates the full Extract → Transform → Load pipeline.
Can be run standalone or called by Apache Airflow DAG.
"""

import logging
import os
import sys
from datetime import datetime

# Add project root to path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from scripts.extract import run_extraction
from scripts.transform import run_transformation
from scripts.load import run_loading

LOG_PATH = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_PATH, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_PATH, "pipeline.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """
    Execute the complete ETL pipeline.

    Steps:
        1. Extract data from source database
        2. Transform raw data into analytics-ready format
        3. Load transformed data into data warehouse

    Returns:
        dict with pipeline summary including timings and row counts
    """
    pipeline_start = datetime.now()
    batch_id = pipeline_start.strftime("BATCH_%Y%m%d_%H%M%S")

    print("=" * 60)
    print(f"  E-COMMERCE ETL PIPELINE")
    print(f"  Batch ID: {batch_id}")
    print(f"  Started:  {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    logger.info(f"Pipeline started | Batch: {batch_id}")
    results = {"batch_id": batch_id, "steps": {}}

    try:
        # ── STEP 1: EXTRACT ──
        print("\n[1/3] EXTRACTING data from source database...")
        step_start = datetime.now()
        extracted_files = run_extraction()
        step_elapsed = (datetime.now() - step_start).total_seconds()
        results["steps"]["extract"] = {
            "status": "success",
            "files": len(extracted_files),
            "duration_sec": step_elapsed,
        }
        logger.info(f"Extract complete | Files: {len(extracted_files)} | Time: {step_elapsed:.2f}s")

        # ── STEP 2: TRANSFORM ──
        print("\n[2/3] TRANSFORMING raw data...")
        step_start = datetime.now()
        transform_results = run_transformation()
        step_elapsed = (datetime.now() - step_start).total_seconds()
        results["steps"]["transform"] = {
            "status": "success",
            "tables": transform_results,
            "duration_sec": step_elapsed,
        }
        logger.info(f"Transform complete | Tables: {transform_results} | Time: {step_elapsed:.2f}s")

        # ── STEP 3: LOAD ──
        print("\n[3/3] LOADING data into warehouse...")
        step_start = datetime.now()
        load_results = run_loading()
        step_elapsed = (datetime.now() - step_start).total_seconds()
        results["steps"]["load"] = {
            "status": "success",
            "tables_loaded": len(load_results),
            "duration_sec": step_elapsed,
        }
        logger.info(f"Load complete | Tables: {len(load_results)} | Time: {step_elapsed:.2f}s")

        # ── SUMMARY ──
        total_elapsed = (datetime.now() - pipeline_start).total_seconds()
        results["total_duration_sec"] = total_elapsed
        results["status"] = "success"

        print("\n" + "=" * 60)
        print(f"  PIPELINE COMPLETE")
        print(f"  Status:   SUCCESS")
        print(f"  Duration: {total_elapsed:.2f} seconds")
        print("=" * 60)

        logger.info(f"Pipeline SUCCESS | Batch: {batch_id} | Duration: {total_elapsed:.2f}s")

    except Exception as e:
        total_elapsed = (datetime.now() - pipeline_start).total_seconds()
        results["status"] = "failed"
        results["error"] = str(e)

        print("\n" + "=" * 60)
        print(f"  PIPELINE FAILED")
        print(f"  Error:    {e}")
        print(f"  Duration: {total_elapsed:.2f} seconds")
        print("=" * 60)

        logger.error(f"Pipeline FAILED | Batch: {batch_id} | Error: {e}")
        raise

    return results


if __name__ == "__main__":
    run_etl_pipeline()
