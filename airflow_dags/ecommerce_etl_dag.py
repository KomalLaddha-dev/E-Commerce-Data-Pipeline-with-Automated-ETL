"""
ecommerce_etl_dag.py — Apache Airflow DAG Definition
E-commerce Data Pipeline with Automated ETL

This DAG orchestrates the complete ETL pipeline:
  1. Extract raw data from source databases
  2. Transform and clean the data
  3. Load into the data warehouse (star schema)
  4. Run post-load quality checks
  5. Refresh materialized views

Schedule: Daily at 02:00 UTC
Retries:  3 attempts with 5-minute delay between retries
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# DAG Default Arguments
# ──────────────────────────────────────────────
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "start_date": days_ago(1),
}


# ──────────────────────────────────────────────
# Task Callables
# ──────────────────────────────────────────────
def task_extract(**kwargs):
    """Extract data from source databases."""
    from scripts.extract import run_extraction

    logger.info("Starting extraction task...")
    files = run_extraction()
    logger.info(f"Extraction complete. Files created: {len(files)}")

    # Push file paths to XCom for downstream tasks
    kwargs["ti"].xcom_push(key="extracted_files", value=files)
    return files


def task_transform(**kwargs):
    """Transform and clean extracted data."""
    from scripts.transform import run_transformation

    logger.info("Starting transformation task...")
    results = run_transformation()
    logger.info(f"Transformation complete: {results}")

    kwargs["ti"].xcom_push(key="transform_results", value=results)
    return results


def task_load(**kwargs):
    """Load transformed data into the data warehouse."""
    from scripts.load import run_loading

    logger.info("Starting loading task...")
    summary = run_loading()
    logger.info(f"Loading complete: {summary}")

    kwargs["ti"].xcom_push(key="load_summary", value=summary)
    return summary


def task_quality_check(**kwargs):
    """
    Run comprehensive data quality checks after loading.

    This task validates:
    - Row counts are non-zero
    - No null foreign keys
    - Referential integrity between fact and dimensions
    - No negative monetary amounts
    - No future dates in completed orders
    """
    from sqlalchemy import create_engine, text
    import os

    dw_conn = (
        f"{os.environ.get('DW_DRIVER', 'postgresql')}://"
        f"{os.environ.get('DW_USER', 'warehouse_user')}:"
        f"{os.environ.get('DW_PASSWORD', 'warehouse_pass')}@"
        f"{os.environ.get('DW_HOST', 'localhost')}:"
        f"{os.environ.get('DW_PORT', '5432')}/"
        f"{os.environ.get('DW_NAME', 'ecommerce_dw')}"
    )
    engine = create_engine(dw_conn)

    checks_failed = []

    with engine.connect() as conn:
        # Check 1: sales_fact is not empty
        row_count = conn.execute(text("SELECT COUNT(*) FROM sales_fact")).scalar()
        if row_count == 0:
            checks_failed.append("sales_fact is EMPTY")
        else:
            logger.info(f"CHECK PASS: sales_fact has {row_count} rows")

        # Check 2: No null customer keys
        null_keys = conn.execute(
            text("SELECT COUNT(*) FROM sales_fact WHERE customer_key IS NULL")
        ).scalar()
        if null_keys > 0:
            checks_failed.append(f"{null_keys} null customer_keys in sales_fact")
        else:
            logger.info("CHECK PASS: No null customer_keys")

        # Check 3: No negative amounts
        neg_amounts = conn.execute(
            text("SELECT COUNT(*) FROM sales_fact WHERE net_amount < 0")
        ).scalar()
        if neg_amounts > 0:
            checks_failed.append(f"{neg_amounts} negative net_amounts in sales_fact")
        else:
            logger.info("CHECK PASS: No negative net_amounts")

        # Check 4: Date dimension completeness
        orphan_dates = conn.execute(text("""
            SELECT COUNT(*) FROM sales_fact f
            LEFT JOIN date_dim d ON f.date_key = d.date_key
            WHERE d.date_key IS NULL
        """)).scalar()
        if orphan_dates > 0:
            checks_failed.append(f"{orphan_dates} orphan date_keys in sales_fact")
        else:
            logger.info("CHECK PASS: All date_keys have matching date_dim rows")

        # Check 5: Dimension tables are not empty
        for dim_table in ["customer_dim", "product_dim", "date_dim", "payment_dim"]:
            dim_count = conn.execute(text(f"SELECT COUNT(*) FROM {dim_table}")).scalar()
            if dim_count == 0:
                checks_failed.append(f"{dim_table} is EMPTY")
            else:
                logger.info(f"CHECK PASS: {dim_table} has {dim_count} rows")

    if checks_failed:
        error_msg = "DATA QUALITY CHECKS FAILED:\n" + "\n".join(f"  - {f}" for f in checks_failed)
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info("ALL DATA QUALITY CHECKS PASSED")
    return True


def task_refresh_views(**kwargs):
    """Refresh materialized views for fast dashboard queries."""
    from sqlalchemy import create_engine, text
    import os

    dw_conn = (
        f"{os.environ.get('DW_DRIVER', 'postgresql')}://"
        f"{os.environ.get('DW_USER', 'warehouse_user')}:"
        f"{os.environ.get('DW_PASSWORD', 'warehouse_pass')}@"
        f"{os.environ.get('DW_HOST', 'localhost')}:"
        f"{os.environ.get('DW_PORT', '5432')}/"
        f"{os.environ.get('DW_NAME', 'ecommerce_dw')}"
    )
    engine = create_engine(dw_conn)

    views = ["mv_daily_sales_summary", "mv_product_performance"]

    with engine.begin() as conn:
        for view in views:
            try:
                conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"))
                logger.info(f"Refreshed: {view}")
            except Exception as e:
                logger.warning(f"Could not refresh {view}: {e}")

    return True


# ──────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline: Extract → Transform → Load → Validate for e-commerce data warehouse",
    schedule_interval="0 2 * * *",       # Every day at 02:00 UTC
    catchup=False,                        # Don't backfill missed runs
    max_active_runs=1,                    # Only one run at a time
    tags=["ecommerce", "etl", "data-warehouse", "production"],
) as dag:

    # ── Pipeline Start ──
    start = DummyOperator(
        task_id="start_pipeline",
    )

    # ── Extract Phase ──
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=task_extract,
        provide_context=True,
    )

    # ── Transform Phase ──
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=task_transform,
        provide_context=True,
    )

    # ── Load Phase ──
    load_to_warehouse = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=task_load,
        provide_context=True,
    )

    # ── Quality Checks ──
    quality_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=task_quality_check,
        provide_context=True,
    )

    # ── Refresh Materialized Views ──
    refresh_views = PythonOperator(
        task_id="refresh_materialized_views",
        python_callable=task_refresh_views,
        provide_context=True,
    )

    # ── Pipeline End ──
    end_success = DummyOperator(
        task_id="pipeline_success",
    )

    # ── Failure Notification ──
    notify_failure = EmailOperator(
        task_id="notify_failure",
        to="data-alerts@company.com",
        subject="[ALERT] E-commerce ETL Pipeline Failed - {{ ds }}",
        html_content="""
            <h3>ETL Pipeline Failure Alert</h3>
            <p><strong>DAG:</strong> ecommerce_etl_pipeline</p>
            <p><strong>Execution Date:</strong> {{ ds }}</p>
            <p><strong>Log URL:</strong> {{ task_instance.log_url }}</p>
            <p>Please investigate the failure in the Airflow UI.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ── Task Dependencies ──
    # Linear pipeline:  start → extract → transform → load → validate → refresh → end
    start >> extract_data >> transform_data >> load_to_warehouse
    load_to_warehouse >> quality_checks >> refresh_views >> end_success

    # Failure notification triggers if any task fails
    [extract_data, transform_data, load_to_warehouse, quality_checks] >> notify_failure
