"""
transform.py — Data Transformation Module
E-commerce Data Pipeline with Automated ETL

Reads raw extracted data from the staging area, applies cleaning,
normalization, deduplication, enrichment, and aggregation logic,
and produces analytics-ready datasets for the data warehouse.
"""

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, "data", "processed")
LOG_PATH = os.path.join(BASE_DIR, "logs")

os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_PATH, "transform.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Utility Functions
# ──────────────────────────────────────────────
def find_latest_file(table_name, directory=None):
    """
    Find the most recently created file for a given table in the staging area.

    Files are named like: orders_20251201_103000.csv
    Sorting by name descending gives us the latest file.
    """
    directory = directory or RAW_DATA_PATH
    files = [f for f in os.listdir(directory) if f.startswith(table_name + "_") and f.endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"No raw files found for '{table_name}' in {directory}")
    files.sort(reverse=True)
    return os.path.join(directory, files[0])


def log_transform_stats(table_name, before_count, after_count):
    """Log transformation statistics for a table."""
    removed = before_count - after_count
    pct = (removed / before_count * 100) if before_count > 0 else 0
    logger.info(
        f"{table_name}: {before_count} → {after_count} rows "
        f"(removed {removed}, {pct:.1f}%)"
    )


# ──────────────────────────────────────────────
# Cleaning Functions
# ──────────────────────────────────────────────
def clean_orders(df):
    """
    Clean and validate order records.

    Transformations:
    - Remove duplicate order_ids (keep latest)
    - Standardize status values to lowercase
    - Parse order_date to datetime
    - Remove records with null dates or negative amounts
    - Fill missing discount/shipping with 0
    - Calculate net_amount
    """
    initial_count = len(df)

    # 1. Remove duplicate orders (keep most recent entry)
    df = df.drop_duplicates(subset=["order_id"], keep="last").copy()

    # 2. Standardize status values
    status_mapping = {
        "Pending": "pending", "PENDING": "pending",
        "Confirmed": "confirmed", "CONFIRMED": "confirmed",
        "Shipped": "shipped", "SHIPPED": "shipped",
        "Delivered": "delivered", "DELIVERED": "delivered", "Delvrd": "delivered",
        "Cancelled": "cancelled", "CANCELLED": "cancelled", "Canceled": "cancelled",
        "Returned": "returned", "RETURNED": "returned",
    }
    df["status"] = df["status"].map(status_mapping).fillna(df["status"].str.lower())

    # 3. Parse dates
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # 4. Remove invalid records
    df = df.dropna(subset=["order_date", "customer_id"]).copy()
    df = df[df["total_amount"] >= 0].copy()

    # 5. Fill missing financial columns
    df["discount_amount"] = pd.to_numeric(df["discount_amount"], errors="coerce").fillna(0)
    df["shipping_cost"] = pd.to_numeric(df["shipping_cost"], errors="coerce").fillna(0)

    # 6. Calculate net amount
    df["net_amount"] = df["total_amount"] - df["discount_amount"] + df["shipping_cost"]

    # 7. Ensure correct data types
    df["customer_id"] = df["customer_id"].astype(int)
    df["order_id"] = df["order_id"].astype(int)

    log_transform_stats("orders", initial_count, len(df))
    return df


def clean_customers(df):
    """
    Clean and standardize customer data.

    Transformations:
    - Deduplicate by email (keep most recent registration)
    - Standardize names to Title Case
    - Normalize email to lowercase
    - Standardize country names
    - Fill missing phone numbers
    - Assign customer segments
    """
    initial_count = len(df)

    # 1. Deduplicate by email (keep most recently registered)
    df = df.copy()
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    df = df.sort_values("registration_date", ascending=False)
    df = df.drop_duplicates(subset=["email"], keep="first").copy()

    # 2. Standardize names
    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"] = df["last_name"].str.strip().str.title()

    # 3. Normalize email
    df["email"] = df["email"].str.strip().str.lower()

    # 4. Standardize country names
    country_mapping = {
        "IN": "India", "in": "India", "india": "India",
        "US": "United States", "us": "United States", "usa": "United States",
        "UK": "United Kingdom", "uk": "United Kingdom",
    }
    df["country"] = df["country"].replace(country_mapping).fillna("Unknown")

    # 5. Fill missing fields
    df["phone"] = df["phone"].fillna("N/A")
    df["city"] = df["city"].fillna("Unknown")
    df["state"] = df["state"].fillna("Unknown")

    # 6. Assign default segment (will be enriched later)
    if "segment" not in df.columns:
        df["segment"] = "Regular"

    log_transform_stats("customers", initial_count, len(df))
    return df


def clean_products(df):
    """
    Clean and validate product data.

    Transformations:
    - Deduplicate by product_id (keep latest)
    - Standardize category/brand to Title Case
    - Remove products with zero/negative price
    - Calculate profit margin
    """
    initial_count = len(df)

    # 1. Deduplicate
    df = df.drop_duplicates(subset=["product_id"], keep="last").copy()

    # 2. Standardize text fields
    for col in ["category", "sub_category", "brand"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    df["product_name"] = df["product_name"].str.strip()

    # 3. Validate prices
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce")
    df = df[df["price"] > 0].copy()
    df["cost_price"] = df["cost_price"].fillna(df["price"] * 0.6)  # Default 60% cost

    # 4. Calculate profit margin
    df["profit_margin"] = ((df["price"] - df["cost_price"]) / df["price"] * 100).round(2)

    # 5. Fill missing categories
    df["category"] = df["category"].fillna("Uncategorized")
    df["sub_category"] = df["sub_category"].fillna("General")
    df["brand"] = df["brand"].fillna("Unbranded")

    log_transform_stats("products", initial_count, len(df))
    return df


def clean_payments(df):
    """
    Clean and validate payment data.

    Transformations:
    - Deduplicate by payment_id
    - Standardize payment methods and status to lowercase
    - Parse payment_date
    - Filter to completed payments for fact table
    """
    initial_count = len(df)

    # 1. Deduplicate
    df = df.drop_duplicates(subset=["payment_id"], keep="last").copy()

    # 2. Standardize text fields
    df["payment_method"] = df["payment_method"].str.strip().str.lower()
    df["payment_status"] = df["payment_status"].str.strip().str.lower()

    # 3. Parse dates
    df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce")

    # 4. Validate amounts
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["amount", "payment_date"]).copy()

    log_transform_stats("payments", initial_count, len(df))
    return df


# ──────────────────────────────────────────────
# Aggregation & Fact Table Building
# ──────────────────────────────────────────────
def build_sales_fact(orders_df, items_df, payments_df):
    """
    Build the Sales Fact table by joining orders with aggregated
    order items and payment information.

    Grain: One row per order.
    """
    # Aggregate order items to order level
    items_agg = items_df.groupby("order_id").agg(
        total_items=("quantity", "sum"),
        total_item_discount=("discount", "sum"),
    ).reset_index()

    # Merge orders with aggregated items
    fact = orders_df.merge(items_agg, on="order_id", how="left")

    # Fill missing item counts
    fact["total_items"] = fact["total_items"].fillna(1).astype(int)

    # Get payment info per order (most recent payment)
    payment_info = (
        payments_df
        .sort_values("payment_date", ascending=False)
        .drop_duplicates(subset=["order_id"], keep="first")
        [["order_id", "payment_method", "payment_status", "payment_id"]]
    )
    fact = fact.merge(payment_info, on="order_id", how="left")

    # Generate date key (YYYYMMDD integer)
    fact["date_key"] = fact["order_date"].dt.strftime("%Y%m%d").astype(int)

    # Select and rename columns for the fact table
    fact = fact[[
        "order_id", "customer_id", "date_key", "status",
        "total_amount", "discount_amount", "shipping_cost", "net_amount",
        "total_items", "payment_method", "payment_id",
    ]].rename(columns={"status": "order_status"})

    logger.info(f"Sales Fact built: {len(fact)} rows")
    return fact


def build_date_dimension(start_date="2024-01-01", end_date="2026-12-31"):
    """
    Generate a complete Date Dimension table.

    Creates one row per calendar day with useful attributes
    for time-based analysis (day of week, quarter, fiscal year, etc.).
    """
    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    date_dim = pd.DataFrame({
        "date_key": dates.strftime("%Y%m%d").astype(int),
        "full_date": dates.date,
        "day": dates.day,
        "month": dates.month,
        "year": dates.year,
        "quarter": dates.quarter,
        "day_of_week": dates.dayofweek,        # 0=Monday, 6=Sunday
        "day_name": dates.strftime("%A"),       # Monday, Tuesday, ...
        "month_name": dates.strftime("%B"),     # January, February, ...
        "is_weekend": dates.dayofweek.isin([5, 6]),
        "fiscal_year": dates.year,
        "week_of_year": dates.isocalendar().week.astype(int),
    })

    logger.info(f"Date Dimension built: {len(date_dim)} rows ({start_date} to {end_date})")
    return date_dim


def enrich_customer_segments(customers_df, orders_df):
    """
    Enrich customer data with segments based on order history.

    Segments:
    - Premium:  Total spending > 10,000 or > 5 orders
    - Regular:  Total spending > 2,000 or > 1 order
    - New:      Single order or no order history
    """
    # Calculate customer spending
    customer_stats = orders_df.groupby("customer_id").agg(
        total_orders=("order_id", "nunique"),
        total_spent=("net_amount", "sum"),
    ).reset_index()

    # Merge with customer data
    customers_enriched = customers_df.merge(customer_stats, on="customer_id", how="left")
    customers_enriched["total_orders"] = customers_enriched["total_orders"].fillna(0)
    customers_enriched["total_spent"] = customers_enriched["total_spent"].fillna(0)

    # Assign segments
    conditions = [
        (customers_enriched["total_spent"] > 10000) | (customers_enriched["total_orders"] > 5),
        (customers_enriched["total_spent"] > 2000) | (customers_enriched["total_orders"] > 1),
    ]
    choices = ["Premium", "Regular"]
    customers_enriched["segment"] = np.select(conditions, choices, default="New")

    # Drop helper columns
    customers_enriched = customers_enriched.drop(columns=["total_orders", "total_spent"])

    logger.info(f"Customer segments assigned: {customers_enriched['segment'].value_counts().to_dict()}")
    return customers_enriched


# ──────────────────────────────────────────────
# Main Transform Pipeline
# ──────────────────────────────────────────────
def run_transformation():
    """
    Execute the full transformation pipeline.

    Steps:
    1. Load raw CSV files from staging area
    2. Clean and validate each dataset
    3. Enrich customer segments
    4. Build fact and dimension tables
    5. Save processed data to output directory

    Returns:
        dict with row counts for each output table
    """
    logger.info("=" * 60)
    logger.info("TRANSFORMATION PIPELINE STARTED")
    logger.info("=" * 60)

    start_time = datetime.now()

    # ── Step 1: Load raw data ──
    logger.info("Loading raw data from staging area...")
    orders_df = pd.read_csv(find_latest_file("orders"))
    items_df = pd.read_csv(find_latest_file("order_items"))
    customers_df = pd.read_csv(find_latest_file("customers"))
    products_df = pd.read_csv(find_latest_file("products"))
    payments_df = pd.read_csv(find_latest_file("payments"))

    logger.info(
        f"Raw data loaded: orders={len(orders_df)}, items={len(items_df)}, "
        f"customers={len(customers_df)}, products={len(products_df)}, "
        f"payments={len(payments_df)}"
    )

    # ── Step 2: Clean each dataset ──
    logger.info("Cleaning datasets...")
    orders_clean = clean_orders(orders_df)
    customers_clean = clean_customers(customers_df)
    products_clean = clean_products(products_df)
    payments_clean = clean_payments(payments_df)

    # ── Step 3: Enrich customer segments ──
    logger.info("Enriching customer segments...")
    customers_enriched = enrich_customer_segments(customers_clean, orders_clean)

    # ── Step 4: Build warehouse tables ──
    logger.info("Building fact and dimension tables...")
    sales_fact = build_sales_fact(orders_clean, items_df, payments_clean)
    date_dim = build_date_dimension()

    # ── Step 5: Save processed data ──
    logger.info("Saving processed data...")
    output_files = {
        "sales_fact.csv": sales_fact,
        "customer_dim.csv": customers_enriched,
        "product_dim.csv": products_clean,
        "date_dim.csv": date_dim,
        "payment_dim.csv": payments_clean,
    }

    for filename, df in output_files.items():
        filepath = os.path.join(PROCESSED_DATA_PATH, filename)
        df.to_csv(filepath, index=False, encoding="utf-8")
        logger.info(f"Saved: {filepath} | Rows: {len(df)}")

    # ── Summary ──
    elapsed = (datetime.now() - start_time).total_seconds()
    results = {name.replace(".csv", ""): len(df) for name, df in output_files.items()}

    logger.info(f"TRANSFORMATION COMPLETE | Time: {elapsed:.2f}s")

    print("\n-- Transformation Summary --")
    print(f"{'Output Table':<20} {'Rows':<10}")
    print("-" * 30)
    for table, count in results.items():
        print(f"{table:<20} {count:<10}")
    print(f"\nTotal time: {elapsed:.2f} seconds")

    return results


if __name__ == "__main__":
    results = run_transformation()
