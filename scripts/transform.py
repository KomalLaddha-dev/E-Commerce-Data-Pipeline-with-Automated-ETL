"""
transform.py — Data Transformation Module
E-commerce Data Pipeline with Automated ETL

Reads raw extracted data, applies cleaning, normalization,
enrichment, and builds star schema tables:
  - dim_date, dim_customer, dim_product, dim_payment_method, dim_location
  - fact_order_items (line-item grain)
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

# Indian geographic mapping for dim_location
CITY_GEO_MAP = {
    "Mumbai": ("Maharashtra", "West", "Tier 1"),
    "Delhi": ("Delhi", "North", "Tier 1"),
    "Bengaluru": ("Karnataka", "South", "Tier 1"),
    "Hyderabad": ("Telangana", "South", "Tier 1"),
    "Chennai": ("Tamil Nadu", "South", "Tier 1"),
    "Kolkata": ("West Bengal", "East", "Tier 1"),
    "Pune": ("Maharashtra", "West", "Tier 1"),
    "Ahmedabad": ("Gujarat", "West", "Tier 1"),
    "Jaipur": ("Rajasthan", "North", "Tier 1"),
    "Lucknow": ("Uttar Pradesh", "North", "Tier 2"),
    "Chandigarh": ("Punjab", "North", "Tier 2"),
    "Kochi": ("Kerala", "South", "Tier 2"),
    "Indore": ("Madhya Pradesh", "Central", "Tier 2"),
    "Nagpur": ("Maharashtra", "West", "Tier 2"),
    "Bhopal": ("Madhya Pradesh", "Central", "Tier 2"),
    "Vadodara": ("Gujarat", "West", "Tier 2"),
    "Coimbatore": ("Tamil Nadu", "South", "Tier 2"),
    "Visakhapatnam": ("Andhra Pradesh", "South", "Tier 2"),
    "Patna": ("Bihar", "East", "Tier 2"),
    "Thiruvananthapuram": ("Kerala", "South", "Tier 2"),
    "Surat": ("Gujarat", "West", "Tier 2"),
    "Guwahati": ("Assam", "East", "Tier 2"),
    "Bhubaneswar": ("Odisha", "East", "Tier 2"),
    "Dehradun": ("Uttarakhand", "North", "Tier 2"),
    "Ranchi": ("Jharkhand", "East", "Tier 2"),
    "Mysuru": ("Karnataka", "South", "Tier 2"),
    "Noida": ("Uttar Pradesh", "North", "Tier 2"),
    "Gurgaon": ("Haryana", "North", "Tier 2"),
    "Faridabad": ("Haryana", "North", "Tier 3"),
    "Agra": ("Uttar Pradesh", "North", "Tier 3"),
    "Varanasi": ("Uttar Pradesh", "North", "Tier 3"),
    "Jodhpur": ("Rajasthan", "North", "Tier 3"),
    "Madurai": ("Tamil Nadu", "South", "Tier 3"),
    "Nashik": ("Maharashtra", "West", "Tier 3"),
    "Aurangabad": ("Maharashtra", "West", "Tier 3"),
    "Rajkot": ("Gujarat", "West", "Tier 3"),
    "Vijayawada": ("Andhra Pradesh", "South", "Tier 3"),
    "Raipur": ("Chhattisgarh", "Central", "Tier 3"),
    "Amritsar": ("Punjab", "North", "Tier 3"),
    "Udaipur": ("Rajasthan", "North", "Tier 3"),
}


# ──────────────────────────────────────────────
# Utility Functions
# ──────────────────────────────────────────────
def find_latest_file(table_name, directory=None):
    """Find the most recently created file for a given table."""
    directory = directory or RAW_DATA_PATH
    files = [f for f in os.listdir(directory) if f.startswith(table_name + "_") and f.endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"No raw files found for '{table_name}' in {directory}")
    files.sort(reverse=True)
    return os.path.join(directory, files[0])


def log_transform_stats(table_name, before_count, after_count):
    """Log transformation statistics."""
    removed = before_count - after_count
    pct = (removed / before_count * 100) if before_count > 0 else 0
    logger.info(f"{table_name}: {before_count} → {after_count} rows (removed {removed}, {pct:.1f}%)")


# ──────────────────────────────────────────────
# Cleaning Functions
# ──────────────────────────────────────────────
def clean_orders(df):
    """Clean and validate order records."""
    initial_count = len(df)
    df = df.drop_duplicates(subset=["order_id"], keep="last").copy()

    status_mapping = {
        "Pending": "pending", "PENDING": "pending",
        "Confirmed": "confirmed", "CONFIRMED": "confirmed",
        "Shipped": "shipped", "SHIPPED": "shipped",
        "Delivered": "delivered", "DELIVERED": "delivered", "Delvrd": "delivered",
        "Cancelled": "cancelled", "CANCELLED": "cancelled", "Canceled": "cancelled",
        "Returned": "returned", "RETURNED": "returned",
    }
    df["status"] = df["status"].map(status_mapping).fillna(df["status"].str.lower())
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df.dropna(subset=["order_date", "customer_id"]).copy()
    df = df[df["total_amount"] >= 0].copy()
    df["discount_amount"] = pd.to_numeric(df["discount_amount"], errors="coerce").fillna(0)
    df["shipping_cost"] = pd.to_numeric(df["shipping_cost"], errors="coerce").fillna(0)
    df["net_amount"] = df["total_amount"] - df["discount_amount"] + df["shipping_cost"]
    df["customer_id"] = df["customer_id"].astype(int)
    df["order_id"] = df["order_id"].astype(int)
    df["date_key"] = df["order_date"].dt.strftime("%Y%m%d").astype(int)

    log_transform_stats("orders", initial_count, len(df))
    return df


def clean_customers(df):
    """Clean and standardize customer data."""
    initial_count = len(df)
    df = df.copy()
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    df = df.sort_values("registration_date", ascending=False)
    df = df.drop_duplicates(subset=["email"], keep="first").copy()

    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"] = df["last_name"].str.strip().str.title()
    df["email"] = df["email"].str.strip().str.lower()

    country_mapping = {
        "IN": "India", "in": "India", "india": "India",
        "US": "United States", "us": "United States", "usa": "United States",
    }
    df["country"] = df["country"].replace(country_mapping).fillna("India")
    df["phone"] = df["phone"].fillna("N/A")
    df["city"] = df["city"].fillna("Unknown")
    df["state"] = df["state"].fillna("Unknown")

    if "segment" not in df.columns:
        df["segment"] = "New"

    log_transform_stats("customers", initial_count, len(df))
    return df


def clean_products(df):
    """Clean and validate product data."""
    initial_count = len(df)
    df = df.drop_duplicates(subset=["product_id"], keep="last").copy()

    for col in ["category", "sub_category", "brand"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()
    df["product_name"] = df["product_name"].str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce")
    df = df[df["price"] > 0].copy()
    df["cost_price"] = df["cost_price"].fillna(df["price"] * 0.6)
    df["profit_margin"] = ((df["price"] - df["cost_price"]) / df["price"] * 100).round(2)
    df["category"] = df["category"].fillna("Uncategorized")
    df["sub_category"] = df["sub_category"].fillna("General")
    df["brand"] = df["brand"].fillna("Unbranded")

    log_transform_stats("products", initial_count, len(df))
    return df


def clean_order_items(df):
    """Clean order items."""
    initial_count = len(df)
    df = df.drop_duplicates(subset=["item_id"], keep="last").copy()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype(int)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce").fillna(0)
    df = df.dropna(subset=["unit_price", "order_id", "product_id"]).copy()
    df["line_total"] = (df["quantity"] * df["unit_price"] - df["discount"]).round(2)

    log_transform_stats("order_items", initial_count, len(df))
    return df


def clean_payments(df):
    """Clean and validate payment data."""
    initial_count = len(df)
    df = df.drop_duplicates(subset=["payment_id"], keep="last").copy()
    df["payment_method"] = df["payment_method"].str.strip().str.lower()
    df["payment_status"] = df["payment_status"].str.strip().str.lower()
    df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["amount", "payment_date"]).copy()

    log_transform_stats("payments", initial_count, len(df))
    return df


# ──────────────────────────────────────────────
# Dimension Builders
# ──────────────────────────────────────────────
def build_date_dimension(start_date="2024-01-01", end_date="2027-12-31"):
    """Generate Date Dimension table."""
    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    date_dim = pd.DataFrame({
        "date_key": dates.strftime("%Y%m%d").astype(int),
        "full_date": dates.date,
        "day": dates.day,
        "month": dates.month,
        "year": dates.year,
        "quarter": dates.quarter,
        "day_of_week": dates.dayofweek,
        "day_name": dates.strftime("%A"),
        "month_name": dates.strftime("%B"),
        "is_weekend": dates.dayofweek.isin([5, 6]),
        "fiscal_year": dates.year,
        "week_of_year": dates.isocalendar().week.astype(int),
    })

    logger.info(f"dim_date built: {len(date_dim)} rows ({start_date} to {end_date})")
    return date_dim


def build_location_dimension(customers_df):
    """Build Location Dimension from customer cities."""
    cities = customers_df[["city", "state"]].drop_duplicates().copy()
    cities = cities.dropna(subset=["city", "state"])

    # Enrich with region and tier from our mapping
    def get_geo_info(row):
        info = CITY_GEO_MAP.get(row["city"], (row["state"], "Other", "Tier 3"))
        return pd.Series({"region": info[1], "tier": info[2]})

    geo = cities.apply(get_geo_info, axis=1)
    cities = pd.concat([cities, geo], axis=1)
    cities["country"] = "India"
    cities = cities.reset_index(drop=True)
    cities.index = cities.index + 1
    cities.index.name = "location_key"
    cities = cities.reset_index()

    logger.info(f"dim_location built: {len(cities)} locations")
    return cities


def build_payment_method_dimension():
    """Build the conformed Payment Method Dimension (~6 rows)."""
    methods = pd.DataFrame([
        {"payment_method_key": 1, "payment_method": "credit_card", "display_name": "Credit Card", "method_type": "card"},
        {"payment_method_key": 2, "payment_method": "debit_card", "display_name": "Debit Card", "method_type": "card"},
        {"payment_method_key": 3, "payment_method": "upi", "display_name": "UPI", "method_type": "digital"},
        {"payment_method_key": 4, "payment_method": "net_banking", "display_name": "Net Banking", "method_type": "digital"},
        {"payment_method_key": 5, "payment_method": "wallet", "display_name": "Wallet", "method_type": "digital"},
        {"payment_method_key": 6, "payment_method": "cod", "display_name": "Cash on Delivery", "method_type": "cash"},
    ])
    logger.info(f"dim_payment_method built: {len(methods)} rows")
    return methods


def enrich_customer_segments(customers_df, orders_df):
    """
    Enrich customers with segments based on order history.

    Segments:
    - Premium:  Total spending > 50,000 AND > 5 orders (high-value loyal)
    - Regular:  Total spending > 2,000 OR > 1 order    (moderate buyers)
    - New:      Low activity / no order history
    """
    customer_stats = orders_df.groupby("customer_id").agg(
        total_orders=("order_id", "nunique"),
        total_spent=("net_amount", "sum"),
        first_order_date=("order_date", "min"),
        last_order_date=("order_date", "max"),
    ).reset_index()

    customers_enriched = customers_df.merge(customer_stats, on="customer_id", how="left")
    customers_enriched["total_orders"] = customers_enriched["total_orders"].fillna(0).astype(int)
    customers_enriched["total_spent"] = customers_enriched["total_spent"].fillna(0)
    customers_enriched["first_order_date"] = pd.to_datetime(customers_enriched["first_order_date"]).dt.date
    customers_enriched["last_order_date"] = pd.to_datetime(customers_enriched["last_order_date"]).dt.date
    customers_enriched["avg_order_value"] = (
        customers_enriched["total_spent"] / customers_enriched["total_orders"].replace(0, 1)
    ).round(2)

    conditions = [
        (customers_enriched["total_spent"] > 50000) & (customers_enriched["total_orders"] > 5),
        (customers_enriched["total_spent"] > 2000) | (customers_enriched["total_orders"] > 1),
    ]
    choices = ["Premium", "Regular"]
    customers_enriched["segment"] = np.select(conditions, choices, default="New")

    # Assign surrogate keys
    customers_enriched = customers_enriched.reset_index(drop=True)
    customers_enriched["customer_key"] = customers_enriched.index + 1

    logger.info(f"Customer segments: {customers_enriched['segment'].value_counts().to_dict()}")
    return customers_enriched


def build_product_dimension(products_df):
    """Build Product Dimension with surrogate keys."""
    products_df = products_df.copy()
    products_df = products_df.reset_index(drop=True)
    products_df["product_key"] = products_df.index + 1
    logger.info(f"dim_product built: {len(products_df)} products")
    return products_df


# ──────────────────────────────────────────────
# Fact Table Builder
# ──────────────────────────────────────────────
def build_order_items_fact(orders_df, items_df, payments_df,
                           customers_dim, products_dim,
                           payment_method_dim, location_dim):
    """
    Build the Order Items Fact table.

    Grain: One row per order line item.
    Joins all dimensions through surrogate keys.
    """
    # Create lookup maps for surrogate keys
    customer_key_map = customers_dim.set_index("customer_id")["customer_key"].to_dict()
    product_key_map = products_dim.set_index("product_id")["product_key"].to_dict()
    pm_key_map = payment_method_dim.set_index("payment_method")["payment_method_key"].to_dict()

    # Location key map: city → location_key
    location_key_map = location_dim.set_index("city")["location_key"].to_dict()

    # Customer city map: customer_id → city
    customer_city_map = customers_dim.set_index("customer_id")["city"].to_dict()

    # Product cost map: product_id → cost_price
    product_cost_map = products_dim.set_index("product_id")["cost_price"].to_dict()

    # Get payment info per order (most recent payment)
    payment_info = (
        payments_df
        .sort_values("payment_date", ascending=False)
        .drop_duplicates(subset=["order_id"], keep="first")
        [["order_id", "payment_method", "payment_status", "transaction_id"]]
    )

    # Merge order-level data into items
    fact = items_df.merge(
        orders_df[["order_id", "customer_id", "date_key", "status",
                    "discount_amount", "shipping_cost", "total_amount"]],
        on="order_id", how="inner"
    )

    # Merge payment info
    fact = fact.merge(payment_info, on="order_id", how="left")

    # Calculate order-level line count for pro-rata allocation
    line_counts = fact.groupby("order_id")["item_id"].transform("count")
    line_totals = fact.groupby("order_id")["line_total"].transform("sum")

    # Pro-rate order-level discount and shipping to each line
    fact["line_share"] = fact["line_total"] / line_totals.replace(0, 1)
    fact["order_discount_alloc"] = (fact["discount_amount"] * fact["line_share"]).round(2)
    fact["shipping_cost_alloc"] = (fact["shipping_cost"] * fact["line_share"]).round(2)
    fact["net_amount"] = (fact["line_total"] - fact["order_discount_alloc"] + fact["shipping_cost_alloc"]).round(2)

    # Map surrogate keys
    fact["customer_key"] = fact["customer_id"].map(customer_key_map)
    fact["product_key"] = fact["product_id"].map(product_key_map)
    fact["payment_method_key"] = fact["payment_method"].map(pm_key_map)

    # Map location through customer city
    fact["customer_city"] = fact["customer_id"].map(customer_city_map)
    fact["location_key"] = fact["customer_city"].map(location_key_map)

    # Calculate cost and profit
    fact["cost_price"] = fact["product_id"].map(product_cost_map).fillna(0)
    fact["cost_amount"] = (fact["quantity"] * fact["cost_price"]).round(2)
    fact["gross_profit"] = (fact["net_amount"] - fact["cost_amount"]).round(2)

    # Select final columns
    fact_final = fact[[
        "order_id", "item_id", "date_key", "customer_key", "product_key",
        "payment_method_key", "location_key",
        "status", "payment_status", "transaction_id",
        "quantity", "unit_price", "discount",
        "line_total", "order_discount_alloc", "shipping_cost_alloc",
        "net_amount", "cost_amount", "gross_profit",
    ]].rename(columns={
        "status": "order_status",
        "discount": "line_discount",
    })

    # Drop rows with missing keys
    fact_final = fact_final.dropna(subset=["customer_key", "product_key", "date_key"])
    fact_final["customer_key"] = fact_final["customer_key"].astype(int)
    fact_final["product_key"] = fact_final["product_key"].astype(int)
    fact_final["date_key"] = fact_final["date_key"].astype(int)
    fact_final["payment_method_key"] = pd.to_numeric(fact_final["payment_method_key"], errors="coerce").astype("Int64")
    fact_final["location_key"] = pd.to_numeric(fact_final["location_key"], errors="coerce").astype("Int64")

    logger.info(f"fact_order_items built: {len(fact_final)} rows from {fact_final['order_id'].nunique()} orders")
    return fact_final


# ──────────────────────────────────────────────
# Main Transform Pipeline
# ──────────────────────────────────────────────
def run_transformation():
    """
    Execute the full transformation pipeline.

    Steps:
    1. Load raw CSVs
    2. Clean each dataset
    3. Build dimension tables
    4. Build fact table
    5. Save processed data
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
    items_clean = clean_order_items(items_df)
    payments_clean = clean_payments(payments_df)

    # ── Step 3: Build dimension tables ──
    logger.info("Building dimension tables...")
    date_dim = build_date_dimension()
    customers_dim = enrich_customer_segments(customers_clean, orders_clean)
    products_dim = build_product_dimension(products_clean)
    payment_method_dim = build_payment_method_dimension()
    location_dim = build_location_dimension(customers_dim)

    # ── Step 4: Build fact table ──
    logger.info("Building fact table...")
    fact = build_order_items_fact(
        orders_clean, items_clean, payments_clean,
        customers_dim, products_dim,
        payment_method_dim, location_dim,
    )

    # ── Step 5: Save processed data ──
    logger.info("Saving processed data...")
    output_files = {
        "dim_date.csv": date_dim,
        "dim_customer.csv": customers_dim,
        "dim_product.csv": products_dim,
        "dim_payment_method.csv": payment_method_dim,
        "dim_location.csv": location_dim,
        "fact_order_items.csv": fact,
    }

    for filename, df in output_files.items():
        filepath = os.path.join(PROCESSED_DATA_PATH, filename)
        df.to_csv(filepath, index=False, encoding="utf-8")
        logger.info(f"Saved: {filepath} | Rows: {len(df)}")

    # ── Summary ──
    elapsed = (datetime.now() - start_time).total_seconds()
    results = {name.replace(".csv", ""): len(df) for name, df in output_files.items()}

    logger.info(f"TRANSFORMATION COMPLETE | Time: {elapsed:.2f}s")

    print("\n── Transformation Summary ──")
    print(f"{'Output Table':<25} {'Rows':<10}")
    print("─" * 35)
    for table, count in results.items():
        print(f"{table:<25} {count:<10}")
    print(f"\nTotal time: {elapsed:.2f} seconds")

    return results


if __name__ == "__main__":
    results = run_transformation()
