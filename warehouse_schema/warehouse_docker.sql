-- ============================================================
-- DATA WAREHOUSE SCHEMA (Docker-Adapted)
-- E-commerce Data Pipeline with Automated ETL
-- 
-- This schema matches the exact column layout produced by
-- the ETL transform step, enabling direct CSV-to-table loading.
-- Target: PostgreSQL 15+ in Docker
-- ============================================================

-- ────────────────────────────────────────────
-- Date Dimension
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS date_dim (
    date_key        INT PRIMARY KEY,
    full_date       DATE,
    day             SMALLINT,
    month           SMALLINT,
    year            SMALLINT,
    quarter         SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    month_name      VARCHAR(10),
    is_weekend      BOOLEAN,
    fiscal_year     SMALLINT,
    week_of_year    SMALLINT
);

CREATE INDEX IF NOT EXISTS idx_dd_full_date ON date_dim(full_date);
CREATE INDEX IF NOT EXISTS idx_dd_year_month ON date_dim(year, month);

-- ────────────────────────────────────────────
-- Customer Dimension
-- Columns match transform output exactly
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customer_dim (
    customer_id       INT PRIMARY KEY,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    email             VARCHAR(255),
    phone             VARCHAR(20),
    address_line1     VARCHAR(255),
    address_line2     VARCHAR(255),
    city              VARCHAR(100),
    state             VARCHAR(100),
    country           VARCHAR(100),
    postal_code       VARCHAR(20),
    registration_date TIMESTAMP,
    is_active         BOOLEAN,
    segment           VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_cd_segment ON customer_dim(segment);
CREATE INDEX IF NOT EXISTS idx_cd_city ON customer_dim(city);

-- ────────────────────────────────────────────
-- Product Dimension
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS product_dim (
    product_id      INT PRIMARY KEY,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    brand           VARCHAR(100),
    price           DECIMAL(10, 2),
    cost_price      DECIMAL(10, 2),
    stock_quantity  INT,
    weight_kg       DECIMAL(5, 2),
    is_active       BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    profit_margin   DECIMAL(5, 2)
);

CREATE INDEX IF NOT EXISTS idx_pd_category ON product_dim(category);
CREATE INDEX IF NOT EXISTS idx_pd_brand ON product_dim(brand);

-- ────────────────────────────────────────────
-- Payment Dimension
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS payment_dim (
    payment_id      INT PRIMARY KEY,
    order_id        INT,
    payment_date    TIMESTAMP,
    payment_method  VARCHAR(50),
    payment_status  VARCHAR(50),
    amount          DECIMAL(12, 2),
    transaction_id  VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_pmd_method ON payment_dim(payment_method);
CREATE INDEX IF NOT EXISTS idx_pmd_status ON payment_dim(payment_status);

-- ────────────────────────────────────────────
-- Sales Fact Table
-- Uses business keys directly (no surrogate keys)
-- Grain: One row per order
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sales_fact (
    order_id          INT PRIMARY KEY,
    customer_id       INT,
    date_key          INT,
    order_status      VARCHAR(50),
    total_amount      DECIMAL(12, 2),
    discount_amount   DECIMAL(10, 2) DEFAULT 0,
    shipping_cost     DECIMAL(10, 2) DEFAULT 0,
    net_amount        DECIMAL(12, 2),
    total_items       INT DEFAULT 1,
    payment_method    VARCHAR(50),
    payment_id        INT
);

CREATE INDEX IF NOT EXISTS idx_sf_customer ON sales_fact(customer_id);
CREATE INDEX IF NOT EXISTS idx_sf_date ON sales_fact(date_key);
CREATE INDEX IF NOT EXISTS idx_sf_status ON sales_fact(order_status);

-- ────────────────────────────────────────────
-- Materialized View: Daily Sales Summary
-- ────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_sales_summary AS
SELECT
    d.date_key,
    d.full_date,
    d.day_name,
    d.month_name,
    d.year,
    d.quarter,
    d.is_weekend,
    COUNT(DISTINCT f.order_id)       AS total_orders,
    SUM(f.total_items)               AS total_items_sold,
    SUM(f.total_amount)              AS gross_revenue,
    SUM(f.discount_amount)           AS total_discounts,
    SUM(f.shipping_cost)             AS total_shipping,
    SUM(f.net_amount)                AS net_revenue,
    ROUND(AVG(f.net_amount), 2)      AS avg_order_value
FROM sales_fact f
JOIN date_dim d ON f.date_key = d.date_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY d.date_key, d.full_date, d.day_name, d.month_name,
         d.year, d.quarter, d.is_weekend;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily ON mv_daily_sales_summary(date_key);

-- ────────────────────────────────────────────
-- Materialized View: Product Performance
-- (Product-level view using payment_dim as proxy for product data)
-- Note: Since sales_fact grain is per-order (no product_id),
-- this view summarizes at the product dimension level only.
-- ────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    p.cost_price,
    p.profit_margin,
    p.stock_quantity
FROM product_dim p;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_prod ON mv_product_performance(product_id);

-- ────────────────────────────────────────────
-- ETL Audit Log
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_audit_log (
    audit_id        SERIAL PRIMARY KEY,
    batch_id        VARCHAR(50),
    table_name      VARCHAR(100),
    rows_processed  INT DEFAULT 0,
    status          VARCHAR(20),
    started_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at    TIMESTAMP,
    error_message   TEXT
);
