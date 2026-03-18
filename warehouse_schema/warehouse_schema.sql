-- ============================================================
-- DATA WAREHOUSE SCHEMA (Star Schema)
-- E-commerce Data Pipeline with Automated ETL
-- Target: PostgreSQL / Snowflake / BigQuery
-- ============================================================

-- ────────────────────────────────────────────
-- Date Dimension
-- Pre-populated calendar table for time-based analysis
-- ────────────────────────────────────────────
CREATE TABLE date_dim (
    date_key        INT PRIMARY KEY,            -- YYYYMMDD format (e.g., 20251201)
    full_date       DATE NOT NULL UNIQUE,
    day             SMALLINT NOT NULL,           -- 1-31
    month           SMALLINT NOT NULL,           -- 1-12
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,           -- 1-4
    day_of_week     SMALLINT NOT NULL,           -- 0=Monday, 6=Sunday
    day_name        VARCHAR(10) NOT NULL,        -- Monday, Tuesday, ...
    month_name      VARCHAR(10) NOT NULL,        -- January, February, ...
    is_weekend      BOOLEAN NOT NULL,
    fiscal_year     SMALLINT NOT NULL,
    week_of_year    SMALLINT NOT NULL
);

CREATE INDEX idx_date_full ON date_dim(full_date);
CREATE INDEX idx_date_year_month ON date_dim(year, month);

-- ────────────────────────────────────────────
-- Customer Dimension
-- Stores customer profile and segmentation data
-- ────────────────────────────────────────────
CREATE TABLE customer_dim (
    customer_key      SERIAL PRIMARY KEY,
    customer_id       INT UNIQUE NOT NULL,       -- Business key from source
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    email             VARCHAR(255),
    phone             VARCHAR(20),
    city              VARCHAR(100),
    state             VARCHAR(100),
    country           VARCHAR(100),
    postal_code       VARCHAR(20),
    segment           VARCHAR(50),               -- Premium / Regular / New
    registration_date DATE,
    is_active         BOOLEAN DEFAULT TRUE,
    -- SCD Type 2 fields (for tracking historical changes)
    effective_date    DATE DEFAULT CURRENT_DATE,
    expiration_date   DATE DEFAULT '9999-12-31',
    is_current        BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_customer_id ON customer_dim(customer_id);
CREATE INDEX idx_customer_segment ON customer_dim(segment);

-- ────────────────────────────────────────────
-- Product Dimension
-- Stores product catalog and pricing data
-- ────────────────────────────────────────────
CREATE TABLE product_dim (
    product_key     SERIAL PRIMARY KEY,
    product_id      INT UNIQUE NOT NULL,         -- Business key from source
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    brand           VARCHAR(100),
    price           DECIMAL(10, 2),
    cost_price      DECIMAL(10, 2),
    profit_margin   DECIMAL(5, 2),               -- Calculated: ((price - cost) / price) * 100
    is_active       BOOLEAN DEFAULT TRUE,
    effective_date  DATE DEFAULT CURRENT_DATE,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current      BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_product_id ON product_dim(product_id);
CREATE INDEX idx_product_category ON product_dim(category);
CREATE INDEX idx_product_brand ON product_dim(brand);

-- ────────────────────────────────────────────
-- Payment Dimension
-- Stores payment method and gateway information
-- ────────────────────────────────────────────
CREATE TABLE payment_dim (
    payment_key     SERIAL PRIMARY KEY,
    payment_id      INT UNIQUE,
    payment_method  VARCHAR(50),                 -- credit_card, debit_card, upi, etc.
    payment_status  VARCHAR(50),                 -- completed, pending, failed, refunded
    transaction_id  VARCHAR(255),
    gateway         VARCHAR(100) DEFAULT 'default'
);

CREATE INDEX idx_payment_method ON payment_dim(payment_method);
CREATE INDEX idx_payment_status ON payment_dim(payment_status);

-- ────────────────────────────────────────────
-- Sales Fact Table
-- Central fact table linking all dimensions
-- Grain: One row per order
-- ────────────────────────────────────────────
CREATE TABLE sales_fact (
    sale_id           SERIAL PRIMARY KEY,
    order_id          INT NOT NULL UNIQUE,        -- Degenerate dimension
    customer_key      INT REFERENCES customer_dim(customer_key),
    product_key       INT REFERENCES product_dim(product_key),
    date_key          INT REFERENCES date_dim(date_key),
    payment_key       INT REFERENCES payment_dim(payment_key),

    -- Measures (additive facts)
    quantity          INT,
    unit_price        DECIMAL(10, 2),
    total_amount      DECIMAL(12, 2),
    discount_amount   DECIMAL(10, 2) DEFAULT 0,
    shipping_cost     DECIMAL(10, 2) DEFAULT 0,
    net_amount        DECIMAL(12, 2),             -- total - discount + shipping

    -- Descriptive attributes
    order_status      VARCHAR(50),
    total_items       INT DEFAULT 1,

    -- Audit columns
    etl_loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id      VARCHAR(50)
);

CREATE INDEX idx_fact_customer ON sales_fact(customer_key);
CREATE INDEX idx_fact_product ON sales_fact(product_key);
CREATE INDEX idx_fact_date ON sales_fact(date_key);
CREATE INDEX idx_fact_payment ON sales_fact(payment_key);
CREATE INDEX idx_fact_order ON sales_fact(order_id);
CREATE INDEX idx_fact_status ON sales_fact(order_status);

-- ────────────────────────────────────────────
-- Staging Table for Upsert Operations
-- Used during the Load phase of ETL
-- ────────────────────────────────────────────
CREATE TABLE staging_sales_fact (
    LIKE sales_fact INCLUDING ALL
);

-- ────────────────────────────────────────────
-- Materialized View: Daily Sales Summary
-- Refreshed after each ETL run for fast queries
-- ────────────────────────────────────────────
CREATE MATERIALIZED VIEW mv_daily_sales_summary AS
SELECT
    d.date_key,
    d.full_date,
    d.day_name,
    d.month_name,
    d.year,
    d.quarter,
    d.is_weekend,
    COUNT(DISTINCT f.order_id)       AS total_orders,
    SUM(f.quantity)                   AS total_items_sold,
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

CREATE UNIQUE INDEX idx_mv_daily_date ON mv_daily_sales_summary(date_key);

-- ────────────────────────────────────────────
-- Materialized View: Product Performance
-- ────────────────────────────────────────────
CREATE MATERIALIZED VIEW mv_product_performance AS
SELECT
    p.product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    p.cost_price,
    p.profit_margin,
    COUNT(DISTINCT f.order_id)       AS total_orders,
    SUM(f.quantity)                   AS total_units_sold,
    SUM(f.net_amount)                AS total_revenue,
    ROUND(AVG(f.unit_price), 2)      AS avg_selling_price
FROM sales_fact f
JOIN product_dim p ON f.product_key = p.product_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY p.product_key, p.product_id, p.product_name, p.category,
         p.brand, p.price, p.cost_price, p.profit_margin;

CREATE UNIQUE INDEX idx_mv_product ON mv_product_performance(product_key);

-- ────────────────────────────────────────────
-- ETL Audit Table
-- Tracks every pipeline run for monitoring
-- ────────────────────────────────────────────
CREATE TABLE etl_audit_log (
    audit_id        SERIAL PRIMARY KEY,
    batch_id        VARCHAR(50) NOT NULL,
    dag_id          VARCHAR(100),
    task_id         VARCHAR(100),
    table_name      VARCHAR(100),
    rows_processed  INT DEFAULT 0,
    status          VARCHAR(20),                 -- started, completed, failed
    started_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at    TIMESTAMP,
    error_message   TEXT
);

CREATE INDEX idx_audit_batch ON etl_audit_log(batch_id);
CREATE INDEX idx_audit_status ON etl_audit_log(status);
