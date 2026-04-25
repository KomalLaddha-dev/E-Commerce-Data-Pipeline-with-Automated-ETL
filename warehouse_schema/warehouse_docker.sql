-- ============================================================
-- DATA WAREHOUSE SCHEMA (Docker-Adapted)
-- E-commerce Data Pipeline with Automated ETL
--
-- Identical to warehouse_schema.sql but structured for
-- Docker init. Target: PostgreSQL 15+ in Docker.
-- ============================================================

-- ── DIMENSION: Date ─────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    day             SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,
    day_name        VARCHAR(10) NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    fiscal_year     SMALLINT NOT NULL,
    week_of_year    SMALLINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_ym ON dim_date(year, month);

-- ── DIMENSION: Customer ─────────────────────
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     INT UNIQUE NOT NULL,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255),
    phone           VARCHAR(20),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100) DEFAULT 'India',
    postal_code     VARCHAR(20),
    registration_date TIMESTAMP,
    is_active       BOOLEAN DEFAULT TRUE,
    segment         VARCHAR(50),
    total_orders    INT DEFAULT 0,
    total_spent     DECIMAL(14,2) DEFAULT 0,
    first_order_date DATE,
    last_order_date  DATE,
    avg_order_value  DECIMAL(10,2) DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_dc_segment ON dim_customer(segment);
CREATE INDEX IF NOT EXISTS idx_dc_city ON dim_customer(city);
CREATE INDEX IF NOT EXISTS idx_dc_customer_id ON dim_customer(customer_id);

-- ── DIMENSION: Product ──────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_key     SERIAL PRIMARY KEY,
    product_id      INT UNIQUE NOT NULL,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    brand           VARCHAR(100),
    price           DECIMAL(10,2),
    cost_price      DECIMAL(10,2),
    stock_quantity  INT DEFAULT 0,
    weight_kg       DECIMAL(5,2),
    is_active       BOOLEAN DEFAULT TRUE,
    profit_margin   DECIMAL(5,2)
);

CREATE INDEX IF NOT EXISTS idx_dp_category ON dim_product(category);
CREATE INDEX IF NOT EXISTS idx_dp_brand ON dim_product(brand);
CREATE INDEX IF NOT EXISTS idx_dp_product_id ON dim_product(product_id);

-- ── DIMENSION: Payment Method ───────────────
CREATE TABLE IF NOT EXISTS dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method     VARCHAR(50) UNIQUE NOT NULL,
    display_name       VARCHAR(100) NOT NULL,
    method_type        VARCHAR(30)
);

INSERT INTO dim_payment_method (payment_method, display_name, method_type) VALUES
    ('credit_card',  'Credit Card',      'card'),
    ('debit_card',   'Debit Card',       'card'),
    ('upi',          'UPI',              'digital'),
    ('net_banking',  'Net Banking',      'digital'),
    ('wallet',       'Wallet',           'digital'),
    ('cod',          'Cash on Delivery', 'cash')
ON CONFLICT (payment_method) DO NOTHING;

-- ── DIMENSION: Location ─────────────────────
CREATE TABLE IF NOT EXISTS dim_location (
    location_key    SERIAL PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    state           VARCHAR(100) NOT NULL,
    region          VARCHAR(50),
    tier            VARCHAR(20),
    country         VARCHAR(100) DEFAULT 'India',
    UNIQUE(city, state)
);

CREATE INDEX IF NOT EXISTS idx_dl_state ON dim_location(state);
CREATE INDEX IF NOT EXISTS idx_dl_region ON dim_location(region);

-- ── FACT: Order Items ───────────────────────
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_key      SERIAL PRIMARY KEY,
    order_id            INT NOT NULL,
    item_id             INT,
    date_key            INT NOT NULL,
    customer_key        INT NOT NULL,
    product_key         INT NOT NULL,
    payment_method_key  INT,
    location_key        INT,
    order_status        VARCHAR(50),
    payment_status      VARCHAR(50),
    transaction_id      VARCHAR(255),
    quantity            INT NOT NULL DEFAULT 1,
    unit_price          DECIMAL(10,2) NOT NULL,
    line_discount       DECIMAL(10,2) DEFAULT 0,
    line_total          DECIMAL(12,2) NOT NULL,
    order_discount_alloc  DECIMAL(10,2) DEFAULT 0,
    shipping_cost_alloc   DECIMAL(10,2) DEFAULT 0,
    net_amount            DECIMAL(12,2) NOT NULL,
    cost_amount           DECIMAL(12,2) DEFAULT 0,
    gross_profit          DECIMAL(12,2) DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_foi_order ON fact_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_foi_date ON fact_order_items(date_key);
CREATE INDEX IF NOT EXISTS idx_foi_customer ON fact_order_items(customer_key);
CREATE INDEX IF NOT EXISTS idx_foi_product ON fact_order_items(product_key);
CREATE INDEX IF NOT EXISTS idx_foi_status ON fact_order_items(order_status);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_foi_date') THEN
        ALTER TABLE fact_order_items
        ADD CONSTRAINT fk_foi_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_foi_customer') THEN
        ALTER TABLE fact_order_items
        ADD CONSTRAINT fk_foi_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_foi_product') THEN
        ALTER TABLE fact_order_items
        ADD CONSTRAINT fk_foi_product FOREIGN KEY (product_key) REFERENCES dim_product(product_key);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_foi_payment_method') THEN
        ALTER TABLE fact_order_items
        ADD CONSTRAINT fk_foi_payment_method FOREIGN KEY (payment_method_key) REFERENCES dim_payment_method(payment_method_key);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_foi_location') THEN
        ALTER TABLE fact_order_items
        ADD CONSTRAINT fk_foi_location FOREIGN KEY (location_key) REFERENCES dim_location(location_key);
    END IF;
END $$;

-- ── MATERIALIZED VIEW: Daily Sales ──────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_sales AS
SELECT
    d.date_key, d.full_date, d.day_name, d.month_name,
    d.year, d.quarter, d.is_weekend,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_items_sold,
    SUM(f.line_total) AS gross_revenue,
    SUM(f.order_discount_alloc + f.line_discount) AS total_discounts,
    SUM(f.shipping_cost_alloc) AS total_shipping,
    SUM(f.net_amount) AS net_revenue,
    SUM(f.gross_profit) AS gross_profit,
    ROUND(AVG(f.net_amount), 2) AS avg_line_value,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY d.date_key, d.full_date, d.day_name, d.month_name,
         d.year, d.quarter, d.is_weekend;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily ON mv_daily_sales(date_key);

-- ── MATERIALIZED VIEW: Product Performance ──
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_performance AS
SELECT
    p.product_key, p.product_id, p.product_name, p.category,
    p.sub_category, p.brand, p.price, p.cost_price,
    p.profit_margin, p.stock_quantity,
    COALESCE(COUNT(DISTINCT f.order_id), 0) AS total_orders,
    COALESCE(SUM(f.quantity), 0) AS total_units_sold,
    COALESCE(SUM(f.net_amount), 0) AS total_revenue,
    COALESCE(SUM(f.gross_profit), 0) AS total_profit,
    ROUND(COALESCE(AVG(f.unit_price), 0), 2) AS avg_selling_price
FROM dim_product p
LEFT JOIN fact_order_items f ON p.product_key = f.product_key
    AND f.order_status NOT IN ('cancelled', 'returned')
GROUP BY p.product_key, p.product_id, p.product_name, p.category,
         p.sub_category, p.brand, p.price, p.cost_price,
         p.profit_margin, p.stock_quantity;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_prod ON mv_product_performance(product_key);

-- ── MATERIALIZED VIEW: Customer Summary ─────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customer_summary AS
SELECT
    c.customer_key, c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.city, c.state, c.segment,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.net_amount) AS lifetime_value,
    ROUND(AVG(f.net_amount), 2) AS avg_item_value,
    MIN(d.full_date) AS first_purchase,
    MAX(d.full_date) AS last_purchase,
    MAX(d.full_date) - MIN(d.full_date) AS tenure_days
FROM dim_customer c
LEFT JOIN fact_order_items f ON c.customer_key = f.customer_key
    AND f.order_status NOT IN ('cancelled', 'returned')
LEFT JOIN dim_date d ON f.date_key = d.date_key
GROUP BY c.customer_key, c.customer_id, c.first_name, c.last_name,
         c.city, c.state, c.segment;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_cust ON mv_customer_summary(customer_key);

-- ── ETL Audit Log ───────────────────────────
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

CREATE INDEX IF NOT EXISTS idx_audit_batch ON etl_audit_log(batch_id);
CREATE INDEX IF NOT EXISTS idx_audit_status ON etl_audit_log(status);
