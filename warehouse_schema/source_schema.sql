-- ============================================================
-- SOURCE DATABASE SCHEMA (OLTP)
-- E-commerce Data Pipeline with Automated ETL
-- Database: PostgreSQL
-- ============================================================

-- ────────────────────────────────────────────
-- Customers Table
-- Stores registered customer information
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers (
    customer_id       SERIAL PRIMARY KEY,
    first_name        VARCHAR(100) NOT NULL,
    last_name         VARCHAR(100) NOT NULL,
    email             VARCHAR(255) UNIQUE NOT NULL,
    phone             VARCHAR(20),
    address_line1     VARCHAR(255),
    address_line2     VARCHAR(255),
    city              VARCHAR(100),
    state             VARCHAR(100),
    country           VARCHAR(100) DEFAULT 'India',
    postal_code       VARCHAR(20),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active         BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_city ON customers(city);
CREATE INDEX IF NOT EXISTS idx_customers_reg_date ON customers(registration_date);

-- ────────────────────────────────────────────
-- Products Table
-- Stores product catalog information
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    product_id      SERIAL PRIMARY KEY,
    product_name    VARCHAR(255) NOT NULL,
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    brand           VARCHAR(100),
    price           DECIMAL(10, 2) NOT NULL CHECK (price > 0),
    cost_price      DECIMAL(10, 2) CHECK (cost_price >= 0),
    stock_quantity  INT DEFAULT 0 CHECK (stock_quantity >= 0),
    weight_kg       DECIMAL(5, 2),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
CREATE INDEX IF NOT EXISTS idx_products_updated ON products(updated_at);

-- ────────────────────────────────────────────
-- Orders Table
-- Stores order header information
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    order_id         SERIAL PRIMARY KEY,
    customer_id      INT NOT NULL,
    order_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status           VARCHAR(20) CHECK (status IN (
                         'pending', 'confirmed', 'shipped',
                         'delivered', 'cancelled', 'returned'
                     )),
    total_amount     DECIMAL(12, 2) NOT NULL CHECK (total_amount >= 0),
    discount_amount  DECIMAL(10, 2) DEFAULT 0.00,
    shipping_cost    DECIMAL(10, 2) DEFAULT 0.00,
    shipping_address VARCHAR(500),
    billing_address  VARCHAR(500),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

-- ────────────────────────────────────────────
-- Order Items Table
-- Stores individual line items per order
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_items (
    item_id       SERIAL PRIMARY KEY,
    order_id      INT NOT NULL,
    product_id    INT NOT NULL,
    quantity      INT NOT NULL DEFAULT 1 CHECK (quantity > 0),
    unit_price    DECIMAL(10, 2) NOT NULL CHECK (unit_price > 0),
    discount      DECIMAL(10, 2) DEFAULT 0.00,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE INDEX IF NOT EXISTS idx_items_order ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_items_product ON order_items(product_id);

-- ────────────────────────────────────────────
-- Payments Table
-- Stores payment transactions per order
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS payments (
    payment_id      SERIAL PRIMARY KEY,
    order_id        INT NOT NULL,
    payment_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payment_method  VARCHAR(50) CHECK (payment_method IN (
                        'credit_card', 'debit_card', 'upi',
                        'net_banking', 'wallet', 'cod'
                    )),
    payment_status  VARCHAR(20) CHECK (payment_status IN (
                        'pending', 'completed', 'failed', 'refunded'
                    )),
    amount          DECIMAL(12, 2) NOT NULL CHECK (amount > 0),
    transaction_id  VARCHAR(255),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(payment_date);
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(payment_status);

-- ────────────────────────────────────────────
-- ETL Metadata Table
-- Tracks pipeline run history and watermarks
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_metadata (
    id              SERIAL PRIMARY KEY,
    table_name      VARCHAR(100) NOT NULL,
    last_run        TIMESTAMP,
    rows_extracted  INT DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'pending',
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO etl_metadata (table_name) VALUES
    ('orders'), ('order_items'), ('customers'), ('products'), ('payments')
ON CONFLICT DO NOTHING;
