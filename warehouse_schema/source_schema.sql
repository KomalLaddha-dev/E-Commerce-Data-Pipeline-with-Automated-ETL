-- ============================================================
-- SOURCE DATABASE SCHEMA (OLTP)
-- E-commerce Data Pipeline with Automated ETL
-- Database: MySQL / PostgreSQL
-- ============================================================

-- ────────────────────────────────────────────
-- Customers Table
-- Stores registered customer information
-- ────────────────────────────────────────────
CREATE TABLE customers (
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

CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_reg_date ON customers(registration_date);

-- ────────────────────────────────────────────
-- Products Table
-- Stores product catalog information
-- ────────────────────────────────────────────
CREATE TABLE products (
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

CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_brand ON products(brand);
CREATE INDEX idx_products_updated ON products(updated_at);

-- ────────────────────────────────────────────
-- Orders Table
-- Stores order header information
-- ────────────────────────────────────────────
CREATE TABLE orders (
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

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);

-- ────────────────────────────────────────────
-- Order Items Table
-- Stores individual line items per order
-- ────────────────────────────────────────────
CREATE TABLE order_items (
    item_id       SERIAL PRIMARY KEY,
    order_id      INT NOT NULL,
    product_id    INT NOT NULL,
    quantity      INT NOT NULL DEFAULT 1 CHECK (quantity > 0),
    unit_price    DECIMAL(10, 2) NOT NULL CHECK (unit_price > 0),
    discount      DECIMAL(10, 2) DEFAULT 0.00,
    total_price   DECIMAL(12, 2) GENERATED ALWAYS AS (
                      (unit_price * quantity) - discount
                  ) STORED,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE INDEX idx_items_order ON order_items(order_id);
CREATE INDEX idx_items_product ON order_items(product_id);

-- ────────────────────────────────────────────
-- Payments Table
-- Stores payment transactions per order
-- ────────────────────────────────────────────
CREATE TABLE payments (
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

CREATE INDEX idx_payments_order ON payments(order_id);
CREATE INDEX idx_payments_date ON payments(payment_date);
CREATE INDEX idx_payments_status ON payments(payment_status);

-- ────────────────────────────────────────────
-- ETL Metadata Table
-- Tracks pipeline run history and watermarks
-- ────────────────────────────────────────────
CREATE TABLE etl_metadata (
    id              SERIAL PRIMARY KEY,
    table_name      VARCHAR(100) NOT NULL,
    last_run        TIMESTAMP,
    rows_extracted  INT DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'pending',
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO etl_metadata (table_name) VALUES
    ('orders'), ('order_items'), ('customers'), ('products'), ('payments');


-- ============================================================
-- SAMPLE DATA INSERTS
-- ============================================================

-- Sample Customers
INSERT INTO customers (first_name, last_name, email, phone, city, state, country, postal_code) VALUES
    ('Rahul',   'Sharma',   'rahul.sharma@email.com',   '9876543210', 'Mumbai',    'Maharashtra',  'India', '400001'),
    ('Priya',   'Patel',    'priya.patel@email.com',    '9876543211', 'Ahmedabad', 'Gujarat',      'India', '380001'),
    ('Amit',    'Kumar',    'amit.kumar@email.com',     '9876543212', 'Delhi',     'Delhi',        'India', '110001'),
    ('Sneha',   'Reddy',    'sneha.reddy@email.com',    '9876543213', 'Hyderabad', 'Telangana',    'India', '500001'),
    ('Vikram',  'Singh',    'vikram.singh@email.com',   '9876543214', 'Jaipur',    'Rajasthan',    'India', '302001'),
    ('Ananya',  'Das',      'ananya.das@email.com',     '9876543215', 'Kolkata',   'West Bengal',  'India', '700001'),
    ('Karthik', 'Nair',     'karthik.nair@email.com',   '9876543216', 'Kochi',     'Kerala',       'India', '682001'),
    ('Deepa',   'Gupta',    'deepa.gupta@email.com',    '9876543217', 'Lucknow',   'Uttar Pradesh','India', '226001'),
    ('Arjun',   'Mehta',    'arjun.mehta@email.com',    '9876543218', 'Pune',      'Maharashtra',  'India', '411001'),
    ('Pooja',   'Iyer',     'pooja.iyer@email.com',     '9876543219', 'Chennai',   'Tamil Nadu',   'India', '600001');

-- Sample Products
INSERT INTO products (product_name, category, brand, price, cost_price, stock_quantity) VALUES
    ('Wireless Bluetooth Earbuds',    'Electronics', 'boAt',      1299.00,  650.00, 500),
    ('Cotton Round Neck T-Shirt',     'Clothing',    'H&M',       599.00,   200.00, 1200),
    ('Stainless Steel Water Bottle',  'Home',        'Milton',    449.00,   180.00, 800),
    ('Running Shoes Pro',             'Footwear',    'Nike',      4999.00,  2200.00, 300),
    ('Laptop Backpack 15.6 inch',     'Accessories', 'Wildcraft', 1199.00,  500.00, 650),
    ('Smartphone 128GB',              'Electronics', 'Samsung',   15999.00, 10500.00, 200),
    ('Yoga Mat Premium',              'Fitness',     'Decathlon', 899.00,   350.00, 400),
    ('Organic Green Tea (100 bags)',   'Grocery',     'Organic India', 349.00, 150.00, 2000),
    ('LED Desk Lamp',                 'Home',        'Philips',   1599.00,  700.00, 350),
    ('Bestseller Novel Collection',   'Books',       'Penguin',   799.00,   400.00, 1500);

-- Sample Orders
INSERT INTO orders (customer_id, order_date, status, total_amount, discount_amount, shipping_cost) VALUES
    (1, '2025-12-01 10:30:00', 'delivered',  2498.00, 200.00, 49.00),
    (2, '2025-12-01 11:15:00', 'delivered',  5598.00, 500.00,  0.00),
    (3, '2025-12-02 09:00:00', 'shipped',   15999.00,   0.00,  0.00),
    (4, '2025-12-02 14:22:00', 'cancelled',   449.00,   0.00, 49.00),
    (5, '2025-12-03 08:45:00', 'delivered',  6198.00, 300.00,  0.00),
    (1, '2025-12-04 16:00:00', 'confirmed',  1199.00, 100.00, 49.00),
    (6, '2025-12-05 12:30:00', 'delivered',  1698.00,   0.00,  0.00),
    (7, '2025-12-06 19:15:00', 'shipped',    4999.00, 250.00,  0.00),
    (3, '2025-12-07 10:00:00', 'pending',     349.00,   0.00, 29.00),
    (8, '2025-12-08 11:45:00', 'delivered',  2398.00, 150.00,  0.00);

-- Sample Order Items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount) VALUES
    (1, 1, 1, 1299.00, 100.00),
    (1, 2, 2,  599.00,  50.00),
    (2, 4, 1, 4999.00, 500.00),
    (2, 2, 1,  599.00,   0.00),
    (3, 6, 1, 15999.00,  0.00),
    (4, 3, 1,  449.00,   0.00),
    (5, 4, 1, 4999.00, 200.00),
    (5, 5, 1, 1199.00, 100.00),
    (6, 5, 1, 1199.00, 100.00),
    (7, 1, 1, 1299.00,   0.00),
    (7, 7, 1,  899.00,   0.00),
    (8, 4, 1, 4999.00, 250.00),
    (9, 8, 1,  349.00,   0.00),
    (10, 9, 1, 1599.00, 100.00),
    (10, 10,1,  799.00,  50.00);

-- Sample Payments
INSERT INTO payments (order_id, payment_date, payment_method, payment_status, amount, transaction_id) VALUES
    (1, '2025-12-01 10:31:00', 'upi',         'completed', 2347.00, 'TXN20251201001'),
    (2, '2025-12-01 11:16:00', 'credit_card',  'completed', 5098.00, 'TXN20251201002'),
    (3, '2025-12-02 09:01:00', 'net_banking',  'completed', 15999.00,'TXN20251202001'),
    (4, '2025-12-02 14:23:00', 'upi',          'refunded',   498.00, 'TXN20251202002'),
    (5, '2025-12-03 08:46:00', 'debit_card',   'completed', 5898.00, 'TXN20251203001'),
    (6, '2025-12-04 16:01:00', 'wallet',       'pending',   1148.00, 'TXN20251204001'),
    (7, '2025-12-05 12:31:00', 'credit_card',  'completed', 1698.00, 'TXN20251205001'),
    (8, '2025-12-06 19:16:00', 'upi',          'completed', 4749.00, 'TXN20251206001'),
    (9, '2025-12-07 10:01:00', 'cod',          'pending',    378.00, 'TXN20251207001'),
    (10,'2025-12-08 11:46:00', 'debit_card',   'completed', 2248.00, 'TXN20251208001');
