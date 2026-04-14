"""
generate_large_data.py - Synthetic large-scale data generator for source OLTP.

Creates realistic high-volume customers, products, orders, order_items, and payments
inside the source PostgreSQL database using set-based SQL generation.
"""

import os
from datetime import datetime

import psycopg2


PG_HOST = os.environ.get("PG_HOST", os.environ.get("SOURCE_DB_HOST", "localhost"))
PG_PORT = int(os.environ.get("PG_PORT", os.environ.get("SOURCE_DB_PORT", "5432")))
PG_USER = os.environ.get("PG_USER", os.environ.get("SOURCE_DB_USER", "etl_user"))
PG_PASSWORD = os.environ.get("PG_PASSWORD", os.environ.get("SOURCE_DB_PASSWORD", "etl_password"))
SOURCE_DB = os.environ.get("SOURCE_DB_NAME", "ecommerce_db")


def _connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=SOURCE_DB,
    )


def _count_rows(cur, table_name):
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return int(cur.fetchone()[0])


def _get_order_date_distribution(cur):
    """Return min/max/span/distinct-day metrics for source orders."""
    cur.execute(
        """
        SELECT
            MIN(order_date) AS min_order_date,
            MAX(order_date) AS max_order_date,
            COALESCE(MAX(order_date)::date - MIN(order_date)::date, 0) AS span_days,
            COUNT(DISTINCT order_date::date) AS distinct_days
        FROM orders
        """
    )
    row = cur.fetchone()

    return {
        "min_order_date": row[0],
        "max_order_date": row[1],
        "span_days": int(row[2]) if row[2] is not None else 0,
        "distinct_days": int(row[3]) if row[3] is not None else 0,
    }


def _rebalance_existing_order_dates(cur, lookback_hours):
    """
    Redistribute existing order/payment timestamps across the configured horizon.

    This is used when current data is concentrated in a narrow date window.
    """
    cur.execute(
        """
        WITH randomized AS (
            SELECT
                order_id,
                NOW() - (random() * %s * INTERVAL '1 hour') AS new_order_date
            FROM orders
        )
        UPDATE orders o
        SET
            order_date = r.new_order_date,
            updated_at = NOW()
        FROM randomized r
        WHERE o.order_id = r.order_id
        """,
        (lookback_hours,),
    )
    orders_updated = cur.rowcount

    cur.execute(
        """
        UPDATE payments p
        SET payment_date = o.order_date + (random() * 6 * INTERVAL '1 hour')
        FROM orders o
        WHERE p.order_id = o.order_id
        """
    )
    payments_updated = cur.rowcount

    return orders_updated, payments_updated


def _insert_customers(cur, rows_to_add, run_token):
    if rows_to_add <= 0:
        return 0

    cur.execute(
        """
        INSERT INTO customers (
            first_name, last_name, email, phone,
            address_line1, address_line2, city, state, country,
            postal_code, registration_date, is_active, created_at, updated_at
        )
        SELECT
            'SyntheticFirst' || gs::text,
            'SyntheticLast' || gs::text,
            format('customer_%%s_%%s@example.com', %s, gs),
            LPAD(((7000000000 + floor(random() * 2999999999))::bigint)::text, 10, '0'),
            format('Address line 1 #%%s', gs),
            NULL,
            (ARRAY['Mumbai','Delhi','Bengaluru','Pune','Hyderabad','Chennai','Ahmedabad','Kolkata'])[1 + floor(random() * 8)::int],
            (ARRAY['Maharashtra','Delhi','Karnataka','Maharashtra','Telangana','Tamil Nadu','Gujarat','West Bengal'])[1 + floor(random() * 8)::int],
            'India',
            LPAD(((100000 + floor(random() * 899999))::int)::text, 6, '0'),
            NOW() - (random() * 180 * INTERVAL '1 day'),
            TRUE,
            NOW(),
            NOW()
        FROM generate_series(1, %s) AS gs
        """,
        (run_token, rows_to_add),
    )
    return rows_to_add


def _insert_products(cur, rows_to_add, run_token):
    if rows_to_add <= 0:
        return 0

    cur.execute(
        """
        WITH seeded AS (
            SELECT
                gs,
                (ARRAY['Electronics','Clothing','Home','Footwear','Accessories','Fitness','Grocery','Books'])[1 + floor(random() * 8)::int] AS category,
                (ARRAY['Nova','Vertex','Pulse','Summit','Astra','Core','Orbit','Prime'])[1 + floor(random() * 8)::int] AS brand,
                ROUND((99 + random() * 49000)::numeric, 2) AS price
            FROM generate_series(1, %s) AS gs
        )
        INSERT INTO products (
            product_name, category, sub_category, brand,
            price, cost_price, stock_quantity, weight_kg,
            is_active, created_at, updated_at
        )
        SELECT
            format('Synthetic Product %%s-%%s', %s, gs),
            category,
            CASE category
                WHEN 'Electronics' THEN 'Gadgets'
                WHEN 'Clothing' THEN 'Apparel'
                WHEN 'Home' THEN 'Household'
                WHEN 'Footwear' THEN 'Shoes'
                WHEN 'Accessories' THEN 'Lifestyle'
                WHEN 'Fitness' THEN 'Workout'
                WHEN 'Grocery' THEN 'Essentials'
                ELSE 'General'
            END,
            brand,
            price,
            ROUND((price * (0.45 + random() * 0.35))::numeric, 2),
            (10 + floor(random() * 3000))::int,
            ROUND((0.2 + random() * 9.8)::numeric, 2),
            TRUE,
            NOW(),
            NOW()
        FROM seeded
        """,
        (rows_to_add, run_token),
    )
    return rows_to_add


def _insert_orders_with_children(cur, rows_to_add, lookback_hours, run_token):
    if rows_to_add <= 0:
        return 0, 0, 0

    cur.execute("CREATE TEMP TABLE tmp_new_orders (order_id INT PRIMARY KEY) ON COMMIT DROP")

    cur.execute(
        """
        WITH customer_ids AS (
            SELECT ARRAY_AGG(customer_id) AS ids FROM customers
        ),
        inserted AS (
            INSERT INTO orders (
                customer_id, order_date, status, total_amount, discount_amount,
                shipping_cost, shipping_address, billing_address, created_at, updated_at
            )
            SELECT
                customer_ids.ids[1 + floor(random() * array_length(customer_ids.ids, 1))::int],
                NOW() - (random() * %s * INTERVAL '1 hour'),
                (ARRAY['delivered','delivered','delivered','shipped','confirmed','pending','cancelled','returned'])[1 + floor(random() * 8)::int],
                0,
                0,
                0,
                format('Synthetic shipping address %%s', gs),
                format('Synthetic billing address %%s', gs),
                NOW(),
                NOW()
            FROM generate_series(1, %s) AS gs
            CROSS JOIN customer_ids
            RETURNING order_id
        )
        INSERT INTO tmp_new_orders (order_id)
        SELECT order_id FROM inserted
        """,
        (lookback_hours, rows_to_add),
    )

    cur.execute(
        """
        WITH product_ids AS (
            SELECT ARRAY_AGG(product_id) AS ids FROM products
        )
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount)
        SELECT
            t.order_id,
            pick.product_id,
            pick.quantity,
            p.price,
            ROUND((p.price * pick.quantity * (random() * 0.15))::numeric, 2)
        FROM tmp_new_orders t
        CROSS JOIN product_ids pid
        JOIN LATERAL generate_series(1, (1 + floor(random() * 3))::int) line(item_no) ON TRUE
        JOIN LATERAL (
            SELECT
                pid.ids[1 + floor(random() * array_length(pid.ids, 1))::int] AS product_id,
                (1 + floor(random() * 4))::int AS quantity
        ) pick ON TRUE
        JOIN products p ON p.product_id = pick.product_id
        """
    )
    order_items_inserted = cur.rowcount

    cur.execute(
        """
        UPDATE orders o
        SET
            total_amount = agg.gross_amount,
            discount_amount = agg.discount_amount,
            shipping_cost = agg.shipping_cost,
            updated_at = NOW()
        FROM (
            SELECT
                oi.order_id,
                ROUND(SUM(oi.unit_price * oi.quantity)::numeric, 2) AS gross_amount,
                ROUND(SUM(oi.discount)::numeric, 2) AS discount_amount,
                ROUND((20 + random() * 180)::numeric, 2) AS shipping_cost
            FROM order_items oi
            INNER JOIN tmp_new_orders t ON t.order_id = oi.order_id
            GROUP BY oi.order_id
        ) agg
        WHERE o.order_id = agg.order_id
        """
    )

    cur.execute(
        """
        INSERT INTO payments (
            order_id, payment_date, payment_method, payment_status, amount, transaction_id
        )
        SELECT
            o.order_id,
            o.order_date + (random() * 6 * INTERVAL '1 hour'),
            (ARRAY['credit_card','debit_card','upi','net_banking','wallet','cod'])[1 + floor(random() * 6)::int],
            CASE
                WHEN o.status IN ('cancelled', 'returned') THEN 'refunded'
                WHEN o.status = 'pending' THEN 'pending'
                ELSE CASE WHEN random() < 0.94 THEN 'completed' ELSE 'failed' END
            END,
            GREATEST(o.total_amount - o.discount_amount + o.shipping_cost, 1),
            format('SYN-%%s-%%s', %s, o.order_id)
        FROM orders o
        INNER JOIN tmp_new_orders t ON t.order_id = o.order_id
        """,
        (run_token,),
    )
    payments_inserted = cur.rowcount

    return rows_to_add, order_items_inserted, payments_inserted


def generate_large_data(
    target_customers=20000,
    target_products=3000,
    target_orders=120000,
    lookback_hours=8760,
    min_order_span_days=90,
):
    """
    Generate large synthetic data volume for the source OLTP database.

    The generation is idempotent against target counts: only missing rows are added.
    """
    run_token = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    with _connect() as conn:
        with conn.cursor() as cur:
            current_customers = _count_rows(cur, "customers")
            current_products = _count_rows(cur, "products")
            current_orders = _count_rows(cur, "orders")
            current_items = _count_rows(cur, "order_items")
            current_payments = _count_rows(cur, "payments")

            add_customers = max(0, int(target_customers) - current_customers)
            add_products = max(0, int(target_products) - current_products)
            add_orders = max(0, int(target_orders) - current_orders)

            inserted_customers = _insert_customers(cur, add_customers, run_token)
            inserted_products = _insert_products(cur, add_products, run_token)

            inserted_orders, inserted_items, inserted_payments = _insert_orders_with_children(
                cur,
                add_orders,
                int(lookback_hours),
                run_token,
            )

            distribution_before = _get_order_date_distribution(cur)
            rebalanced_orders = 0
            rebalanced_payments = 0

            # If orders are concentrated in too few days, rebalance timestamps.
            if (
                distribution_before["distinct_days"] <= 1
                or distribution_before["span_days"] < int(min_order_span_days)
            ):
                rebalanced_orders, rebalanced_payments = _rebalance_existing_order_dates(
                    cur,
                    int(lookback_hours),
                )

            distribution_after = _get_order_date_distribution(cur)

        conn.commit()

    return {
        "inserted": {
            "customers": inserted_customers,
            "products": inserted_products,
            "orders": inserted_orders,
            "order_items": inserted_items,
            "payments": inserted_payments,
        },
        "final_counts": {
            "customers": current_customers + inserted_customers,
            "products": current_products + inserted_products,
            "orders": current_orders + inserted_orders,
            "order_items": current_items + inserted_items,
            "payments": current_payments + inserted_payments,
        },
        "date_distribution": {
            "before": distribution_before,
            "after": distribution_after,
            "rebalanced_orders": rebalanced_orders,
            "rebalanced_payments": rebalanced_payments,
        },
    }


if __name__ == "__main__":
    targets = {
        "target_customers": int(os.environ.get("LARGE_DATA_CUSTOMERS", "20000")),
        "target_products": int(os.environ.get("LARGE_DATA_PRODUCTS", "3000")),
        "target_orders": int(os.environ.get("LARGE_DATA_ORDERS", "120000")),
        "lookback_hours": int(os.environ.get("LARGE_DATA_ORDER_LOOKBACK_HOURS", "8760")),
        "min_order_span_days": int(os.environ.get("LARGE_DATA_MIN_ORDER_SPAN_DAYS", "90")),
    }

    summary = generate_large_data(**targets)
    print("Large data generation complete:")
    print(summary)
