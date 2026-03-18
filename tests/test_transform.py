"""
test_transform.py — Unit Tests for the Transform Module
E-commerce Data Pipeline with Automated ETL

Tests data cleaning, normalization, and aggregation logic
using sample data to validate transformation correctness.
"""

import unittest
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from scripts.transform import (
    clean_orders,
    clean_customers,
    clean_products,
    clean_payments,
    build_date_dimension,
)


class TestCleanOrders(unittest.TestCase):
    """Tests for the clean_orders() function."""

    def setUp(self):
        """Create sample order data for testing."""
        self.sample_orders = pd.DataFrame({
            "order_id": [1001, 1002, 1003, 1001, 1004],  # 1001 is duplicated
            "customer_id": [1, 2, 3, 1, 4],
            "order_date": [
                "2025-12-01 10:30:00",
                "2025-12-02 11:15:00",
                "2025-12-03 09:00:00",
                "2025-12-01 10:30:00",         # duplicate
                "2025-12-04 14:22:00",
            ],
            "status": ["delivered", "CONFIRMED", "Shipped", "delivered", "Cancelled"],
            "total_amount": [2498.00, 5598.00, 15999.00, 2498.00, 449.00],
            "discount_amount": [200.00, None, 0.00, 200.00, 0.00],
            "shipping_cost": [49.00, 0.00, None, 49.00, 49.00],
        })

    def test_removes_duplicates(self):
        """Duplicate order_ids should be removed (keep last)."""
        result = clean_orders(self.sample_orders)
        self.assertEqual(result["order_id"].nunique(), len(result))

    def test_standardizes_status(self):
        """All status values should be lowercase."""
        result = clean_orders(self.sample_orders)
        for status in result["status"]:
            self.assertEqual(status, status.lower())

    def test_fills_missing_discount(self):
        """Null discount_amount should be filled with 0."""
        result = clean_orders(self.sample_orders)
        self.assertFalse(result["discount_amount"].isna().any())

    def test_fills_missing_shipping(self):
        """Null shipping_cost should be filled with 0."""
        result = clean_orders(self.sample_orders)
        self.assertFalse(result["shipping_cost"].isna().any())

    def test_calculates_net_amount(self):
        """net_amount should equal total - discount + shipping."""
        result = clean_orders(self.sample_orders)
        for _, row in result.iterrows():
            expected = row["total_amount"] - row["discount_amount"] + row["shipping_cost"]
            self.assertAlmostEqual(row["net_amount"], expected, places=2)

    def test_rejects_negative_amounts(self):
        """Orders with negative total_amount should be removed."""
        bad_orders = self.sample_orders.copy()
        bad_orders.loc[0, "total_amount"] = -100.00
        result = clean_orders(bad_orders)
        self.assertTrue((result["total_amount"] >= 0).all())

    def test_parses_dates(self):
        """order_date should be parsed to datetime."""
        result = clean_orders(self.sample_orders)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["order_date"]))


class TestCleanCustomers(unittest.TestCase):
    """Tests for the clean_customers() function."""

    def setUp(self):
        """Create sample customer data for testing."""
        self.sample_customers = pd.DataFrame({
            "customer_id": [1, 2, 3, 4],
            "first_name": ["  rahul  ", "PRIYA", "amit", "Sneha"],
            "last_name": ["  sharma  ", "PATEL", "kumar", "Reddy"],
            "email": ["Rahul@Email.COM", "priya@email.com", "AMIT@email.com", "sneha@email.com"],
            "phone": ["9876543210", None, "9876543212", "9876543213"],
            "city": ["Mumbai", None, "Delhi", "Hyderabad"],
            "state": ["Maharashtra", None, "Delhi", "Telangana"],
            "country": ["India", "IN", "usa", "UK"],
            "registration_date": [
                "2025-01-15", "2025-02-20", "2025-03-10", "2025-04-05"
            ],
        })

    def test_title_case_names(self):
        """Names should be converted to Title Case regardless of row order."""
        result = clean_customers(self.sample_customers)
        for name in result["first_name"]:
            self.assertEqual(name, name.strip().title(), f"Name not Title Case: {name}")
        for name in result["last_name"]:
            self.assertEqual(name, name.strip().title(), f"Name not Title Case: {name}")

    def test_lowercase_emails(self):
        """Emails should be lowercase and trimmed."""
        result = clean_customers(self.sample_customers)
        for email in result["email"]:
            self.assertEqual(email, email.lower())
            self.assertEqual(email, email.strip())

    def test_fills_missing_phone(self):
        """Null phone numbers should be filled with 'N/A'."""
        result = clean_customers(self.sample_customers)
        self.assertFalse(result["phone"].isna().any())

    def test_standardizes_country(self):
        """Country abbreviations should be expanded."""
        result = clean_customers(self.sample_customers)
        countries = result["country"].tolist()
        self.assertIn("India", countries)
        self.assertNotIn("IN", countries)

    def test_deduplicates_by_email(self):
        """Duplicate emails should be removed."""
        duped = self.sample_customers.copy()
        duped = pd.concat([duped, duped.iloc[[0]]], ignore_index=True)
        result = clean_customers(duped)
        self.assertEqual(result["email"].nunique(), len(result))


class TestCleanProducts(unittest.TestCase):
    """Tests for the clean_products() function."""

    def setUp(self):
        """Create sample product data for testing."""
        self.sample_products = pd.DataFrame({
            "product_id": [1, 2, 3, 4],
            "product_name": ["Wireless Earbuds", "T-Shirt", "Water Bottle", "Running Shoes"],
            "category": ["  electronics  ", "CLOTHING", "home", "Footwear"],
            "sub_category": ["Audio", "Men", "Kitchen", "Sports"],
            "brand": ["  boat  ", "H&M", "MILTON", "Nike"],
            "price": [1299.00, 599.00, 449.00, 4999.00],
            "cost_price": [650.00, 200.00, 180.00, 2200.00],
            "is_active": [True, True, True, True],
        })

    def test_title_case_categories(self):
        """Categories should be Title Case."""
        result = clean_products(self.sample_products)
        for cat in result["category"]:
            self.assertEqual(cat, cat.strip().title())

    def test_calculates_profit_margin(self):
        """Profit margin should be calculated correctly."""
        result = clean_products(self.sample_products)
        for _, row in result.iterrows():
            expected = round((row["price"] - row["cost_price"]) / row["price"] * 100, 2)
            self.assertAlmostEqual(row["profit_margin"], expected, places=2)

    def test_rejects_zero_price(self):
        """Products with zero or negative price should be removed."""
        bad_products = self.sample_products.copy()
        bad_products.loc[0, "price"] = 0
        result = clean_products(bad_products)
        self.assertTrue((result["price"] > 0).all())

    def test_removes_duplicates(self):
        """Duplicate product_ids should be removed."""
        duped = pd.concat([self.sample_products, self.sample_products.iloc[[0]]], ignore_index=True)
        result = clean_products(duped)
        self.assertEqual(result["product_id"].nunique(), len(result))


class TestCleanPayments(unittest.TestCase):
    """Tests for the clean_payments() function."""

    def setUp(self):
        """Create sample payment data for testing."""
        self.sample_payments = pd.DataFrame({
            "payment_id": [1, 2, 3, 4],
            "order_id": [1001, 1002, 1003, 1004],
            "payment_date": [
                "2025-12-01 10:31:00",
                "2025-12-02 11:16:00",
                "2025-12-03 09:01:00",
                "2025-12-04 14:23:00",
            ],
            "payment_method": ["  UPI  ", "Credit_Card", "NET_BANKING", "upi"],
            "payment_status": ["completed", "completed", "completed", "refunded"],
            "amount": [2347.00, 5098.00, 15999.00, 498.00],
        })

    def test_lowercase_payment_method(self):
        """Payment methods should be lowercase and trimmed."""
        result = clean_payments(self.sample_payments)
        for method in result["payment_method"]:
            self.assertEqual(method, method.lower().strip())

    def test_parses_dates(self):
        """payment_date should be parsed to datetime."""
        result = clean_payments(self.sample_payments)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["payment_date"]))

    def test_removes_duplicates(self):
        """Duplicate payment_ids should be removed."""
        duped = pd.concat([self.sample_payments, self.sample_payments.iloc[[0]]], ignore_index=True)
        result = clean_payments(duped)
        self.assertEqual(result["payment_id"].nunique(), len(result))


class TestDateDimension(unittest.TestCase):
    """Tests for the build_date_dimension() function."""

    def test_correct_date_range(self):
        """Date dimension should cover the full range."""
        date_dim = build_date_dimension("2025-01-01", "2025-12-31")
        self.assertEqual(len(date_dim), 365)

    def test_date_key_format(self):
        """Date keys should be YYYYMMDD integers."""
        date_dim = build_date_dimension("2025-01-01", "2025-01-03")
        expected_keys = [20250101, 20250102, 20250103]
        self.assertListEqual(date_dim["date_key"].tolist(), expected_keys)

    def test_weekend_flag(self):
        """Weekend flag should be True for Saturday/Sunday."""
        date_dim = build_date_dimension("2025-12-06", "2025-12-07")  # Sat-Sun
        self.assertTrue(date_dim["is_weekend"].all())

    def test_weekday_flag(self):
        """Weekend flag should be False for Monday-Friday."""
        date_dim = build_date_dimension("2025-12-01", "2025-12-01")  # Monday
        self.assertFalse(date_dim["is_weekend"].iloc[0])

    def test_quarter_assignment(self):
        """Quarter should be correctly assigned."""
        date_dim = build_date_dimension("2025-01-01", "2025-12-31")
        jan_rows = date_dim[date_dim["month"] == 1]
        self.assertTrue((jan_rows["quarter"] == 1).all())
        oct_rows = date_dim[date_dim["month"] == 10]
        self.assertTrue((oct_rows["quarter"] == 4).all())

    def test_has_all_columns(self):
        """Date dimension should have all required columns."""
        date_dim = build_date_dimension("2025-01-01", "2025-01-01")
        required_cols = [
            "date_key", "full_date", "day", "month", "year",
            "quarter", "day_of_week", "day_name", "month_name", "is_weekend"
        ]
        for col in required_cols:
            self.assertIn(col, date_dim.columns)


if __name__ == "__main__":
    unittest.main(verbosity=2)
