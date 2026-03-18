"""
test_quality.py — Data Quality Test Suite
E-commerce Data Pipeline with Automated ETL

Validates data quality rules on processed datasets
before they are loaded into the data warehouse.
Run these checks as a gate before the Load step.
"""

import unittest
import pandas as pd
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

SAMPLE_DATA_PATH = os.path.join(BASE_DIR, "data", "sample")


class TestOrdersQuality(unittest.TestCase):
    """Data quality checks for orders data."""

    @classmethod
    def setUpClass(cls):
        """Load sample orders data."""
        cls.df = pd.read_csv(os.path.join(SAMPLE_DATA_PATH, "orders_sample.csv"))

    def test_no_null_order_ids(self):
        """order_id must not contain nulls."""
        self.assertEqual(self.df["order_id"].isna().sum(), 0)

    def test_unique_order_ids(self):
        """order_id must be unique."""
        self.assertEqual(self.df["order_id"].nunique(), len(self.df))

    def test_no_null_customer_ids(self):
        """customer_id must not contain nulls."""
        self.assertEqual(self.df["customer_id"].isna().sum(), 0)

    def test_no_negative_amounts(self):
        """total_amount must be non-negative."""
        self.assertTrue((self.df["total_amount"] >= 0).all())

    def test_valid_statuses(self):
        """status must be one of the allowed values."""
        allowed = {"pending", "confirmed", "shipped", "delivered", "cancelled", "returned"}
        actual = set(self.df["status"].str.lower().unique())
        self.assertTrue(actual.issubset(allowed), f"Unexpected statuses: {actual - allowed}")

    def test_order_date_not_null(self):
        """order_date must not contain nulls."""
        self.assertEqual(self.df["order_date"].isna().sum(), 0)

    def test_order_date_parseable(self):
        """order_date must be parseable as datetime."""
        parsed = pd.to_datetime(self.df["order_date"], errors="coerce")
        self.assertEqual(parsed.isna().sum(), 0)


class TestCustomersQuality(unittest.TestCase):
    """Data quality checks for customers data."""

    @classmethod
    def setUpClass(cls):
        """Load sample customers data."""
        cls.df = pd.read_csv(os.path.join(SAMPLE_DATA_PATH, "customers_sample.csv"))

    def test_no_null_customer_ids(self):
        """customer_id must not contain nulls."""
        self.assertEqual(self.df["customer_id"].isna().sum(), 0)

    def test_unique_customer_ids(self):
        """customer_id must be unique."""
        self.assertEqual(self.df["customer_id"].nunique(), len(self.df))

    def test_unique_emails(self):
        """email must be unique across customers."""
        self.assertEqual(self.df["email"].nunique(), len(self.df))

    def test_email_format(self):
        """Emails must contain '@' symbol."""
        for email in self.df["email"].dropna():
            self.assertIn("@", email, f"Invalid email format: {email}")

    def test_no_null_names(self):
        """first_name and last_name must not be null."""
        self.assertEqual(self.df["first_name"].isna().sum(), 0)
        self.assertEqual(self.df["last_name"].isna().sum(), 0)


class TestProductsQuality(unittest.TestCase):
    """Data quality checks for products data."""

    @classmethod
    def setUpClass(cls):
        """Load sample products data."""
        cls.df = pd.read_csv(os.path.join(SAMPLE_DATA_PATH, "products_sample.csv"))

    def test_no_null_product_ids(self):
        """product_id must not contain nulls."""
        self.assertEqual(self.df["product_id"].isna().sum(), 0)

    def test_unique_product_ids(self):
        """product_id must be unique."""
        self.assertEqual(self.df["product_id"].nunique(), len(self.df))

    def test_positive_prices(self):
        """price must be positive."""
        self.assertTrue((self.df["price"] > 0).all())

    def test_cost_less_than_price(self):
        """cost_price should be less than or equal to price."""
        valid = self.df.dropna(subset=["cost_price"])
        self.assertTrue((valid["cost_price"] <= valid["price"]).all())

    def test_non_negative_stock(self):
        """stock_quantity must be non-negative."""
        self.assertTrue((self.df["stock_quantity"] >= 0).all())

    def test_product_name_not_empty(self):
        """product_name must not be null or empty."""
        self.assertEqual(self.df["product_name"].isna().sum(), 0)
        self.assertTrue((self.df["product_name"].str.len() > 0).all())


class TestPaymentsQuality(unittest.TestCase):
    """Data quality checks for payments data."""

    @classmethod
    def setUpClass(cls):
        """Load sample payments data."""
        cls.df = pd.read_csv(os.path.join(SAMPLE_DATA_PATH, "payments_sample.csv"))

    def test_no_null_payment_ids(self):
        """payment_id must not contain nulls."""
        self.assertEqual(self.df["payment_id"].isna().sum(), 0)

    def test_unique_payment_ids(self):
        """payment_id must be unique."""
        self.assertEqual(self.df["payment_id"].nunique(), len(self.df))

    def test_positive_amounts(self):
        """amount must be positive."""
        self.assertTrue((self.df["amount"] > 0).all())

    def test_valid_payment_methods(self):
        """payment_method must be one of the allowed values."""
        allowed = {"credit_card", "debit_card", "upi", "net_banking", "wallet", "cod"}
        actual = set(self.df["payment_method"].str.lower().unique())
        self.assertTrue(actual.issubset(allowed), f"Unexpected methods: {actual - allowed}")

    def test_valid_payment_statuses(self):
        """payment_status must be one of the allowed values."""
        allowed = {"pending", "completed", "failed", "refunded"}
        actual = set(self.df["payment_status"].str.lower().unique())
        self.assertTrue(actual.issubset(allowed), f"Unexpected statuses: {actual - allowed}")

    def test_unique_transaction_ids(self):
        """transaction_id must be unique."""
        non_null = self.df["transaction_id"].dropna()
        self.assertEqual(non_null.nunique(), len(non_null))


class TestReferentialIntegrity(unittest.TestCase):
    """Cross-table referential integrity checks."""

    @classmethod
    def setUpClass(cls):
        """Load all sample datasets."""
        cls.orders = pd.read_csv(os.path.join(SAMPLE_DATA_PATH, "orders_sample.csv"))
        cls.customers = pd.read_csv(os.path.join(SAMPLE_DATA_PATH, "customers_sample.csv"))
        cls.products = pd.read_csv(os.path.join(SAMPLE_DATA_PATH, "products_sample.csv"))
        cls.items = pd.read_csv(os.path.join(SAMPLE_DATA_PATH, "order_items_sample.csv"))
        cls.payments = pd.read_csv(os.path.join(SAMPLE_DATA_PATH, "payments_sample.csv"))

    def test_orders_customer_ids_exist(self):
        """All customer_ids in orders must exist in customers table."""
        valid_ids = set(self.customers["customer_id"])
        order_ids = set(self.orders["customer_id"])
        orphans = order_ids - valid_ids
        self.assertEqual(len(orphans), 0, f"Orphan customer_ids in orders: {orphans}")

    def test_order_items_order_ids_exist(self):
        """All order_ids in order_items must exist in orders table."""
        valid_ids = set(self.orders["order_id"])
        item_ids = set(self.items["order_id"])
        orphans = item_ids - valid_ids
        self.assertEqual(len(orphans), 0, f"Orphan order_ids in items: {orphans}")

    def test_order_items_product_ids_exist(self):
        """All product_ids in order_items must exist in products table."""
        valid_ids = set(self.products["product_id"])
        item_ids = set(self.items["product_id"])
        orphans = item_ids - valid_ids
        self.assertEqual(len(orphans), 0, f"Orphan product_ids in items: {orphans}")

    def test_payments_order_ids_exist(self):
        """All order_ids in payments must exist in orders table."""
        valid_ids = set(self.orders["order_id"])
        payment_ids = set(self.payments["order_id"])
        orphans = payment_ids - valid_ids
        self.assertEqual(len(orphans), 0, f"Orphan order_ids in payments: {orphans}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
