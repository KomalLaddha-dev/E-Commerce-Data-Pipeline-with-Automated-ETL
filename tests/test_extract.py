"""
test_extract.py — Unit Tests for the Extract Module
E-commerce Data Pipeline with Automated ETL

Tests extraction utilities and file-saving logic
using mocked database connections.
"""

import unittest
import pandas as pd
import tempfile
import os
import sys
import shutil
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from scripts.extract import save_raw_data, load_metadata, save_metadata


class TestSaveRawData(unittest.TestCase):
    """Tests for the save_raw_data() function."""

    def setUp(self):
        """Create a temp directory and sample DataFrame."""
        self.temp_dir = tempfile.mkdtemp()
        self.sample_df = pd.DataFrame({
            "order_id": [1001, 1002, 1003],
            "customer_id": [1, 2, 3],
            "total_amount": [2498.00, 5598.00, 15999.00],
        })
        # Patch the RAW_DATA_PATH in the module
        import scripts.extract as ext_module
        self.original_path = ext_module.RAW_DATA_PATH
        ext_module.RAW_DATA_PATH = self.temp_dir

    def tearDown(self):
        """Clean up temp directory and restore path."""
        import scripts.extract as ext_module
        ext_module.RAW_DATA_PATH = self.original_path
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_creates_csv_file(self):
        """save_raw_data should create a CSV file in the staging area."""
        filepath = save_raw_data(self.sample_df, "orders")
        self.assertIsNotNone(filepath)
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(filepath.endswith(".csv"))

    def test_file_contains_correct_rows(self):
        """Saved CSV should contain the same number of rows as the DataFrame."""
        filepath = save_raw_data(self.sample_df, "orders")
        loaded = pd.read_csv(filepath)
        self.assertEqual(len(loaded), len(self.sample_df))

    def test_file_contains_correct_columns(self):
        """Saved CSV should have the same columns as the DataFrame."""
        filepath = save_raw_data(self.sample_df, "orders")
        loaded = pd.read_csv(filepath)
        self.assertListEqual(list(loaded.columns), list(self.sample_df.columns))

    def test_filename_contains_table_name(self):
        """Filename should start with the table name."""
        filepath = save_raw_data(self.sample_df, "orders")
        filename = os.path.basename(filepath)
        self.assertTrue(filename.startswith("orders_"))

    def test_empty_dataframe_returns_none(self):
        """Empty DataFrame should not create a file and return None."""
        empty_df = pd.DataFrame()
        filepath = save_raw_data(empty_df, "orders")
        self.assertIsNone(filepath)


class TestMetadata(unittest.TestCase):
    """Tests for metadata load/save functions."""

    def setUp(self):
        """Create a temp directory for metadata."""
        self.temp_dir = tempfile.mkdtemp()
        import scripts.extract as ext_module
        self.original_path = ext_module.METADATA_PATH
        ext_module.METADATA_PATH = os.path.join(self.temp_dir, "test_state.json")

    def tearDown(self):
        """Clean up."""
        import scripts.extract as ext_module
        ext_module.METADATA_PATH = self.original_path
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_load_empty_metadata(self):
        """Loading non-existent metadata should return empty dict."""
        result = load_metadata()
        self.assertEqual(result, {})

    def test_save_and_load_roundtrip(self):
        """Metadata should survive a save/load roundtrip."""
        test_data = {
            "orders": {
                "last_run": "2025-12-01 02:00:00",
                "rows_extracted": 150,
            }
        }
        save_metadata(test_data)
        loaded = load_metadata()
        self.assertEqual(loaded["orders"]["rows_extracted"], 150)


if __name__ == "__main__":
    unittest.main(verbosity=2)
