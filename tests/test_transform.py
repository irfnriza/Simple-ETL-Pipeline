import unittest
import pandas as pd
import numpy as np
from utils.transform import (
    clean_price, clean_rating, clean_colors, clean_size, clean_gender,
    remove_dirty_data, transform_data
)
import logging

# Suppress logging during tests
logging.getLogger().setLevel(logging.CRITICAL)

class TestTransform(unittest.TestCase):
    def setUp(self):
        self.sample_df = pd.DataFrame({
            "title": ["Test Product", "Unknown Product", "Valid Product"],
            "price": ["$99.99", "N/A", "100,50"],
            "rating": ["4.5 / 5", "N/A", "3.2 out of 5"],
            "colors": ["3 Colors", "Unknown Colors", "2 Colors"],
            "size": ["Size: M", "", "Size: L"],
            "gender": ["Gender: Unisex", "", "Gender: Male"],
            "timestamp": ["2023-10-01", "2023-10-01", "2023-10-01"]
        })

    def test_clean_price_valid(self):
        self.assertEqual(clean_price("$99.99"), 99.99 * 16000)
        self.assertEqual(clean_price("100,50"), 100.50 * 16000)
        self.assertEqual(clean_price("1,000.50"), 1000.50 * 16000)

    def test_clean_price_invalid(self):
        self.assertIsNone(clean_price("N/A"))
        self.assertIsNone(clean_price(""))
        self.assertIsNone(clean_price(None))
        self.assertIsNone(clean_price("Invalid"))

    def test_clean_rating_valid(self):
        self.assertEqual(clean_rating("4.5 / 5"), 4.5)
        self.assertEqual(clean_rating("3.2 out of 5"), 3.2)
        self.assertEqual(clean_rating("тнР4.5"), 4.5)

    def test_clean_rating_invalid(self):
        self.assertIsNone(clean_rating("N/A"))
        self.assertIsNone(clean_rating(""))
        self.assertIsNone(clean_rating(None))
        self.assertIsNone(clean_rating("Invalid"))

    def test_clean_colors_valid(self):
        self.assertEqual(clean_colors("3 Colors"), 3)
        self.assertEqual(clean_colors("2 Colors Available"), 2)

    def test_clean_colors_invalid(self):
        self.assertIsNone(clean_colors("Unknown Colors"))
        self.assertIsNone(clean_colors(""))
        self.assertIsNone(clean_colors(None))

    def test_clean_size_valid(self):
        self.assertEqual(clean_size("Size: M"), "M")
        self.assertEqual(clean_size("Size: Large"), "Large")

    def test_clean_size_invalid(self):
        self.assertIsNone(clean_size(""))
        self.assertIsNone(clean_size(None))
        self.assertIsNone(clean_size(123))  # Non-string input

    def test_clean_gender_valid(self):
        self.assertEqual(clean_gender("Gender: Unisex"), "Unisex")
        self.assertEqual(clean_gender("Gender: Male"), "Male")

    def test_clean_gender_invalid(self):
        self.assertIsNone(clean_gender(""))
        self.assertIsNone(clean_gender(None))
        self.assertIsNone(clean_gender(123))  # Non-string input

    def test_remove_dirty_data(self):
        df = remove_dirty_data(self.sample_df)
        self.assertEqual(len(df), 1)  # Only "Valid Product" should remain
        self.assertEqual(df.iloc[0]["title"], "Valid Product")

    def test_remove_dirty_data_empty_df(self):
        df = pd.DataFrame()
        result = remove_dirty_data(df)
        self.assertTrue(result.empty)

    def test_transform_data(self):
        df = transform_data(self.sample_df)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["title"], "Valid Product")
        self.assertEqual(df.iloc[0]["price"], 100.50 * 16000)
        self.assertEqual(df.iloc[0]["rating"], 3.2)
        self.assertEqual(df.iloc[0]["colors"], 2)
        self.assertEqual(df.iloc[0]["size"], "L")
        self.assertEqual(df.iloc[0]["gender"], "Male")

    def test_transform_data_empty_df(self):
        df = pd.DataFrame()
        result = transform_data(df)
        self.assertTrue(result.empty)

    def test_transform_data_missing_columns(self):
        df = pd.DataFrame({"title": ["Test Product"]})
        result = transform_data(df)
        self.assertTrue(result.empty)  # Should drop due to missing required columns

if __name__ == '__main__':
    unittest.main()