import unittest
from unittest.mock import patch, Mock
import pandas as pd
import os
import tempfile
import logging
from utils.load import save_to_csv, save_to_google_sheets, save_to_postgresql, load_data, LoadError

# Suppress logging during tests
logging.getLogger().setLevel(logging.CRITICAL)

class TestLoad(unittest.TestCase):
    def setUp(self):
        self.sample_df = pd.DataFrame({
            "title": ["Test Product"],
            "price": [99.99],
            "rating": [4.5],
            "colors": [3],
            "size": ["M"],
            "gender": ["Unisex"]
        })
        self.temp_dir = tempfile.mkdtemp()
        self.connection_params = {
            "host": "localhost",
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
            "port": 5432
        }

    def tearDown(self):
        # Clean up temporary directory
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_save_to_csv_success(self):
        file_path = save_to_csv(self.sample_df, self.temp_dir, "test.csv")
        self.assertTrue(os.path.exists(file_path))
        df_read = pd.read_csv(file_path)
        self.assertEqual(len(df_read), 1)
        self.assertEqual(df_read.iloc[0]["title"], "Test Product")

    def test_save_to_csv_empty_df(self):
        with self.assertRaises(LoadError):
            save_to_csv(pd.DataFrame(), self.temp_dir, "test.csv")

    def test_save_to_csv_permission_error(self):
        with patch('pandas.DataFrame.to_csv', side_effect=PermissionError("Permission denied")):
            with self.assertRaises(LoadError):
                save_to_csv(self.sample_df, "/root/test", "test.csv")

    @patch('utils.load.gspread.authorize')
    @patch('utils.load.Credentials.from_service_account_file')
    def test_save_to_google_sheets_success(self, mock_credentials, mock_authorize):
        mock_spreadsheet = Mock()
        mock_worksheet = Mock()
        mock_spreadsheet.worksheet.side_effect = Exception("Worksheet not found")
        mock_spreadsheet.add_worksheet.return_value = mock_worksheet
        mock_client = Mock()
        mock_client.create.return_value = mock_spreadsheet
        mock_client.open_by_key.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client
        mock_credentials.return_value = Mock()

        with patch('utils.load.set_with_dataframe', return_value=None):
            spreadsheet_id = save_to_google_sheets(
                self.sample_df,
                "fake_credentials.json",
                sheet_name="TestSheet",
                create_if_not_exists=True
            )
            self.assertIsInstance(spreadsheet_id, str)

    def test_save_to_google_sheets_empty_df(self):
        with self.assertRaises(LoadError):
            save_to_google_sheets(pd.DataFrame(), "fake_credentials.json")

    def test_save_to_google_sheets_no_credentials(self):
        with self.assertRaises(LoadError):
            save_to_google_sheets(self.sample_df, "nonexistent.json")

    @patch('utils.load.create_engine')
    def test_save_to_postgresql_success(self, mock_engine):
        mock_conn = Mock()
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = None
        with patch('pandas.DataFrame.to_sql', return_value=None):
            result = save_to_postgresql(
                self.sample_df,
                "products",
                self.connection_params,
                if_exists="replace",
                schema="public"
            )
            self.assertTrue(result)

    def test_save_to_postgresql_empty_df(self):
        with self.assertRaises(LoadError):
            save_to_postgresql(pd.DataFrame(), "products", self.connection_params)

    def test_save_to_postgresql_missing_params(self):
        incomplete_params = {"host": "localhost"}
        with self.assertRaises(LoadError):
            save_to_postgresql(self.sample_df, "products", incomplete_params)

    @patch('utils.load.save_to_csv')
    @patch('utils.load.save_to_google_sheets')
    @patch('utils.load.save_to_postgresql')
    def test_load_data_all_destinations(self, mock_postgres, mock_sheets, mock_csv):
        mock_csv.return_value = "/path/to/test.csv"
        mock_sheets.return_value = "spreadsheet_id"
        mock_postgres.return_value = True

        results = load_data(
            self.sample_df,
            save_csv=True,
            save_sheets=True,
            save_postgres=True,
            csv_path=self.temp_dir,
            csv_filename="test.csv",
            sheets_credentials_path="fake_credentials.json",
            sheets_name="TestSheet",
            postgres_params=self.connection_params,
            postgres_table="products"
        )

        self.assertEqual(results["csv_path"], "/path/to/test.csv")
        self.assertEqual(results["sheets_id"], "spreadsheet_id")
        self.assertTrue(results["postgres_success"])

    def test_load_data_no_destination(self):
        with self.assertRaises(ValueError):
            load_data(self.sample_df, save_csv=False, save_sheets=False, save_postgres=False)

    @patch('utils.load.save_to_csv')
    def test_load_data_csv_error(self, mock_csv):
        mock_csv.side_effect = LoadError("CSV error")
        results = load_data(
            self.sample_df,
            save_csv=True,
            csv_path=self.temp_dir,
            csv_filename="test.csv"
        )
        self.assertIsNone(results["csv_path"])
        self.assertEqual(results["csv_error"], "CSV error")

if __name__ == '__main__':
    unittest.main()