"""
Main ETL pipeline runner.
Executes the complete Extract, Transform, Load process.
"""

import os
import pandas as pd
import logging
import argparse
from datetime import datetime
from utils.extract import extract_data
from utils.transform import transform_data
from utils.load import load_data, LoadError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main ETL process."""
    try:
        # Step 1: Extract raw data
        logger.info("Starting data extraction...")
        raw_data = extract_data()
        if raw_data.empty:
            logger.error("Data extraction returned empty DataFrame. Aborting.")
            return False
        logger.info(f"Data extraction completed. {len(raw_data)} records extracted.")
        
        # Step 2: Transform data
        logger.info("Starting data transformation...")
        transformed_data = transform_data(raw_data)
        if transformed_data.empty:
            logger.error("Data transformation returned empty DataFrame. Aborting.")
            return False
        logger.info(f"Data transformation completed. {len(transformed_data)} valid records after transformation.")
        
        # Step 3: Load data to all destinations
        logger.info("Starting data loading to all destinations...")
        
        # Hardcoded configuration parameters
        csv_path = './'
        csv_filename = f"products.csv"
        
        sheets_credentials_path = 'google-sheets-api.json'
        sheets_id = '173byRKN5zsxFwCp3-tL0W4A4t9fYqjIx0CYdVjEJirk'
        sheets_name = 'Products'
        
        postgres_params = {
            'host': 'localhost',
            'database': 'fashion_data',
            'user': 'etl_user',
            'password': 'irfn321',
            'port': 5432
        }
        postgres_table = 'products'
        
        # Execute load operation to all destinations
        results = load_data(
            transformed_data,
            save_csv=True,
            save_sheets=True,
            save_postgres=True,
            csv_path=csv_path,
            csv_filename=csv_filename,
            sheets_credentials_path=sheets_credentials_path,
            sheets_id=sheets_id,
            sheets_name=sheets_name,
            postgres_params=postgres_params,
            postgres_table=postgres_table
        )
        
        # Log results
        if results.get('csv_path'):
            logger.info(f"Data successfully saved to CSV: {results['csv_path']}")
        else:
            logger.error(f"Failed to save to CSV: {results.get('csv_error', 'Unknown error')}")
            
        if results.get('sheets_id'):
            logger.info(f"Data successfully saved to Google Sheets. ID: {results['sheets_id']}")
        else:
            logger.error(f"Failed to save to Google Sheets: {results.get('sheets_error', 'Unknown error')}")
            
        if results.get('postgres_success'):
            logger.info(f"Data successfully saved to PostgreSQL table: {postgres_table}")
        else:
            logger.error(f"Failed to save to PostgreSQL: {results.get('postgres_error', 'Unknown error')}")
        
        logger.info("ETL process completed successfully!")
        
        # Display head of the saved data
        logger.info("Displaying first few rows of the saved data:")
        print(transformed_data.head())

        # Display info of the saved data
        logger.info("Displaying dataframe structure:")
        print(transformed_data.info())
        
        return True
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)