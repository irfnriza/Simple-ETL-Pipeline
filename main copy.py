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
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run ETL pipeline for fashion data')
    parser.add_argument('--csv', action='store_true', help='Save data to CSV')
    parser.add_argument('--sheets', action='store_true', help='Save data to Google Sheets')
    parser.add_argument('--postgres', action='store_true', help='Save data to PostgreSQL')
    parser.add_argument('--csv-path', default='./data', help='Path to save CSV file')
    parser.add_argument('--sheets-creds', help='Path to Google Sheets credentials JSON')
    parser.add_argument('--sheets-id', help='Google Sheets ID (optional)')
    parser.add_argument('--sheets-name', default='Products', help='Google Sheets worksheet name')
    parser.add_argument('--pg-host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--pg-db', default='fashion_data', help='PostgreSQL database name')
    parser.add_argument('--pg-user', help='PostgreSQL username')
    parser.add_argument('--pg-pass', help='PostgreSQL password')
    parser.add_argument('--pg-port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--pg-table', default='products', help='PostgreSQL table name')
    
    args = parser.parse_args()
    
     # Validate at least one destination is selected
    if not any([args.csv, args.sheets, args.postgres]):
        args.csv = True  # Default to CSV if none specified
        logger.info("No destination specified, defaulting to CSV")
    
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
        
        # Step 3: Load data to chosen destinations
        logger.info("Starting data loading...")
        
        # Prepare PostgreSQL parameters if needed
        postgres_params = None
        if args.postgres:
            if not all([args.pg_user, args.pg_pass]):
                logger.error("PostgreSQL username and password are required")
                return False
                
            postgres_params = {
                'host': args.pg_host,
                'database': args.pg_db,
                'user': args.pg_user,
                'password': args.pg_pass,
                'port': args.pg_port
            }
        
        # Execute load operation
        results = load_data(
            transformed_data,
            save_csv=args.csv,
            save_sheets=args.sheets,
            save_postgres=args.postgres,
            csv_path=args.csv_path,
            csv_filename=f"products_{datetime.now().strftime('%Y%m%d')}.csv",
            sheets_credentials_path=args.sheets_creds,
            sheets_id=args.sheets_id,
            sheets_name=args.sheets_name,  # Added parameter
            postgres_params=postgres_params,
            postgres_table=args.pg_table
        )
        
        # Log results
        if args.csv and results.get('csv_path'):
            logger.info(f"Data successfully saved to CSV: {results['csv_path']}")
        elif args.csv:
            logger.error(f"Failed to save to CSV: {results.get('csv_error', 'Unknown error')}")
            
        if args.sheets and results.get('sheets_id'):
            logger.info(f"Data successfully saved to Google Sheets. ID: {results['sheets_id']}")
        elif args.sheets:
            logger.error(f"Failed to save to Google Sheets: {results.get('sheets_error', 'Unknown error')}")
            
        if args.postgres and results.get('postgres_success'):
            logger.info(f"Data successfully saved to PostgreSQL table: {args.pg_table}")
        elif args.postgres:
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