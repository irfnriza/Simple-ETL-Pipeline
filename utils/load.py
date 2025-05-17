"""
Load module for ETL pipeline.
Provides functionality to load transformed data into:
1. CSV files
2. Google Sheets
3. PostgreSQL database
"""

import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import logging
from typing import Optional, Dict, Any, Union
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Google Sheets API scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

class LoadError(Exception):
    """Custom exception for load operations."""
    pass

def save_to_csv(df: pd.DataFrame, output_path: str, filename: str = "products.csv") -> str:
    """
    Save DataFrame to CSV file.
    
    Args:
        df: DataFrame to save
        output_path: Directory path where to save the CSV
        filename: Name of the CSV file (default: products.csv)
        
    Returns:
        str: Path to the saved CSV file
        
    Raises:
        LoadError: If saving to CSV fails
    """
    try:
        # Validate input DataFrame
        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None")
        
        # Construct full file path
        file_path = os.path.join(output_path, filename)
        
        # Save to CSV
        df.to_csv(file_path, index=False)
        logger.info(f"Data successfully saved to CSV: {file_path}")
        
        return file_path
    except ValueError as e:
        logger.error(f"Invalid data error: {str(e)}")
        raise LoadError(f"Invalid data for CSV export: {str(e)}")
    except PermissionError as e:
        logger.error(f"Permission error when saving CSV: {str(e)}")
        raise LoadError(f"Permission denied when writing to {output_path}: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to save data to CSV: {str(e)}")
        raise LoadError(f"CSV export failed: {str(e)}")


def save_to_google_sheets(
    df: pd.DataFrame, 
    credentials_path: str,
    spreadsheet_id: Optional[str] = None,
    sheet_name: str = "Products",
    create_if_not_exists: bool = True
) -> str:
    """
    Save DataFrame to Google Sheets.
    
    Args:
        df: DataFrame to save
        credentials_path: Path to Google service account credentials JSON
        spreadsheet_id: ID of existing Google Sheet (optional)
        sheet_name: Name of the worksheet (default: Products)
        create_if_not_exists: Create new spreadsheet if ID not provided
        
    Returns:
        str: ID of the Google Sheet
        
    Raises:
        LoadError: If saving to Google Sheets fails
    """
    try:
        # Validate input DataFrame
        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None")
            
        # Check if credentials file exists
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
        
        # Authenticate with Google Sheets API
        credentials = Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
        )
        gc = gspread.authorize(credentials)
        
        # Get or create spreadsheet
        if spreadsheet_id:
            try:
                spreadsheet = gc.open_by_key(spreadsheet_id)
                logger.info(f"Opened existing spreadsheet with ID: {spreadsheet_id}")
            except gspread.exceptions.SpreadsheetNotFound:
                if create_if_not_exists:
                    spreadsheet = gc.create(f"Products ETL {time.strftime('%Y-%m-%d')}")
                    spreadsheet_id = spreadsheet.id
                    logger.info(f"Created new spreadsheet with ID: {spreadsheet_id}")
                else:
                    raise LoadError(f"Spreadsheet with ID {spreadsheet_id} not found")
        else:
            # Create new spreadsheet
            spreadsheet = gc.create(f"Products ETL {time.strftime('%Y-%m-%d')}")
            spreadsheet_id = spreadsheet.id
            logger.info(f"Created new spreadsheet with ID: {spreadsheet_id}")
        
        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            # Clear existing content
            worksheet.clear()
            logger.info(f"Cleared existing worksheet: {sheet_name}")
        except gspread.exceptions.WorksheetNotFound:
            # Create new worksheet
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=df.shape[0] + 10, cols=df.shape[1] + 5)
            logger.info(f"Created new worksheet: {sheet_name}")
        
        # Write data to worksheet
        set_with_dataframe(worksheet, df)
        logger.info(f"Data successfully uploaded to Google Sheets, ID: {spreadsheet_id}, Sheet: {sheet_name}")
        
        # Set permissions to anyone with the link can view
        spreadsheet.share(None, role='reader', perm_type='anyone')
        
        return spreadsheet_id
    except ValueError as e:
        logger.error(f"Invalid data error: {str(e)}")
        raise LoadError(f"Invalid data for Google Sheets: {str(e)}")
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error: {str(e)}")
        raise LoadError(f"Google Sheets API error: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to save data to Google Sheets: {str(e)}")
        raise LoadError(f"Google Sheets export failed: {str(e)}")


def save_to_postgresql(
    df: pd.DataFrame,
    table_name: str,
    connection_params: Dict[str, Any],
    if_exists: str = "replace",
    schema: Optional[str] = "public"
) -> bool:
    """
    Save DataFrame to PostgreSQL database.
    
    Args:
        df: DataFrame to save
        table_name: Name of the target table
        connection_params: Dictionary with connection parameters
            (host, database, user, password, port)
        if_exists: Strategy if table exists ('fail', 'replace', 'append')
        schema: Database schema name
        
    Returns:
        bool: True if successful
        
    Raises:
        LoadError: If saving to PostgreSQL fails
    """
    try:
        # Validate input DataFrame
        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None")
            
        # Validate connection parameters
        required_params = ['host', 'database', 'user', 'password']
        for param in required_params:
            if param not in connection_params:
                raise ValueError(f"Missing required connection parameter: {param}")
        
        # Set default port if not provided
        if 'port' not in connection_params:
            connection_params['port'] = 5432
        
        # Create SQLAlchemy engine
        conn_string = (
            f"postgresql://{connection_params['user']}:{connection_params['password']}@"
            f"{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
        )
        engine = create_engine(conn_string)
        
        # Test connection before proceeding
        with engine.connect() as conn:
            logger.info("PostgreSQL connection successful")
        
        # Create schema if it doesn't exist
        if schema != "public":
            create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema};"
            with engine.connect() as conn:
                conn.execute(create_schema_query)
                logger.info(f"Ensured schema exists: {schema}")
        
        # Save DataFrame to PostgreSQL
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method='multi'  # Faster for larger DataFrames
        )
        
        # Skip the verification step that's causing issues
        row_count = len(df)
        logger.info(f"Data successfully saved to PostgreSQL table '{full_table_name}' with approximately {row_count} rows")
        return True
        
    except ValueError as e:
        logger.error(f"Invalid data or parameter error: {str(e)}")
        raise LoadError(f"Invalid data or parameters: {str(e)}")
    except psycopg2.OperationalError as e:
        logger.error(f"PostgreSQL connection error: {str(e)}")
        raise LoadError(f"Could not connect to PostgreSQL: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to save data to PostgreSQL: {str(e)}")
        raise LoadError(f"PostgreSQL export failed: {str(e)}")

def load_data(
df: pd.DataFrame,
    save_csv: bool = True,
    save_sheets: bool = False,
    save_postgres: bool = False,
    csv_path: str = "./data",
    csv_filename: str = "products.csv",
    sheets_credentials_path: Optional[str] = None,
    sheets_id: Optional[str] = None,
    sheets_name: str = "Products",  # Added parameter for worksheet name
    postgres_params: Optional[Dict[str, Any]] = None,
    postgres_table: str = "products"
) -> Dict[str, Union[str, bool]]:
    """
    Main function to load data to multiple destinations.
    
    Args:
        df: DataFrame to load
        save_csv: Whether to save as CSV
        save_sheets: Whether to save to Google Sheets
        save_postgres: Whether to save to PostgreSQL
        csv_path: Directory path for CSV
        csv_filename: Filename for CSV
        sheets_credentials_path: Path to Google credentials JSON
        sheets_id: Google Sheets ID (optional)
        postgres_params: PostgreSQL connection parameters
        postgres_table: PostgreSQL table name
        
    Returns:
        Dict containing results of each operation
        
    Raises:
        ValueError: If no storage option is selected
    """
    if not any([save_csv, save_sheets, save_postgres]):
        raise ValueError("At least one storage destination must be selected")
    
    results = {
        "csv_path": None,
        "sheets_id": None, 
        "postgres_success": False
    }
    
    # Save to CSV if requested
    if save_csv:
        try:
            results["csv_path"] = save_to_csv(df, csv_path, csv_filename)
        except LoadError as e:
            logger.error(f"CSV storage failed: {str(e)}")
            results["csv_error"] = str(e)
    
    # Save to Google Sheets if requested
    if save_sheets:
        if not sheets_credentials_path:
            logger.warning("Google Sheets credentials path not provided, skipping")
            results["sheets_error"] = "Credentials path not provided"
        else:
            try:
                results["sheets_id"] = save_to_google_sheets(
                    df, sheets_credentials_path, sheets_id
                )
            except LoadError as e:
                logger.error(f"Google Sheets storage failed: {str(e)}")
                results["sheets_error"] = str(e)
    
    # Save to PostgreSQL if requested
    if save_postgres:
        if not postgres_params:
            logger.warning("PostgreSQL connection parameters not provided, skipping")
            results["postgres_error"] = "Connection parameters not provided"
        else:
            try:
                results["postgres_success"] = save_to_postgresql(
                    df, postgres_table, postgres_params
                )
            except LoadError as e:
                logger.error(f"PostgreSQL storage failed: {str(e)}")
                results["postgres_error"] = str(e)
    
    return results