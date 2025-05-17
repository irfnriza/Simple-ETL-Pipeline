"""
Transform module for ETL pipeline.
Provides functionality to transform and clean extracted data.
"""

import pandas as pd
import re
from typing import Optional, Union
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurable settings
DIRTY_PATTERNS = {
    "title": ["Unknown Product", "N/A", ""],
    "rating": ["Invalid Rating / 5", "Not Rated", "N/A", ""],
    "price": ["Price Unavailable", "N/A", "", None]
}

CURRENCY_CONVERSION = 16000  # USD to IDR

def clean_price(price_str: Optional[str]) -> Optional[float]:
    """Clean price string and convert to numeric value.
    Handles: $100.50, 100,50, '1,000.50', etc.
    Returns None for invalid inputs."""
    if pd.isna(price_str) or not str(price_str).strip():
        return None
        
    try:
        # Remove all non-digit characters except dots and commas
        cleaned = re.sub(r"[^\d.,]", "", str(price_str))
        # Replace comma with dot if used as decimal separator
        if ',' in cleaned and '.' not in cleaned:
            cleaned = cleaned.replace(',', '.')
        # Remove thousand separators
        cleaned = cleaned.replace(',', '')
        return float(cleaned) * CURRENCY_CONVERSION
    except (ValueError, TypeError, AttributeError):
        return None

def clean_rating(rating_str: Optional[str]) -> Optional[float]:
    """Extract numeric rating from string.
    Handles: '4.8 / 5', '⭐4.5', '3.2 out of 5', etc."""
    if pd.isna(rating_str) or not str(rating_str).strip():
        return None
        
    try:
        # Match first occurring number with optional decimal
        match = re.search(r"(\d+(?:\.\d+)?)", str(rating_str))
        return float(match.group(1)) if match else None
    except (ValueError, TypeError, AttributeError):
        return None

def clean_colors(colors_str: Optional[str]) -> int:
    """
    Mengekstrak jumlah warna dari string seperti "X Colors".
    """
    try:
        if not colors_str or colors_str == "Unknown Colors":
            return None

        # Mengekstrak angka dari string menggunakan regex
        color_match = re.search(r'(\d+)', colors_str)
        if color_match:
            return int(color_match.group(1))

        return None

    except Exception as e:
        logger.error(f"Error saat mengekstrak warna dari {colors_str}: {str(e)}")
        return None

def clean_size(size_str: Optional[Union[str, int, float]]) -> Optional[str]:
    """Clean size string. Returns None for non-string inputs."""
    if pd.isna(size_str) or not str(size_str).strip():
        return None
        
    try:
        # Return None if input is not string
        if not isinstance(size_str, str):
            return None
            
        cleaned = re.sub(r"^Size:\s*", "", size_str, flags=re.IGNORECASE).strip()
        return cleaned if cleaned else None
    except (ValueError, TypeError, AttributeError):
        return None

def clean_gender(gender_str: Optional[Union[str, int, float]]) -> Optional[str]:
    """Clean gender string. Returns None for non-string inputs."""
    if pd.isna(gender_str) or not str(gender_str).strip():
        return None
        
    try:
        # Return None if input is not string
        if not isinstance(gender_str, str):
            return None
            
        cleaned = re.sub(r"^Gender:\s*", "", gender_str, flags=re.IGNORECASE).strip()
        return cleaned if cleaned else None
    except (ValueError, TypeError, AttributeError):
        return None
    
def remove_dirty_data(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows matching dirty patterns."""
    if df.empty:
        return df
        
    df_clean = df.copy()
    for column, patterns in DIRTY_PATTERNS.items():
        if column in df_clean.columns:
            # Create mask for valid rows
            mask = ~df_clean[column].isin(patterns)
            # Handle NaN/None separately
            mask &= df_clean[column].notna()
            df_clean = df_clean[mask]
    return df_clean

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Main transformation pipeline."""
    if df.empty:
        logger.warning("Input DataFrame is empty, returning empty DataFrame")
        return pd.DataFrame()
        
    try:
        logger.info(f"Starting transformation with {len(df)} records")
        logger.info(f"Columns in input DataFrame: {', '.join(df.columns)}")
        
        # Create a copy to avoid SettingWithCopyWarning
        df_transformed = df.copy()
        
        # Step 1: Remove dirty data
        df_transformed = remove_dirty_data(df_transformed)
        logger.info(f"After removing dirty data: {len(df_transformed)} records remaining")
        
        # Step 2: Clean all columns - use lowercase column names for consistency
        # Track null values before and after each cleaning step
        logger.info("Starting column cleaning process...")
        
        # Price cleaning
        df_transformed["price"] = df_transformed["price"].apply(clean_price)
        
        # Rating cleaning
        df_transformed["rating"] = df_transformed["rating"].apply(clean_rating)
        
        # Colors cleaning
        df_transformed["colors"] = df_transformed["colors"].apply(clean_colors)
        
        # Size cleaning
        df_transformed["size"] = df_transformed["size"].apply(clean_size)
        
        # Gender cleaning
        df_transformed["gender"] = df_transformed["gender"].apply(clean_gender)

        # Step 3: Drop rows with any null values in the cleaned columns
        clean_columns = ["price", "rating", "colors", "size", "gender"]
        
        df_final = df_transformed.dropna(subset=clean_columns)
        
        logger.info(f"Final transformed DataFrame has {len(df_final)} records")
        logger.info(f"Rows dropped due to nulls: {len(df_transformed) - len(df_final)}")
        
        # Reset index after filtering
        return df_final.reset_index(drop=True)
        
    except Exception as e:
        logger.error(f"Transformation error: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return pd.DataFrame()