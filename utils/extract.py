"""
Extract module for ETL pipeline.
Provides functionality to extract data from fashion studio website.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://fashion-studio.dicoding.dev"
TOTAL_PAGES = 50


def get_page_content(url):
    """Get content from URL with error handling."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch page {url}: {e}")
        return None


def parse_product_card(card):
    """Parse a product card from the HTML and extract details."""
    try:
        # Make sure card exists
        if not card:
            logger.warning("Empty product card received")
            return None

        # Find title with better error handling
        title_elem = card.find("h3", class_="product-title")
        if not title_elem:
            logger.warning("No title found in product card")
            return None
        title = title_elem.text.strip()

        # Find price with error handling
        price_elem = card.find("span", class_="price")
        if not price_elem:
            logger.warning(f"No price found for product: {title}")
            price = "N/A"
        else:
            price = price_elem.text.strip()

        # Extract details with safer approach
        details = card.find_all("p")
        rating = "N/A"
        colors = "N/A"
        size = "N/A"
        gender = "N/A"

        for detail in details:
            text = detail.text.strip()
            if "Rating:" in text:
                rating = text.replace("Rating:", "").strip()
            elif 'Colors' in text:
                colors = text
            elif "Size:" in text:
                size = text.replace("Size:", "").strip()
            elif "Gender:" in text:
                gender = text.replace("Gender:", "").strip()

        return {
            "title": title,
            "price": price,
            "rating": rating,
            "colors": colors,
            "size": size,
            "gender": gender,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to parse product card: {e}")
        return None


def extract_data():
    """
    Main extraction function that scrapes product data from the website.

    Returns:
        pd.DataFrame: DataFrame containing product information
    """
    all_products = []

    for page in range(1, TOTAL_PAGES + 1):
        url = f"{BASE_URL}/page{page}" if page > 1 else BASE_URL
        logger.info(f"Fetching data from {url}")

        html_content = get_page_content(url)
        if html_content is None:
            continue

        try:
            soup = BeautifulSoup(html_content, "html.parser")
            # Try different selectors if the original doesn't work
            product_cards = soup.select(
                "div.collection-card, div.product-card, div.item-card")

            if not product_cards:
                logger.warning(
                    f"No product cards found on page {page}. Trying alternative selectors.")
                # Try more general approach
                product_cards = soup.select(
                    "div[class*='product'], div[class*='collection'], div[class*='item']")

            logger.info(f"Found {len(product_cards)} products on page {page}")

            for card in product_cards:
                product_data = parse_product_card(card)
                if product_data:
                    all_products.append(product_data)
        except Exception as e:
            logger.error(f"Failed to process page {page}: {e}")

        # Be nice to the server
        time.sleep(1.5)  # slightly longer delay to be more polite

    # Create DataFrame from collected products
    df = pd.DataFrame(all_products)

    # Log extraction summary
    if df.empty:
        logger.warning("No products were extracted!")
    else:
        logger.info(f"Successfully extracted {len(df)} products")
        df.to_csv('nama_file.csv', index=False)

    return df


if __name__ == "__main__":
    # For testing the extraction directly
    data = extract_data()
    print(f"Extracted {len(data)} products")
    if not data.empty:
        print(data.head())
