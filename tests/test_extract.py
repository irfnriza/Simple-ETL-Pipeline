import unittest
from unittest.mock import patch, Mock
import pandas as pd
from bs4 import BeautifulSoup
import logging
from utils.extract import get_page_content, parse_product_card, extract_data

# Suppress logging during tests
logging.getLogger().setLevel(logging.CRITICAL)

class TestExtract(unittest.TestCase):
    def setUp(self):
        self.sample_html = """
        <div class="product-card">
            <h3 class="product-title">Test Product</h3>
            <span class="price">$99.99</span>
            <p>Rating: 4.5 / 5</p>
            <p>Colors: 3 Colors</p>
            <p>Size: M</p>
            <p>Gender: Unisex</p>
        </div>
        """
        self.empty_html = "<div></div>"

    @patch('requests.get')
    def test_get_page_content_success(self, mock_get):
        mock_response = Mock()
        mock_response.text = self.sample_html
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = get_page_content("http://test.com")
        self.assertEqual(result, self.sample_html)

    @patch('requests.get')
    def test_get_page_content_request_exception(self, mock_get):
        mock_get.side_effect = Exception("Request failed")
        result = get_page_content("http://test.com")
        self.assertIsNone(result)

    def test_parse_product_card_valid(self):
        soup = BeautifulSoup(self.sample_html, "html.parser")
        card = soup.find("div", class_="product-card")
        result = parse_product_card(card)
        expected = {
            "title": "Test Product",
            "price": "$99.99",
            "rating": "4.5 / 5",
            "colors": "3 Colors",
            "size": "M",
            "gender": "Unisex",
            "timestamp": result["timestamp"]  # Timestamp will vary
        }
        self.assertEqual({k: v for k, v in result.items() if k != "timestamp"}, 
                        {k: v for k, v in expected.items() if k != "timestamp"})
        self.assertTrue(isinstance(result["timestamp"], str))

    def test_parse_product_card_no_title(self):
        invalid_html = """
        <div class="product-card">
            <span class="price">$99.99</span>
        </div>
        """
        soup = BeautifulSoup(invalid_html, "html.parser")
        card = soup.find("div", class_="product-card")
        result = parse_product_card(card)
        self.assertIsNone(result)

    def test_parse_product_card_no_price(self):
        invalid_html = """
        <div class="product-card">
            <h3 class="product-title">Test Product</h3>
            <p>Rating: 4.5 / 5</p>
        </div>
        """
        soup = BeautifulSoup(invalid_html, "html.parser")
        card = soup.find("div", class_="product-card")
        result = parse_product_card(card)
        self.assertEqual(result["title"], "Test Product")
        self.assertEqual(result["price"], "N/A")

    def test_parse_product_card_empty_card(self):
        result = parse_product_card(None)
        self.assertIsNone(result)

    @patch('utils.extract.get_page_content')
    @patch('utils.extract.BeautifulSoup')
    def test_extract_data_success(self, mock_soup, mock_get_page):
        mock_get_page.return_value = self.sample_html
        mock_soup_instance = Mock()
        mock_soup_instance.select.side_effect = [
            [Mock()],  # First page has one product
            []         # Second page has no products
        ]
        mock_soup.return_value = mock_soup_instance

        with patch('utils.extract.parse_product_card', return_value={
            "title": "Test Product",
            "price": "$99.99",
            "rating": "4.5 / 5",
            "colors": "3 Colors",
            "size": "M",
            "gender": "Unisex",
            "timestamp": "2023-10-01T00:00:00"
        }):
            with patch('utils.extract.time.sleep', return_value=None):
                df = extract_data()
                self.assertFalse(df.empty)
                self.assertEqual(len(df), 1)
                self.assertEqual(df.iloc[0]["title"], "Test Product")

    @patch('utils.extract.get_page_content')
    def test_extract_data_no_content(self, mock_get_page):
        mock_get_page.return_value = None
        with patch('utils.extract.time.sleep', return_value=None):
            df = extract_data()
            self.assertTrue(df.empty)

    @patch('utils.extract.get_page_content')
    @patch('utils.extract.BeautifulSoup')
    def test_extract_data_no_products(self, mock_soup, mock_get_page):
        mock_get_page.return_value = self.empty_html
        mock_soup_instance = Mock()
        mock_soup_instance.select.return_value = []
        mock_soup.return_value = mock_soup_instance
        with patch('utils.extract.time.sleep', return_value=None):
            df = extract_data()
            self.assertTrue(df.empty)

if __name__ == '__main__':
    unittest.main()