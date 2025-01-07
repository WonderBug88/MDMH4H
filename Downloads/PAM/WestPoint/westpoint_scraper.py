import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin
import logging
import csv
import html
import unicodedata
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv


class WestPointScraper:
    BASE_URL = "https://buywestpointdirect.com/"

    def __init__(self):
        self.session = requests.Session()
        self.logger = self.setup_logging()  # Initialize the logger

    def setup_logging(self):
        """Set up logging."""
        logger = logging.getLogger("WestPointScraper")
        logger.setLevel(logging.INFO)
        if not logger.handlers:  # Avoid adding multiple handlers
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def get_html(self, url):
        """Fetch HTML content from the URL."""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            return None

    def parse_categories(self):
        """Parse all category links from the main page."""
        html = self.get_html(self.BASE_URL)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        category_links = [
            urljoin(self.BASE_URL, a["href"])
            for a in soup.select('ul.menu-drawer__menu a.menu-drawer__menu-item')
        ]

        # Filter unwanted links
        unwanted_links = ["/pages/contact-us", "/blogs/news"]
        category_links = [
            link for link in category_links if not any(unwanted in link for unwanted in unwanted_links)
        ]
        self.logger.info(f"Found {len(category_links)} Collection: {category_links}")
        return category_links

    def parse_category(self, category_url):
        """Parse all product links from a collection page."""
        html = self.get_html(category_url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        product_links = [
            urljoin(self.BASE_URL, a["href"])
            for a in soup.select('ul#product-grid li.grid__item a.full-unstyled-link')
        ]
        self.logger.info(f"Found {len(product_links)} products in category {category_url}")

        # Handle pagination using the <link rel="next"> tag
        next_page_tag = soup.find("link", rel="next")
        if next_page_tag:
            next_page_url = urljoin(self.BASE_URL, next_page_tag["href"])
            self.logger.info(f"Found next page: {next_page_url}")
            product_links += self.parse_category(next_page_url)

        return product_links
    
    def clean_text(self, text):
        """Clean and normalize text for proper formatting."""
        if not text:
            return text
        import html  # Import required here if not globally defined
        import unicodedata
        # Decode HTML entities
        text = html.unescape(text)
        # Normalize Unicode characters
        text = unicodedata.normalize("NFKD", text)
        # Replace non-breaking spaces and other odd characters
        text = text.replace("\u00a0", " ").replace("\u2019", "'")
        return text.strip()

    def parse_product(self, product_url):
        """Parse product details from a product page."""
        self.logger.info(f"Processing product: {product_url}")
        html = self.get_html(product_url)
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")

        # Extract basic product details from meta tags
        product_name = soup.find("meta", property="og:title")["content"]
        product_url = soup.find("meta", property="og:url")["content"]
        # Extract and clean product description
        description = soup.find("meta", property="og:description")["content"]
        description = self.clean_text(description)
        price = soup.find("meta", property="og:price:amount")["content"]
        currency = soup.find("meta", property="og:price:currency")["content"]

        # Extract images
        images = self.extract_product_images(soup)

        # Extract variants if available
        variants = []
        script_data = soup.select_one("variant-radios script[type='application/json']")
        if script_data:
            try:
                parsed_data = json.loads(script_data.string)
                variants = [
                    {
                        "id": variant.get("id"),
                        "title": variant.get("title"),
                        "sku": variant.get("sku"),
                        "price": variant.get("price", 0) / 100,  # Convert from cents to dollars
                        "weight": variant.get("weight", 0) * 0.00220462,  # Convert from grams to pounds
                        "available": variant.get("available", False),
                    }
                    for variant in parsed_data
                ]
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse JSON in variant-radios on {product_url}: {e}")

        # Return compiled product data
        return {
            "name": product_name,
            "url": product_url,
            "description": description,
            "price": price,
            "currency": currency,
            "images": images,
            "variants": variants,
        }

    def extract_product_images(self, soup):
        """Extract all image URLs from the product gallery."""
        image_urls = []

        # Find the <ul> container with the class and ID for the product gallery
        gallery = soup.find("ul", class_="product__media-list")
        if not gallery:
            return image_urls  # Return empty if no gallery is found

        # Extract all <img> elements within the gallery
        for img_tag in gallery.find_all("img"):
            # Ensure the `src` attribute exists
            img_url = img_tag.get("src")
            if img_url:
                # Convert the relative URLs to absolute URLs
                if img_url.startswith("//"):
                    img_url = f"https:{img_url}"
                image_urls.append(img_url)

        return image_urls

    def scrape(self):
        """Main scraping logic."""
        categories = self.parse_categories()
        all_products = []
        for category in categories:
            product_links = self.parse_category(category)
            for product_link in product_links:
                product_data = self.parse_product(product_link)
                if product_data:
                    all_products.append(product_data)
        self.logger.info(f"Scraped {len(all_products)} products.")
        return all_products

    def flatten_data(self, products):
        """Flatten the hierarchical product data for database export, with deduplication."""
        flat_data = []
        seen_skus = set()  # Track unique SKUs

        for product in products:
            base_data = {
                "name": product["name"],
                "url": product["url"],
                "description": product["description"],
                "price": product["price"],
                "currency": product["currency"],
            }

            # Add images as separate columns
            image_columns = {f"image_{i+1}": img for i, img in enumerate(product["images"])}

            # Add variants if available
            for variant in product["variants"]:
                if variant["sku"] in seen_skus:  # Skip if SKU is already processed
                    continue
                seen_skus.add(variant["sku"])

                flat_data.append({
                    **base_data,
                    **image_columns,
                    "variant_id": variant["id"],
                    "variant_title": variant["title"],
                    "variant_sku": variant["sku"],
                    "variant_price": variant["price"],
                    "variant_weight": variant["weight"],
                    "variant_available": variant["available"],
                })

            # If no variants, include base data with images only
            if not product["variants"]:
                flat_data.append({**base_data, **image_columns})

        return flat_data

    def insert_to_postgres(self, flat_data):
        """Insert flattened data into PostgreSQL with upsert to avoid duplicates."""
        # Load environment variables
        load_dotenv("C:\\Users\\juddu\\Downloads\\PAM\\MDMH4H\\.env")

        db_params = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
        }

        try:
            conn = psycopg2.connect(**db_params)
            cursor = conn.cursor()

            insert_query = """
            INSERT INTO manufactured.westpoint (
                name, description, price, variant_sku, variant_title, variant_price,
                currency, variant_available, variant_weight, variant_id,
                image_1, image_2, image_3, image_4, image_5, image_6, image_7, image_8,
                image_9, image_10, image_11, image_12, image_13, image_14, image_15, 
                image_16, image_17, image_18, image_19, image_20, image_21, image_22, 
                image_23, image_24, image_25, image_26, image_27, image_28, image_29, 
                image_30, image_31, image_32, image_33, image_34, image_35, image_36,
                image_37, image_38, image_39, image_40, image_41, image_42, image_43,
                image_44, image_45, image_46, image_47, image_48, image_49, url
            ) VALUES %s
            ON CONFLICT (variant_sku) DO NOTHING
            """

            rows = []
            for row in flat_data:
                values = [
                    row.get("name"), row.get("description"), row.get("price"), row.get("variant_sku"),
                    row.get("variant_title"), row.get("variant_price"), row.get("currency"),
                    row.get("variant_available"), row.get("variant_weight"), row.get("variant_id")
                ]
                for i in range(1, 50):
                    values.append(row.get(f"image_{i}", None))
                values.append(row.get("url"))
                rows.append(values)

            execute_values(cursor, insert_query, rows)
            conn.commit()
            self.logger.info("Data successfully inserted into PostgreSQL.")
        except Exception as e:
            self.logger.error(f"Failed to insert data into PostgreSQL: {e}")
        finally:
            if conn:
                cursor.close()
                conn.close()


if __name__ == "__main__":
    scraper = WestPointScraper()
    products = scraper.scrape()

    # Flatten the data
    flat_data = scraper.flatten_data(products)

    # Insert data into PostgreSQL
    scraper.insert_to_postgres(flat_data)

    print("Scraping and database insertion completed.")
