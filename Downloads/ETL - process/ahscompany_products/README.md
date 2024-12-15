# AHS Company Products Scraper

This project is a Scrapy-based web scraping script designed to extract product data from the AHS Company website and store it in a PostgreSQL database.

## Features

- **Web Scraping with Scrapy:**
  - Crawls the AHS Company website to extract product data, including:
    - Product title, price, part number, brand, stock status, category, description, and more.
  - Handles product variants and multiple categories.

- **Data Storage in PostgreSQL:**
  - Saves extracted data into the `ahs.products` table.
  - Ensures data consistency with `ON CONFLICT` updates.

- **Configurable Settings:**
  - Customizable headers, retry logic, and feed export settings.

- **Output Options:**
  - Saves scraped data as a CSV file in the `output` folder.
  - Allows easy integration into other systems.

---

## Prerequisites

- **Python**: Version 3.8 or higher.
- **PostgreSQL**: Ensure a database is set up with the correct credentials.
- **Scrapy**: Install using `pip install scrapy`.
- **Other Dependencies**: Install additional requirements:
  ```bash
  pip install psycopg2
