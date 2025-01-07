# WestPoint ETL and Scraper Project

## Overview

This project automates the processes of scraping product data from WestPoint Direct's website, transforming and cleaning data from Google Sheets, and importing the data into a PostgreSQL database. It consists of three main components:

1. **Scraper (`westpoint_scraper.py`)**: Scrapes product and category data from WestPoint Direct.
2. **Google Sheets Feed (`westpoint_supplier_feed.py`)**: Extracts, cleans, and consolidates supplier data from Google Sheets.
3. **Database ETL (`westpoint_freight.py`)**: Validates and updates the database with the extracted data.

---

## Prerequisites

- Python 3.7+
- PostgreSQL database
- Google Sheets API credentials
- Required Python libraries (see `requirements.txt`)

---

## Installation

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set Up Environment Variables**:
   Create a `.env` file in the project root directory with the following keys:
   ```env
   DB_HOST=<database-host>
   DB_PORT=<database-port>
   DB_NAME=<database-name>
   DB_USER=<database-user>
   DB_PASSWORD=<database-password>
   GOOGLE_CREDENTIALS_PATH=<path-to-google-credentials.json>
   GOOGLE_SHEET_URL=<google-sheet-url>
   ```

---

## Usage

### 1. Scraper (`westpoint_scraper.py`)
- **Description**: Scrapes product and category data from the WestPoint Direct website.
- **Run**:
   ```bash
   python westpoint_scraper.py
   ```
- **Features**:
   - Extracts product data, including name, description, price, and images.
   - Handles pagination and deduplicates SKUs.
   - Inserts data into the PostgreSQL database.

---

### 2. Supplier Feed (`westpoint_supplier_feed.py`)
- **Description**: Processes supplier data from multiple Google Sheets tabs into a consolidated CSV file and imports the data into the database.
- **Run**:
   ```bash
   python westpoint_supplier_feed.py
   ```
- **Features**:
   - Extracts and standardizes data from multiple tabs in a Google Sheet.
   - Handles cell formatting to identify availability.
   - Cleans and transforms data for database import.

---

### 3. Freight ETL (`westpoint_freight.py`)
- **Description**: Validates and updates the PostgreSQL database using data from Google Sheets.
- **Run**:
   ```bash
   python westpoint_freight.py
   ```
- **Features**:
   - Validates extracted data.
   - Upserts records into the `manufactured.westpoint` table in PostgreSQL.

---

## Project Structure

```
/project-root
├── westpoint_scraper.py          # Web scraper for WestPoint Direct
├── westpoint_supplier_feed.py    # Processes Google Sheets data
├── westpoint_freight.py          # ETL script for PostgreSQL
├── requirements.txt              # Required Python libraries
└── README.md                     # Project documentation
```

---

## Logging

- All logs are stored in `etl_errors.log`.
- Logs include detailed error messages for debugging and success confirmations.

---

## Contributions

Feel free to submit issues or pull requests for bug fixes, improvements, or additional features.

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

---
