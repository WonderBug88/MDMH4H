import os
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import psycopg2
import logging
from decimal import Decimal

def safe_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_decimal(value):
    try:
        return Decimal(value)
    except (ValueError, TypeError):
        return None

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "")
DB_PORT = os.getenv("DB_PORT", "5432")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")

# Logging setup
logging.basicConfig(
    filename="etl_errors.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Validate environment variables
if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, GOOGLE_CREDENTIALS_PATH, GOOGLE_SHEET_URL]):
    logging.error("Missing required environment variables.")
    raise ValueError("Missing required environment variables.")

logging.info("Environment variables loaded successfully.")

# Authenticate and access Google Sheets
try:
    logging.info("Authenticating Google Sheets API...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=scope)
    client = gspread.authorize(creds)
    logging.info("Google Sheets API authenticated successfully.")
except Exception as e:
    logging.error(f"Google Sheets authentication failed: {e}")
    raise

# Open the Google Sheet
try:
    logging.info("Accessing Google Sheet...")
    spreadsheet = client.open_by_url(GOOGLE_SHEET_URL)
    records = spreadsheet.sheet1.get_all_values()  # Assuming data is in the first sheet
    logging.info(f"Successfully accessed spreadsheet: {spreadsheet.title}")
    logging.info(f"Number of records fetched: {len(records)}")
except Exception as e:
    logging.error(f"Failed to open the Google Sheet: {e}")
    raise

# Connect to PostgreSQL
try:
    logging.info("Connecting to PostgreSQL database...")
    with psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        port=DB_PORT
    ) as conn:
        logging.info("Connected to PostgreSQL database successfully.")
        with conn.cursor() as cursor:
            upsert_query = """
            UPDATE manufactured.westpoint
            SET 
                pattern_collection = %(pattern_collection)s,
                color = %(color)s,
                fabric_finishing_techniques = %(fabric_finishing_techniques)s,
                thread_count = %(thread_count)s,
                material = %(material)s,
                fill_weight = %(fill_weight)s,
                edge_hem_style = %(edge_hem_style)s,
                fill_material = %(fill_material)s,
                size = %(size)s,
                product_width = %(product_width)s,
                product_length = %(product_length)s,
                product_type = %(product_type)s,
                sub_category = %(sub_category)s,
                case_qty = %(case_qty)s,
                nmf_class = %(nmf_class)s,
                nmf_item = %(nmf_item)s,
                country_of_origin = %(country_of_origin)s,
                sewn_country = %(sewn_country)s,
                tariff_hts_code = %(tariff_hts_code)s,
                otexa_category_code = %(otexa_category_code)s,
                case_length = %(case_length)s,
                case_width = %(case_width)s,
                case_height = %(case_height)s,
                variant_weight = %(variant_weight)s,
                care_instructions = %(care_instructions)s
            WHERE variant_sku = %(variant_sku)s;
            """
            for i, row in enumerate(records[1:], start=2):  # Skip header row
                logging.debug(f"Processing row {i}: {row}")
                data = {
                    "variant_sku": row[0].strip() if row[0] else None,
                    "pattern_collection": row[6].strip() if row[6] else None,
                    "color": row[10].strip() if row[10] else None,
                    "fabric_finishing_techniques": row[14].strip() if row[14] else None,
                    "thread_count": safe_int(row[22]),
                    "material": row[24].strip() if row[24] else None,
                    "fill_weight": safe_decimal(row[66]),
                    "edge_hem_style": row[16].strip() if row[16] else None,
                    "fill_material": row[65].strip() if row[65] else None,
                    "size": row[12].strip() if row[12] else None,
                    "product_width": safe_decimal(row[17]),
                    "product_length": safe_decimal(row[18]),
                    "product_type": row[8].strip() if row[8] else None,
                    "sub_category": row[7].strip() if row[7] else None,
                    "case_qty": safe_int(row[43]),
                    "nmf_class": row[59].strip() if row[59] else None,
                    "nmf_item": row[60].strip() if len(row) > 60 and row[60] else None,
                    "country_of_origin": row[57].strip() if row[57] else None,
                    "sewn_country": row[55].strip() if row[55] else None,
                    "tariff_hts_code": row[68].strip() if row[68] else None,
                    "otexa_category_code": row[69].strip() if row[69] else None,
                    "case_length": safe_decimal(row[38]),
                    "case_width": safe_decimal(row[39]),
                    "case_height": safe_decimal(row[40]),
                    "variant_weight": safe_decimal(row[33]),
                    "care_instructions": row[51].strip() if row[51] else None
                }
                if not data["variant_sku"]:
                    logging.error(f"Missing variant_sku for row {i}")
                    continue
                try:
                    cursor.execute(upsert_query, data)
                    logging.info(f"Successfully updated SKU: {data['variant_sku']} (Row {i})")
                except Exception as e:
                    logging.error(f"Error on row {i} (SKU={data['variant_sku']}): {e}")
            conn.commit()  # Commit changes after processing all rows
            logging.info("All records committed to the database successfully.")
except Exception as e:
    logging.error(f"Database connection failed: {e}")
    raise
