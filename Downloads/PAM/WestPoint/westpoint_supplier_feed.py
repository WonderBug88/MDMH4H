import os
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
import time
import psycopg2
import traceback
from dotenv import load_dotenv

# Define the scope for Google Sheets and Drive API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials_path = "C:/Users/juddu/Downloads/PAM/MDMH4H/etl_scripts/gsc/triple-skein-418601-0d9a35a7da48.json"

# Authenticate using the service account key
creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
client = gspread.authorize(creds)

# Open the Google Sheet using its URL
spreadsheet_url = "https://docs.google.com/spreadsheets/d/1dOrkpjnoO9D8hMYCumZKd3x0qn9EMgCbA7MRUOM25Qc/edit"
spreadsheet = client.open_by_url(spreadsheet_url)
spreadsheet_id = spreadsheet.id

# List of tabs
tabs = [
    ("Mainspread Bedspreads", "'1'"),
    ("Duvet Covers | Shams", "'2'"),
    ("Top Sheets | Coverlets", "'3'"),
    ("Bedskirts | Boxspring Wraps", "'4'"),
    ("Martex Rx - Itemized List", "'6'"),
    ("Martex Ultra Touch | Martex Spa", "'7'"),
    ("Patrician Stripe", "'8'"),
    ("Millennium White", "'9'"),
    ("Millennium Bone", "'10'"),
    ("Millennium Stripe", "'11'"),
    ("Five Star Hotel Collection", "'12'"),
    ("Gryphon", "'13'"),
    ("Martex Dryfast", "'14'"),
    ("Martex Colors", "'15'"),
    ("Martex Health | Clean Sheet - Silverbac Antimicrobial", "'16'"),
    ("Martex Green", "'17'"),
    ("Towels", "'18'"),
    ("Pool Towels", "'19'"),
    ("Kitchen", "'20'"),
    ("Blankets | Comforters | Duvets (Inserts)", "'21'"),
    ("Pillow Protectors", "'22'"),
    ("Pillows", "'23'"),
    ("Mattress Pads | Toppers | Encasements", "'24'"),
    ("Bags - Hair Dryer, Blanket Storage", "'25'"),
    ("Apparel - Robes, Aprons, Scrubs, Gowns, Lab Coats", "'27'"),
]

# Master header to standardize columns
# master_header = ["Case QTY", "SKU", "Each", "old_sku_2", "old_sku", "Available"]
master_header = ["Case QTY", "SKU", "Each","Available"]

# Helper function to fetch formatting details
def get_cell_format(spreadsheet_id, range_name, creds):
    service = build("sheets", "v4", credentials=creds)
    request = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=range_name,
        fields="sheets(data(rowData(values(effectiveFormat))))"
    )
    response = request.execute()
    return response

# Helper function to process each tab
def process_tab(description, tab_name):
    try:
        worksheet = spreadsheet.worksheet(tab_name.strip("'"))
        rows = worksheet.get_all_values()

        if not rows:
            print(f"No data in {description} ({tab_name}), skipping.")
            return None

        df = pd.DataFrame(rows)

        # Identify the header row
        header_row_idx = df[df.apply(lambda x: x.str.contains("SKU", case=False).any(), axis=1)].index
        if header_row_idx.empty:
            print(f"No header row found in {description} ({tab_name}), skipping.")
            return None

        header_row_idx = header_row_idx[0]
        df.columns = df.iloc[header_row_idx]  # Set the header row as columns
        df = df.iloc[header_row_idx + 1:]  # Drop the header row from data

        # Standardize columns
        df = df.reindex(columns=master_header, fill_value="")
        df = df.fillna("")  # fill missing cells with empty string

        # Fetch cell formatting
        range_name = f"{tab_name.strip('\'')}!A1:{chr(64 + len(df.columns))}{len(df) + 1}"
        formatting = get_cell_format(spreadsheet_id, range_name, creds)

        # Add "Available" column based on formatting
        available_column = []
        for grid_row in formatting["sheets"][0]["data"][0]["rowData"][1:]:
            is_red = any(
                cell.get("effectiveFormat", {}).get("textFormat", {}).get("foregroundColor", {}) == {"red": 1}
                for cell in grid_row["values"]
            )
            available_column.append("False" if is_red else "True")

        # Append the "Available" column to the DataFrame
        print("len(df) =", len(df))
        print("len(available_column) =", len(available_column))
        available_column = available_column[: len(df)]
        df["Available"] = available_column

        print(f"Data extracted for {description} ({tab_name}).")
        return df

    except Exception as e:
        print(f"Failed to fetch data from {description} ({tab_name}): {e}")
        return None

# Process all tabs
dataframes = []
for description, tab_name in tabs:
    df = process_tab(description, tab_name)
    if df is not None:
        dataframes.append(df)
    time.sleep(2)  # Delay to manage rate limits

# Concatenate all dataframes for consolidated output
if dataframes:
    consolidated_data = pd.concat(dataframes, ignore_index=True)
    consolidated_data = consolidated_data.loc[:, ~consolidated_data.columns.duplicated()]  # Remove duplicate columns

    # Save consolidated data to a CSV file
    consolidated_data.to_csv("consolidated_data.csv", index=False)
    print("Consolidated data saved locally as 'consolidated_data.csv'.")
else:
    print("No valid data extracted. Consolidation aborted.")
    exit()

# Import consolidated data into the database
env_path = "C:/Users/juddu/Downloads/PAM/MDMH4H/etl_scripts/gsc/.env"
load_dotenv(env_path)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

try:
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = connection.cursor()
    print("Connected to the database successfully.")
    # Preprocess the data
    consolidated_data = consolidated_data.rename(columns={
    "SKU": "variant_sku",
    "Available": "variant_available",
    "Case QTY": "case_qty",
    "Each": "price_per_each"

    })

    # Drop rows with missing or null variant_sku
    consolidated_data = consolidated_data[consolidated_data["variant_sku"].notnull() & (consolidated_data["variant_sku"] != "")]

    # Drop duplicates based on the primary key column
    consolidated_data = consolidated_data.drop_duplicates(subset=["variant_sku"])

    # Replace empty strings with None
    consolidated_data = consolidated_data.replace("", None)

    # Clean the "price_per_each" column
    if "price_per_each" in consolidated_data.columns:
        consolidated_data["price_per_each"] = (
            consolidated_data["price_per_each"]
            .fillna("0")  # Replace None or NaN with "0"
            .astype(str)  # Ensure all values are strings for cleaning
            .str.replace(r"[^\d\.-]", "", regex=True)  # Remove non-numeric characters
        )
        consolidated_data["price_per_each"] = pd.to_numeric(consolidated_data["price_per_each"], errors="coerce").fillna(0.0)

    # Convert "case_qty" to numeric and handle NaN values
    if "case_qty" in consolidated_data.columns:
        consolidated_data["case_qty"] = pd.to_numeric(consolidated_data["case_qty"], errors="coerce").fillna(0).astype(int)

    # Convert "variant_available" column to boolean
    if "variant_available" in consolidated_data.columns:
        consolidated_data["variant_available"] = consolidated_data["variant_available"].apply(
            lambda x: str(x).strip().lower() == "true"
        )

    # Fill missing values with None for nullable columns
    # if "old_sku" in consolidated_data.columns:
    #     print("Columns in the DataFrame:", consolidated_data.columns)
    #     print("Sample rows:\n", consolidated_data.head())
    #     consolidated_data["old_sku"] = consolidated_data["old_sku"].fillna(None)

#     if "old_sku_2" in consolidated_data.columns:
  #       print("Columns in the DataFrame:", consolidated_data.columns)
    #     print("Sample rows:\n", consolidated_data.head())
       #  consolidated_data["old_sku_2"] = consolidated_data["old_sku_2"].fillna(None)

    # Final sanity check
    print("Null values per column:\n", consolidated_data.isnull().sum())
    print("Sample cleaned DataFrame:\n", consolidated_data.head())

    # Insert or update rows
    insert_query = """    
    INSERT INTO manufactured.westpoint (variant_sku, variant_available, case_qty, price_per_each)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (variant_sku) DO UPDATE
    SET 
        variant_available = EXCLUDED.variant_available,
        case_qty = EXCLUDED.case_qty,
        price_per_each = EXCLUDED.price_per_each;
    """

    for _, row in consolidated_data.iterrows():
        cursor.execute(insert_query, (
            row["variant_sku"],
            row["variant_available"],
            row["case_qty"],
            row["price_per_each"]

        ))
    connection.commit()
    print("Data successfully imported into the database.")


except Exception as e:
    print(f"Error importing data into the database: {e}")
    traceback.print_exc()     # <--- Show the *full* Python stack trace
    if connection:
        connection.rollback()

finally:
    if connection:
        cursor.close()
        connection.close()
        print("Database connection closed.")