import os
import psycopg2
import psycopg2.extras
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv(dotenv_path=os.path.join(BASE_DIR, '.env'))

# Database connection parameters
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

SERVICE_ACCOUNT_FILE = f'{BASE_DIR}/etl_scripts/gsc/triple-skein-418601-2b7de307bc54.json'

SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
API_SERVICE_NAME = 'webmasters'
API_VERSION = 'v3'


def get_authenticated_service():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


def insert_gsc_data(data):
    """Inserts a batch of data into the PostgreSQL database."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cursor = conn.cursor()
        psycopg2.extras.execute_batch(cursor, '''
            INSERT INTO analytics_data.gsc_data (query, page, clicks, impressions, ctr, position, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (query, page, date) DO UPDATE 
            SET clicks = EXCLUDED.clicks, impressions = EXCLUDED.impressions, ctr = EXCLUDED.ctr, position = EXCLUDED.position
        ''', data)
        conn.commit()
    except Exception as e:
        print(f"Database insertion error: {e}")
    finally:
        if conn:
            conn.close()


def fetch_weekly_gsc_data(service, site_url, start_date, end_date, country, batch_size=5000):
    """Fetches GSC data for the past week."""

    all_data = []
    start_row = 0

    while True:
        response = service.searchanalytics().query(
            siteUrl=site_url,
            body={
                'startDate': start_date,
                'endDate': end_date,
                'dimensions': ['query', 'page', 'date'],
                'dimensionFilterGroups': [{
                    'filters': [{
                        'dimension': 'country',
                        'expression': country
                    }]
                }],
                'rowLimit': batch_size,
                'startRow': start_row
            }).execute()

        rows = response.get('rows', [])
        if not rows:
            break  # No more data to fetch

        for row in rows:
            data_tuple = (
                row['keys'][0],  # query
                row['keys'][1],  # page
                row.get('clicks', 0),
                row.get('impressions', 0),
                row.get('ctr', 0),
                row.get('position', 0),
                row['keys'][2]  # date
            )
            all_data.append(data_tuple)

        start_row += len(rows)
        print(f"Fetched {len(all_data)} rows so far...")

    return all_data


def gsc_weekly_update_main():
    service = get_authenticated_service()

    SITE_URL = 'https://www.hotels4humanity.com'
    COUNTRY = 'USA'

    # Calculate the dates for the current week's data
    today = datetime.now()
    start_of_week = today - timedelta(days=today.weekday())
    end_of_week = start_of_week + timedelta(days=6)

    START_DATE = start_of_week.strftime('%Y-%m-%d')
    END_DATE = end_of_week.strftime('%Y-%m-%d')

    logging.info(f"Fetching GSC data for the week {START_DATE} to {END_DATE}...")
    
    weekly_data = fetch_weekly_gsc_data(
        service, SITE_URL, START_DATE, END_DATE, COUNTRY, batch_size=5000)

    print(f"Total rows fetched for the past week: {len(weekly_data)}")

    # Insert the fetched data into the database
    insert_gsc_data(weekly_data)

    print("Data insertion complete at ", datetime.now())

    return len(weekly_data)
