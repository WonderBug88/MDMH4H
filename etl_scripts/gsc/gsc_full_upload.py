import os
import psycopg2
from psycopg2 import extras
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
from datetime import datetime 

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv(dotenv_path=os.path.join(BASE_DIR, '.env'))


# Database connection parameters
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

# Path to your service account key file
SERVICE_ACCOUNT_FILE = os.environ.get('GOOGLE_SERVICE_ACCOUNT_FILE')

# Define the scopes and API service name/version
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
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
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


def fetch_gsc_data_in_batches(service, site_url, start_date, end_date, country, batch_size=5000):
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
        
        # Process and add the fetched rows to all_data
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

def main():
    service = get_authenticated_service()

    SITE_URL = 'https://www.hotels4humanity.com'
    START_DATE = '2024-11-12'
    END_DATE = '2024-11-13'
    COUNTRY = 'USA'

    # Fetch all data in batches
    all_data = fetch_gsc_data_in_batches(service, SITE_URL, START_DATE, END_DATE, COUNTRY, batch_size=5000)

    print(f"Total rows fetched: {len(all_data)}")
    # Since all_data is already prepared in the fetching function, you can directly pass it to the insertion function.
    # If you're handling a very large dataset, you might still need to insert in smaller batches to avoid overloading the DB.
    # This example shows direct insertion, but you could split all_data into smaller batches if necessary.
    insert_gsc_data(all_data)
    print("Data insertion completed.")

if __name__ == '__main__':
    main()
