import os
import psycopg2
from psycopg2 import extras
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
from datetime import datetime, date, timedelta
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv(dotenv_path=os.path.join(BASE_DIR, '.env'))

# Database connection parameters
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

# Path to your service account key file
SERVICE_ACCOUNT_FILE = os.environ.get('GOOGLE_SERVICE_ACCOUNT_FILE')

# Google Search Console API configs
SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
API_SERVICE_NAME = 'webmasters'
API_VERSION = 'v3'


# ----------------------------------------
# 🔐 AUTH
# ----------------------------------------
def get_authenticated_service():
    print("🔑  Authenticating with Google Search Console…")
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    print("🔓  Authenticated.")
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


# ----------------------------------------
# 🔧 HELPERS
# ----------------------------------------
def normalize_gsc_page(page: str) -> str:
    if not page:
        return ""
    return page.split("?", 1)[0].strip()


def get_last_gsc_date() -> date | None:
    """
    Look at analytics_data.gsc_data and return the max(date), or None if table is empty.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute("SELECT max(date) FROM analytics_data.gsc_data;")
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        return None
    except Exception as e:
        print(f"⚠️  Could not determine last GSC date, defaulting: {e}")
        return None
    finally:
        if conn:
            conn.close()


# ----------------------------------------
# 📝 UPSERT INTO DATABASE
# ----------------------------------------
def insert_gsc_data(data):
    if not data:
        print("⚠️  No aggregated data to insert.")
        return

    conn = None
    try:
        print("🗄️  Connecting to PostgreSQL…")
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cursor = conn.cursor()

        sql = """
            INSERT INTO analytics_data.gsc_data
                (query, page, clicks, impressions, ctr, position, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (query, page, date)
            DO UPDATE SET
                clicks = EXCLUDED.clicks,
                impressions = EXCLUDED.impressions,
                ctr = EXCLUDED.ctr,
                position = EXCLUDED.position;
        """

        print(f"📀  Upserting {len(data)} aggregated rows into database…")
        extras.execute_batch(cursor, sql, data, page_size=5000)
        conn.commit()

        print(f"✅  Upsert complete — {len(data)} rows written.")

    except Exception as e:
        print(f"❌  Database insertion error: {e}")

    finally:
        if conn:
            conn.close()
            print("🔌  Database connection closed.")


# ----------------------------------------
# 📥 FETCH FROM GOOGLE SEARCH CONSOLE
# ----------------------------------------
def fetch_gsc_data_in_batches(service, site_url, start_date, end_date, country, batch_size=5000):
    print("🌐  Fetching Search Console data…")
    print(f"📅  Range: {start_date} ➜ {end_date}")
    print(f"🌎  Country filter: {country}")

    agg = defaultdict(lambda: {
        "clicks": 0,
        "impressions": 0,
        "pos_sum": 0.0,
        "pos_count": 0,
    })

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
            print("📭  No more rows returned from API.")
            break

        for r in rows:
            q = r["keys"][0]
            p = normalize_gsc_page(r["keys"][1])
            d = r["keys"][2]

            clicks = r.get("clicks", 0)
            impr   = r.get("impressions", 0)
            pos    = r.get("position", 0)

            key = (q, p, d)
            agg[key]["clicks"] += clicks
            agg[key]["impressions"] += impr
            agg[key]["pos_sum"] += pos
            agg[key]["pos_count"] += 1

        start_row += len(rows)
        print(f"📥  Processed {start_row:,} raw rows…")

    # Convert to aggregated result
    final = []
    for (query, page, date_str), v in agg.items():
        clicks = v["clicks"]
        impr   = v["impressions"]
        ctr    = (clicks / impr) if impr > 0 else 0
        avg_pos = v["pos_sum"] / v["pos_count"]

        final.append((query, page, clicks, impr, ctr, avg_pos, date_str))

    print(f"📦  Final aggregated rows: {len(final):,}")
    return final


# ----------------------------------------
# 🚀 PUBLIC ENTRYPOINT FOR APP / DJANGO / CLI
# ----------------------------------------
def run_gsc_incremental(start_date: str | None = None,
                        end_date: str | None = None,
                        country: str = "USA"):
    """
    Run an incremental GSC import. This is what Streamlit/Django should call.
    If start_date is None → use last date in analytics_data.gsc_data + 1 day,
    or fall back to a hard-coded early date.
    """
    service = get_authenticated_service()

    SITE_URL = 'https://www.hotels4humanity.com'

    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")

    if start_date is None:
        last = get_last_gsc_date()
        if last:
            start_date = (last + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            # Fallback – you can change this
            start_date = "2024-01-01"

    print(f"\n🚀  Starting incremental GSC pull")
    print(f"🗂️   Site: {SITE_URL}")
    print(f"📅   Dates: {start_date} ➜ {end_date}\n")

    all_data = fetch_gsc_data_in_batches(
        service,
        SITE_URL,
        start_date,
        end_date,
        country,
        batch_size=5000
    )

    print(f"\n🧮  Total aggregated rows ready to insert: {len(all_data):,}")
    insert_gsc_data(all_data)
    print("\n🎉  GSC ETL job complete!\n")


# ----------------------------------------
# CLI wrapper
# ----------------------------------------
def main():
    run_gsc_incremental()   # uses incremental logic above


if __name__ == '__main__':
    main()
