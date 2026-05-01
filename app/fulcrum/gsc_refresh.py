"""Fulcrum-local GSC refresh helpers."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
from google.oauth2 import service_account
from googleapiclient.discovery import build

from app.fulcrum.config import Config


SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
API_SERVICE_NAME = "webmasters"
API_VERSION = "v3"
DEFAULT_SITE_URL = "https://www.hotels4humanity.com"
DEFAULT_COUNTRY = "USA"
DEFAULT_SERVICE_ACCOUNT_FILE = Path(Config.BASE_DIR) / "etl_scripts" / "gsc" / "triple-skein-418601-0d9a35a7da48.json"


def get_authenticated_service(service_account_file: str | Path | None = None):
    credentials = service_account.Credentials.from_service_account_file(
        str(service_account_file or Config.GSC_CREDENTIALS_FILE_PATH or DEFAULT_SERVICE_ACCOUNT_FILE),
        scopes=SCOPES,
    )
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


def insert_gsc_data(data):
    """Insert a batch of GSC rows into analytics_data.gsc_data."""
    conn = None
    try:
        if Config.DATABASE_URL:
            conn = psycopg2.connect(Config.DATABASE_URL)
        else:
            conn = psycopg2.connect(
                host=Config.DB_HOST,
                dbname=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
            )
        cursor = conn.cursor()
        psycopg2.extras.execute_batch(
            cursor,
            """
            INSERT INTO analytics_data.gsc_data (query, page, clicks, impressions, ctr, position, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (query, page, date) DO UPDATE
            SET clicks = EXCLUDED.clicks,
                impressions = EXCLUDED.impressions,
                ctr = EXCLUDED.ctr,
                position = EXCLUDED.position
            """,
            data,
        )
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        logging.exception("Fulcrum GSC insert failed: %s", exc)
        raise
    finally:
        if conn:
            conn.close()


def fetch_weekly_gsc_data(service, site_url, start_date, end_date, country, batch_size=5000):
    """Fetch GSC rows for a site/country/date range."""

    all_data = []
    start_row = 0

    while True:
        response = service.searchanalytics().query(
            siteUrl=site_url,
            body={
                "startDate": start_date,
                "endDate": end_date,
                "dimensions": ["query", "page", "date"],
                "dimensionFilterGroups": [
                    {
                        "filters": [
                            {
                                "dimension": "country",
                                "expression": country,
                            }
                        ]
                    }
                ],
                "rowLimit": batch_size,
                "startRow": start_row,
            },
        ).execute()

        rows = response.get("rows", [])
        if not rows:
            break

        for row in rows:
            all_data.append(
                (
                    row["keys"][0],
                    row["keys"][1],
                    row.get("clicks", 0),
                    row.get("impressions", 0),
                    row.get("ctr", 0),
                    row.get("position", 0),
                    row["keys"][2],
                )
            )

        start_row += len(rows)
        logging.info("Fulcrum GSC refresh fetched %s rows so far.", len(all_data))

    return all_data


def gsc_weekly_update_main(
    *,
    site_url: str = DEFAULT_SITE_URL,
    country: str = DEFAULT_COUNTRY,
    batch_size: int = 5000,
    service_account_file: str | Path | None = None,
) -> int:
    service = get_authenticated_service(service_account_file=service_account_file)

    today = datetime.now()
    start_of_week = today - timedelta(days=today.weekday())
    end_of_week = start_of_week + timedelta(days=6)

    start_date = start_of_week.strftime("%Y-%m-%d")
    end_date = end_of_week.strftime("%Y-%m-%d")

    logging.info("Fulcrum GSC refresh fetching %s to %s.", start_date, end_date)

    weekly_data = fetch_weekly_gsc_data(
        service,
        site_url,
        start_date,
        end_date,
        country,
        batch_size=batch_size,
    )
    insert_gsc_data(weekly_data)

    logging.info("Fulcrum GSC refresh inserted %s rows.", len(weekly_data))
    return len(weekly_data)
