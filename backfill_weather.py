#!/usr/bin/env python3
"""
backfill historical weather from open-meteo archive api.

run once after creating the weather_hourly table to populate data
for the full scraping period. safe to re-run (upserts on conflict).

usage:
    DATABASE_URL=... python backfill_weather.py
    DATABASE_URL=... python backfill_weather.py --start 2026-01-03 --end 2026-06-30
"""

import os
import sys
import argparse
import requests
import psycopg2
import psycopg2.extras
from datetime import date, timedelta

PRAGUE_LAT   = 50.0755
PRAGUE_LNG   = 14.4378
ARCHIVE_URL  = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS  = "temperature_2m,precipitation,windspeed_10m,weathercode,snowfall"

# archive api typically lags by 2-5 days
ARCHIVE_LAG_DAYS = 5


def fetch_weather(start_date: str, end_date: str) -> list[dict]:
    resp = requests.get(ARCHIVE_URL, params={
        "latitude":   PRAGUE_LAT,
        "longitude":  PRAGUE_LNG,
        "start_date": start_date,
        "end_date":   end_date,
        "hourly":     HOURLY_VARS,
        "timezone":   "UTC",
    }, timeout=60)
    resp.raise_for_status()
    hourly = resp.json()["hourly"]

    rows = []
    for i, ts in enumerate(hourly["time"]):
        rows.append((
            ts + "+00:00",                    # make postgres parse it as utc
            hourly["temperature_2m"][i],
            hourly["precipitation"][i],
            hourly["windspeed_10m"][i],
            hourly["weathercode"][i],
            hourly["snowfall"][i],
        ))
    return rows


def upsert_rows(conn, rows: list[tuple]) -> None:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO weather_hourly
                (hour, temperature, precipitation, windspeed, weathercode, snowfall)
            VALUES %s
            ON CONFLICT (hour) DO UPDATE SET
                temperature   = EXCLUDED.temperature,
                precipitation = EXCLUDED.precipitation,
                windspeed     = EXCLUDED.windspeed,
                weathercode   = EXCLUDED.weathercode,
                snowfall      = EXCLUDED.snowfall
            """,
            rows,
            page_size=500,
        )
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2026-01-03", help="start date YYYY-MM-DD")
    parser.add_argument("--end",   default=None,         help="end date YYYY-MM-DD (default: 5 days ago)")
    args = parser.parse_args()

    end_date = args.end or str(date.today() - timedelta(days=ARCHIVE_LAG_DAYS))

    print(f"fetching weather {args.start} to {end_date}...")

    rows = fetch_weather(args.start, end_date)
    print(f"  {len(rows):,} hourly records")

    db_url = os.environ.get("SUPABASE_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        print("set SUPABASE_DATABASE_URL or DATABASE_URL")
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    try:
        upsert_rows(conn, rows)
        print("done.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
