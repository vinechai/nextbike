#!/usr/bin/env python3
"""
scrape nextbike prague and write directly to postgresql.

runs on github actions every 10 minutes. connects to DATABASE_URL
(set to local postgres for dev, supabase url for production).
"""

import os
import sys
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone


PRAGUE_CITY_ID  = 661
LIVE_DATA_URL   = "https://maps.nextbike.net/maps/nextbike-live.flatjson"
WEATHER_URL     = "https://api.open-meteo.com/v1/forecast"
PRAGUE_LAT      = 50.0755
PRAGUE_LNG      = 14.4378
WEATHER_VARS    = "temperature_2m,precipitation,windspeed_10m,weathercode,snowfall"


def get_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("DATABASE_URL not set")
        sys.exit(1)
    try:
        return psycopg2.connect(url)
    except psycopg2.OperationalError as e:
        print(f"could not connect: {e}")
        print("check DATABASE_URL and make sure the database is reachable")
        sys.exit(1)


def fetch_weather(scrape_time):
    """fetch weather for the current utc hour from open-meteo forecast api."""
    current_hour = scrape_time.replace(minute=0, second=0, microsecond=0)
    target_str   = current_hour.strftime("%Y-%m-%dT%H:00")

    resp = requests.get(WEATHER_URL, params={
        "latitude":     PRAGUE_LAT,
        "longitude":    PRAGUE_LNG,
        "hourly":       WEATHER_VARS,
        "timezone":     "UTC",
        "past_days":    1,
        "forecast_days": 1,
    }, timeout=30)
    resp.raise_for_status()
    data = resp.json()["hourly"]

    if target_str not in data["time"]:
        print(f"  weather: hour {target_str} not in api response, skipping")
        return None

    idx = data["time"].index(target_str)
    return {
        "hour":          current_hour,
        "temperature":   data["temperature_2m"][idx],
        "precipitation": data["precipitation"][idx],
        "windspeed":     data["windspeed_10m"][idx],
        "weathercode":   data["weathercode"][idx],
        "snowfall":      data["snowfall"][idx],
    }


def upsert_weather(cur, weather: dict | None) -> None:
    if weather is None:
        return
    cur.execute(
        """
        INSERT INTO weather_hourly
            (hour, temperature, precipitation, windspeed, weathercode, snowfall)
        VALUES (%(hour)s, %(temperature)s, %(precipitation)s, %(windspeed)s,
                %(weathercode)s, %(snowfall)s)
        ON CONFLICT (hour) DO UPDATE SET
            temperature   = EXCLUDED.temperature,
            precipitation = EXCLUDED.precipitation,
            windspeed     = EXCLUDED.windspeed,
            weathercode   = EXCLUDED.weathercode,
            snowfall      = EXCLUDED.snowfall
        """,
        weather,
    )


def fetch_prague():
    resp = requests.get(LIVE_DATA_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    stations = [p for p in data["places"] if p.get("city_id") == PRAGUE_CITY_ID]
    if not stations:
        raise ValueError(f"no prague stations found (city_id={PRAGUE_CITY_ID})")
    return stations


def upsert_stations(cur, stations, scrape_time):
    rows = [
        (
            st["uid"],
            st.get("name", ""),
            float(st.get("lat", 0)),
            float(st.get("lng", 0)),
            int(st.get("bike_racks", 0) or 0),
            bool(st.get("spot", True)),
            bool(st.get("bike", False)),
            scrape_time,
        )
        for st in stations
    ]
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO stations (uid, name, lat, lng, bike_racks, is_spot, is_bike, first_seen_at)
        VALUES %s
        ON CONFLICT (uid) DO NOTHING
        """,
        rows,
        page_size=500,
    )


def insert_snapshots(cur, stations, scrape_time):
    rows = [
        (
            st["uid"],
            int(st.get("bikes", 0) or 0),
            int(st.get("bikes_available_to_rent", 0) or 0),
            int(st.get("free_racks", 0) or 0),
            int(st.get("booked_bikes", 0) or 0),
            scrape_time,
        )
        for st in stations
    ]
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO station_snapshots
            (station_uid, bikes_available, bikes_available_to_rent, free_racks, booked_bikes, scrape_time)
        VALUES %s
        """,
        rows,
        page_size=500,
    )


def update_bike_positions(cur, stations, scrape_time):
    """
    incrementally maintain one row per stay in bike_positions.
    stays are never deleted — old stays keep their last_seen_at from when the bike left.
    """
    # build current state: bike_id -> (station_uid, lat, lng)
    current = {}
    for st in stations:
        if not st.get("bike_numbers"):
            continue
        uid = st["uid"]
        lat, lng = float(st["lat"]), float(st["lng"])
        for bike_id in (b.strip() for b in st["bike_numbers"].split(",") if b.strip()):
            current[bike_id] = (uid, lat, lng)

    # DISTINCT ON with ORDER BY (bike_id, first_seen_at DESC) uses the existing index —
    # postgres reads one entry per bike, not the whole table
    cur.execute("""
        SELECT DISTINCT ON (bike_id)
            bike_id, id AS stay_id, station_uid
        FROM bike_positions
        ORDER BY bike_id, first_seen_at DESC
    """)
    prev = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    # prev: bike_id -> (stay_id, station_uid)

    continuing_ids = []  # stay ids that continue (same station) — just update last_seen_at
    new_stays = []       # (bike_id, station_uid, lat, lng, first_seen_at, last_seen_at)

    for bike_id, (station_uid, lat, lng) in current.items():
        if bike_id in prev:
            stay_id, prev_station = prev[bike_id]
            if station_uid == prev_station:
                continuing_ids.append(stay_id)
            else:
                # moved — old stay keeps its last_seen_at, we open a new one
                new_stays.append((bike_id, station_uid, lat, lng, scrape_time, scrape_time))
        else:
            # new bike or reappeared after >48h
            new_stays.append((bike_id, station_uid, lat, lng, scrape_time, scrape_time))

    if continuing_ids:
        cur.execute(
            "UPDATE bike_positions SET last_seen_at = %s WHERE id = ANY(%s)",
            (scrape_time, continuing_ids),
        )

    if new_stays:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO bike_positions (bike_id, station_uid, lat, lng, first_seen_at, last_seen_at)
            VALUES %s
            """,
            new_stays,
            page_size=500,
        )

    print(f"  bikes in this scrape: {len(current)}")
    print(f"  continuing stays:     {len(continuing_ids)}")
    print(f"  new stays opened:     {len(new_stays)}")


def main():
    print("fetching from nextbike api...")
    stations = fetch_prague()
    scrape_time = datetime.now(timezone.utc)
    print(f"  {len(stations)} stations at {scrape_time:%Y-%m-%d %H:%M:%S} UTC")

    print("fetching weather...")
    weather = fetch_weather(scrape_time)
    if weather:
        print(f"  {weather['temperature']:.1f}°C  precip={weather['precipitation']}mm  code={weather['weathercode']}")

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            upsert_stations(cur, stations, scrape_time)
            insert_snapshots(cur, stations, scrape_time)
            update_bike_positions(cur, stations, scrape_time)
            upsert_weather(cur, weather)
        conn.commit()
        print("done.")
    except Exception as e:
        conn.rollback()
        print(f"error, transaction rolled back: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
