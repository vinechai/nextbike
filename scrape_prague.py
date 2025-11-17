import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

PRAGUE_CITY_ID = 661
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

LIVE_DATA_URL = "https://maps.nextbike.net/maps/nextbike-live.flatjson"


def append_parquet(df, path):
    if path.exists():
        old = pd.read_parquet(path)
        df = pd.concat([old, df], ignore_index=True)
    df.to_parquet(path, index=False)


def scrape_prague_once():
    resp = requests.get(LIVE_DATA_URL)
    resp.raise_for_status()
    data = resp.json()

    scrape_time = datetime.now(timezone.utc)

    # -------------------------------------------
    # 1. STATIONS (FROM places)
    # -------------------------------------------
    stations = [
        place for place in data["places"]
        if place.get("city_id") == PRAGUE_CITY_ID
    ]

    stations_df = pd.json_normalize(stations)
    stations_df["scrape_time"] = scrape_time

    stations_df.to_parquet(DATA_DIR / "stations_latest.parquet", index=False)

    # -------------------------------------------
    # 2. BIKES (EXTRACTED FROM stations)
    # -------------------------------------------
    bike_rows = []

    for st in stations:
        bike_numbers = st.get("bike_numbers")
        if not bike_numbers:
            continue
        
        # convert "485396,481489" → ["485396", "481489"]
        bikes = [b.strip() for b in bike_numbers.split(",")]

        for bike_id in bikes:
            bike_rows.append({
                "bike_id": bike_id,
                "station_uid": st["uid"],
                "station_name": st["name"],
                "lat": st["lat"],
                "lng": st["lng"],
                "scrape_time": scrape_time
            })

    bikes_df = pd.DataFrame(bike_rows)

    bikes_df.to_parquet(DATA_DIR / "bikes_latest.parquet", index=False)
    append_parquet(bikes_df, DATA_DIR / "bikes_history.parquet")

    print(f"[OK] Scraped Prague at {scrape_time}")
    print(f"     Stations: {len(stations)}")
    print(f"     Bikes: {len(bikes_df)}")


if __name__ == "__main__":
    scrape_prague_once()
