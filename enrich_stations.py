#!/usr/bin/env python3
"""
compute geospatial features for each bike station from openstreetmap and elevation data.
run once, output saved to data/station_geo.parquet.

features per station:
  elevation_m: meters above sea level (srtm 30m via opentopodata)
  dist_metro_m: distance to nearest metro entrance
  n_metro_500m: metro entrances within 500m
  dist_tram_m: distance to nearest tram stop
  n_tram_300m: tram stops within 300m
  n_cafe_300m: cafes + restaurants within 300m (activity density proxy)
  dist_park_m: distance to nearest park centroid
  n_park_500m: parks within 500m
  n_office_500m: office buildings within 500m

data sources: overpass api (osm), opentopodata api (both free, no api key).

usage:
    DATABASE_URL=postgresql://... python enrich_stations.py
"""

import os, sys, time, requests
import numpy as np
import pandas as pd
import psycopg2
import pathlib
from sklearn.neighbors import BallTree

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://nextbike:nextbike@localhost:5432/nextbike")
OUTFILE      = pathlib.Path("data/station_geo.parquet")
EARTH_R      = 6_371_000.0


# ─── helpers ─────────────────────────────────────────────────────────────────

def to_rad(df: pd.DataFrame) -> np.ndarray:
    return np.radians(df[["lat", "lon"]].values)


def nearest_m(stations_rad: np.ndarray, targets_rad: np.ndarray) -> np.ndarray:
    """distance in meters from each station to the nearest target."""
    if len(targets_rad) == 0:
        return np.full(len(stations_rad), np.nan)
    dist_rad, _ = BallTree(targets_rad, metric="haversine").query(stations_rad, k=1)
    return (dist_rad[:, 0] * EARTH_R).round(1)


def count_within(stations_rad: np.ndarray, targets_rad: np.ndarray, radius_m: float) -> np.ndarray:
    """count targets within radius_m of each station."""
    if len(targets_rad) == 0:
        return np.zeros(len(stations_rad), dtype=int)
    r_rad = radius_m / EARTH_R
    return BallTree(targets_rad, metric="haversine").query_radius(stations_rad, r=r_rad, count_only=True).astype(int)


# ─── overpass ────────────────────────────────────────────────────────────────

# bounding box covering all of prague: south,west,north,east
BBOX = "(49.94,14.22,50.18,14.71)"


HEADERS = {"User-Agent": "nextbike-prague-portfolio/1.0"}

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]


def overpass(query: str) -> list[dict]:
    """try each overpass mirror in turn, retry once per endpoint on transient errors."""
    last_err = None
    for endpoint in OVERPASS_ENDPOINTS:
        for attempt in range(2):
            try:
                r = requests.post(
                    endpoint,
                    data={"data": query},
                    headers=HEADERS,
                    timeout=90,
                )
                r.raise_for_status()
                return r.json()["elements"]
            except (requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
                last_err = e
                wait = 15 if attempt == 0 else 0
                if attempt == 0:
                    print(f"    {endpoint.split('/')[2]} failed ({e}), retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"    {endpoint.split('/')[2]} failed again, trying next mirror...")
    raise RuntimeError(f"all overpass mirrors failed: {last_err}")


def elements_to_points(elems: list[dict]) -> pd.DataFrame:
    rows = []
    for e in elems:
        if e["type"] == "node":
            rows.append({"lat": e["lat"], "lon": e["lon"]})
        elif "center" in e:
            rows.append({"lat": e["center"]["lat"], "lon": e["center"]["lon"]})
    return pd.DataFrame(rows) if rows else pd.DataFrame({"lat": pd.Series([], dtype=float),
                                                          "lon": pd.Series([], dtype=float)})


def fetch_osm(label: str, query: str) -> pd.DataFrame:
    print(f"  osm: {label}...")
    df = elements_to_points(overpass(query))
    print(f"    {len(df)} features found")
    time.sleep(2)  # be polite to the free api
    return df


# ─── elevation ───────────────────────────────────────────────────────────────

def fetch_elevation(lats: list[float], lons: list[float], batch: int = 100) -> list[float | None]:
    """elevation in meters from opentopodata (srtm30m, 30m resolution, free)."""
    elevations: list[float | None] = []
    total = len(lats)
    for i in range(0, total, batch):
        bl = lats[i:i + batch]
        bn = lons[i:i + batch]
        locations = "|".join(f"{la:.6f},{lo:.6f}" for la, lo in zip(bl, bn))
        try:
            r = requests.get(
                "https://api.opentopodata.org/v1/srtm30m",
                params={"locations": locations},
                timeout=60,
            )
            r.raise_for_status()
            for res in r.json()["results"]:
                elevations.append(res.get("elevation"))
        except Exception as e:
            print(f"    elevation batch {i//batch + 1} failed: {e}")
            elevations.extend([None] * len(bl))
        print(f"    elevation: {min(i + batch, total)}/{total}")
        time.sleep(1.5)
    return elevations


# ─── main ────────────────────────────────────────────────────────────────────

def main() -> None:
    print("loading stations from postgres...")
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        cur.execute("SELECT uid, lat, lng FROM stations WHERE is_spot = TRUE")
        stations = pd.DataFrame(cur.fetchall(), columns=["uid", "lat", "lon"])
    conn.close()
    print(f"  {len(stations):,} stations")

    st_rad = to_rad(stations)

    # metro — station=subway tags the underground metro stations in prague
    metro = fetch_osm("metro stations", f"""
        [out:json][timeout:60];
        node["station"="subway"]{BBOX};
        out;
    """)

    # tram stops
    tram = fetch_osm("tram stops", f"""
        [out:json][timeout:60];
        node["railway"="tram_stop"]{BBOX};
        out;
    """)

    # cafes and restaurants — proxy for pedestrian activity density
    cafes = fetch_osm("cafes + restaurants", f"""
        [out:json][timeout:90];
        (
          node["amenity"~"^(cafe|restaurant|bar|fast_food)$"]{BBOX};
          way["amenity"~"^(cafe|restaurant|bar|fast_food)$"]{BBOX};
        );
        out center;
    """)

    # parks
    parks = fetch_osm("parks", f"""
        [out:json][timeout:90];
        (
          way["leisure"="park"]{BBOX};
          relation["leisure"="park"]{BBOX};
        );
        out center;
    """)

    # offices (proxy for employment density)
    offices = fetch_osm("offices", f"""
        [out:json][timeout:90];
        (
          node["office"]{BBOX};
          way["office"]{BBOX};
          way["building"="office"]{BBOX};
        );
        out center;
    """)

    # proximity features
    print("computing proximity features...")
    result = stations.rename(columns={"uid": "station_uid", "lon": "lng"}).copy()

    specs = [
        ("metro",  metro,   500),
        ("tram",   tram,    300),
        ("cafe",   cafes,   300),
        ("park",   parks,   500),
        ("office", offices, 500),
    ]
    for name, df, radius in specs:
        if len(df) > 0:
            pts = to_rad(df)
            result[f"dist_{name}_m"]    = nearest_m(st_rad, pts)
            result[f"n_{name}_{radius}m"] = count_within(st_rad, pts, radius)
        else:
            result[f"dist_{name}_m"]    = np.nan
            result[f"n_{name}_{radius}m"] = 0

    # elevation
    print("fetching elevation from opentopodata (this takes ~2 min)...")
    result["elevation_m"] = fetch_elevation(
        stations["lat"].tolist(),
        stations["lon"].tolist(),
    )

    OUTFILE.parent.mkdir(exist_ok=True)
    result.to_parquet(OUTFILE, index=False)
    print(f"\nsaved to {OUTFILE}  ({OUTFILE.stat().st_size / 1e3:.0f} KB)")
    print(result[["dist_metro_m", "n_metro_500m", "n_tram_300m", "n_cafe_300m",
                  "n_park_500m", "n_office_500m", "elevation_m"]].describe().round(1))


if __name__ == "__main__":
    main()
