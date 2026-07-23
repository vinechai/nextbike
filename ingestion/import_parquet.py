#!/usr/bin/env python3
"""
one-time import of existing parquet files into postgresql.

run this after starting the database:
    docker compose up -d postgres

the script skips tables that already have data, so it's safe to re-run.
to start fresh, truncate the tables first:
    TRUNCATE bike_positions, station_snapshots, stations RESTART IDENTITY CASCADE;
"""

import os
import sys
import psycopg2
import psycopg2.extras
import pandas as pd
from io import StringIO
from pathlib import Path

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://nextbike:nextbike@localhost:5432/nextbike",
)

ROOT = Path(__file__).parent.parent


def get_conn():
    try:
        return psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as e:
        print(f"could not connect to postgres: {e}")
        print("is the database running? try: docker compose up -d postgres")
        sys.exit(1)


def copy_df(conn, df, table, columns, chunk_size=100_000):
    """bulk insert via postgres COPY, much faster than row-by-row insert."""
    total = len(df)
    for start in range(0, total, chunk_size):
        chunk = df.iloc[start : start + chunk_size][columns]
        buf = StringIO()
        chunk.to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)
        with conn.cursor() as cur:
            cols = ", ".join(columns)
            cur.copy_expert(
                f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                buf,
            )
        conn.commit()
        done = min(start + chunk_size, total)
        print(f"  {done:,} / {total:,}", end="\r", flush=True)
    print(f"  {total:,} rows done")


def table_is_empty(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0] == 0


def import_stations(conn, df):
    print("importing stations...")
    if not table_is_empty(conn, "stations"):
        print("  already populated, skipping")
        return

    # one row per uid, take most recent values
    agg = dict(
        name=("name", "last"),
        lat=("lat", "last"),
        lng=("lng", "last"),
        bike_racks=("bike_racks", "last"),
        first_seen_at=("scrape_time", "min"),
    )
    if "spot" in df.columns:
        agg["is_spot"] = ("spot", "last")
    if "bike" in df.columns:
        agg["is_bike"] = ("bike", "last")

    unique = df.sort_values("scrape_time").groupby("uid", as_index=False).agg(**agg)
    unique["bike_racks"] = unique["bike_racks"].fillna(0).astype(int)
    unique["is_spot"] = unique.get("is_spot", pd.Series(True, index=unique.index)).fillna(False).astype(bool)
    unique["is_bike"] = unique.get("is_bike", pd.Series(False, index=unique.index)).fillna(False).astype(bool)

    rows = unique[["uid", "name", "lat", "lng", "bike_racks", "is_spot", "is_bike", "first_seen_at"]].values.tolist()
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO stations (uid, name, lat, lng, bike_racks, is_spot, is_bike, first_seen_at)
            VALUES %s
            ON CONFLICT (uid) DO NOTHING
            """,
            rows,
            page_size=1000,
        )
    conn.commit()
    print(f"  {len(unique):,} stations done")


def import_station_snapshots(conn, df):
    print("importing station snapshots (this will take a few minutes)...")
    if not table_is_empty(conn, "station_snapshots"):
        print("  already populated, skipping")
        return

    # only insert rows for stations we actually have in the stations table
    with conn.cursor() as cur:
        cur.execute("SELECT uid FROM stations")
        valid_uids = {row[0] for row in cur.fetchall()}

    snaps = df[df["uid"].isin(valid_uids)].copy()
    snaps = snaps.rename(columns={"uid": "station_uid", "bikes": "bikes_available"})

    for col in ["bikes_available", "bikes_available_to_rent", "free_racks", "booked_bikes"]:
        snaps[col] = snaps[col].fillna(0).astype(int)

    cols = ["station_uid", "bikes_available", "bikes_available_to_rent", "free_racks", "booked_bikes", "scrape_time"]
    copy_df(conn, snaps, "station_snapshots", cols)


def import_bike_positions(conn, df):
    """
    collapses bikes_history into per-stay rows.

    bikes_history has one row per bike per scrape. we want one row per
    stay at a station. when a bike changes its station_uid we start a new row.
    """
    print("deriving bike positions from movement history...")
    if not table_is_empty(conn, "bike_positions"):
        print("  already populated, skipping")
        return

    with conn.cursor() as cur:
        cur.execute("SELECT uid FROM stations")
        valid_uids = {row[0] for row in cur.fetchall()}

    df = df[df["station_uid"].isin(valid_uids)].copy()
    df = df.sort_values(["bike_id", "scrape_time"])

    # detect when a bike changes station
    df["prev_station"] = df.groupby("bike_id")["station_uid"].shift(1)
    df["new_stay"] = (df["station_uid"] != df["prev_station"]) | df["prev_station"].isna()
    # cumsum turns boolean flags into a monotonically increasing group id per bike
    df["stay_id"] = df.groupby("bike_id")["new_stay"].cumsum()

    positions = (
        df.groupby(["bike_id", "stay_id"], as_index=False)
        .agg(
            station_uid=("station_uid", "first"),
            lat=("lat", "first"),
            lng=("lng", "first"),
            first_seen_at=("scrape_time", "min"),
            last_seen_at=("scrape_time", "max"),
        )
        .drop(columns=["stay_id"])
    )

    cols = ["bike_id", "station_uid", "lat", "lng", "first_seen_at", "last_seen_at"]
    copy_df(conn, positions, "bike_positions", cols)


def main():
    stations_path = ROOT / "stations_history.parquet"
    bikes_path = ROOT / "bikes_history.parquet"

    for p in [stations_path, bikes_path]:
        if not p.exists():
            print(f"file not found: {p}")
            print("download the parquet files from backblaze first")
            sys.exit(1)

    print("loading parquet files...")
    stations_df = pd.read_parquet(stations_path)
    bikes_df = pd.read_parquet(bikes_path)
    print(f"  stations_history: {len(stations_df):,} rows")
    print(f"  bikes_history:    {len(bikes_df):,} rows")

    conn = get_conn()
    try:
        import_stations(conn, stations_df)
        import_station_snapshots(conn, stations_df)
        import_bike_positions(conn, bikes_df)
        print("\ndone.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
