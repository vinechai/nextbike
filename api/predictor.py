"""
loads model and station metadata once at startup.
builds feature vectors on request and runs inference.
"""

import json
import re
import pathlib
import numpy as np
import pandas as pd
import lightgbm as lgb
import psycopg2
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    _PRAGUE = ZoneInfo("Europe/Prague")
except Exception:
    import pytz
    _PRAGUE = pytz.timezone("Europe/Prague")

DATA_DIR   = pathlib.Path(__file__).parent.parent / "data"
MODEL_PATH = DATA_DIR / "model.lgb"
FEAT_PATH  = DATA_DIR / "feature_cols.json"
GEO_PATH   = DATA_DIR / "station_geo.parquet"

# geo column prefixes — must match enrich_stations.py naming
_GEO_PREFIXES = ("dist_", "n_metro", "n_tram", "n_cafe", "n_park", "n_office", "elevation")
_WEATHER_COLS = {"temperature", "precipitation", "windspeed", "weathercode", "snowfall",
                 "is_raining", "is_snowing"}


def _extract_district(name: str) -> int:
    m = re.match(r"^P(\d+)", name)
    return int(m.group(1)) if m else 0


def _prague_time_features(dt_utc: datetime) -> dict:
    prague = dt_utc.astimezone(_PRAGUE)
    h, dow, month = prague.hour, prague.weekday(), prague.month
    return {
        "hour_of_day": h,
        "dow":         dow,
        "month":       month,
        "is_weekend":  int(dow >= 5),
        "season":      int(month >= 5),
        "hour_sin":    np.sin(2 * np.pi * h / 24),
        "hour_cos":    np.cos(2 * np.pi * h / 24),
        "dow_sin":     np.sin(2 * np.pi * dow / 7),
        "dow_cos":     np.cos(2 * np.pi * dow / 7),
        "month_sin":   np.sin(2 * np.pi * month / 12),
        "month_cos":   np.cos(2 * np.pi * month / 12),
    }


class Predictor:
    def __init__(self, database_url: str) -> None:
        self.db_url = database_url
        self.model = lgb.Booster(model_file=str(MODEL_PATH))
        self.feature_cols: list[str] = json.loads(FEAT_PATH.read_text())
        self._geo_cols = [c for c in self.feature_cols if c.startswith(_GEO_PREFIXES)]
        self._load_stations()

    # ── startup ──────────────────────────────────────────────────────────────

    def _load_stations(self) -> None:
        conn = psycopg2.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT uid AS station_uid, name, lat, lng, bike_racks FROM stations WHERE is_spot = TRUE")
            cols = [d[0] for d in cur.description]
            df = pd.DataFrame(cur.fetchall(), columns=cols)
        conn.close()

        df["district"] = df["name"].apply(_extract_district)
        df["bike_racks"] = df["bike_racks"].fillna(0).astype(int)

        if GEO_PATH.exists():
            geo = pd.read_parquet(GEO_PATH)
            geo_cols = [c for c in geo.columns if c not in ("station_uid", "lat", "lng")]
            for col in geo_cols:
                geo[col] = pd.to_numeric(geo[col], errors="coerce")
            df = df.merge(geo[["station_uid"] + geo_cols], on="station_uid", how="left")

        self.stations: pd.DataFrame = df.set_index("station_uid")

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _get_weather(self, target_utc: datetime) -> dict:
        target_h = target_utc.replace(minute=0, second=0, microsecond=0, tzinfo=None)
        conn = psycopg2.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT temperature, precipitation, windspeed, weathercode, snowfall
                FROM weather_hourly
                ORDER BY ABS(EXTRACT(EPOCH FROM (hour - %s::timestamptz)))
                LIMIT 1
            """, (target_h,))
            row = cur.fetchone()
        conn.close()
        if row is None:
            return {c: np.nan for c in ("temperature", "precipitation", "windspeed", "weathercode", "snowfall")}
        w = dict(zip(("temperature", "precipitation", "windspeed", "weathercode", "snowfall"), row))
        w["is_raining"] = int(float(w.get("precipitation") or 0) > 0)
        w["is_snowing"]  = int(float(w.get("snowfall") or 0)  > 0)
        return w

    def _get_lags_one(self, station_uid: int, target_utc: datetime) -> dict:
        target_h = target_utc.replace(minute=0, second=0, microsecond=0)
        conn = psycopg2.connect(self.db_url)
        result: dict = {}
        with conn.cursor() as cur:
            for col, offset in [("lag_1h", 1), ("lag_24h", 24), ("lag_168h", 168)]:
                lookup = target_h - timedelta(hours=offset)
                cur.execute("""
                    SELECT AVG(bikes_available_to_rent)
                    FROM station_snapshots
                    WHERE station_uid = %s
                      AND scrape_time >= %s AND scrape_time < %s + INTERVAL '1 hour'
                """, (station_uid, lookup, lookup))
                row = cur.fetchone()
                result[col] = float(row[0]) if row and row[0] is not None else np.nan
        conn.close()
        return result

    def _get_lags_all(self, target_utc: datetime) -> dict[int, dict]:
        """one round-trip to get lag features for all stations."""
        target_h = target_utc.replace(minute=0, second=0, microsecond=0)
        offsets = {"lag_1h": 1, "lag_24h": 24, "lag_168h": 168}
        lookup_hours = {col: target_h - timedelta(hours=n) for col, n in offsets.items()}

        conn = psycopg2.connect(self.db_url)
        lags: dict[int, dict] = {}
        with conn.cursor() as cur:
            for col, lookup in lookup_hours.items():
                cur.execute("""
                    SELECT station_uid, AVG(bikes_available_to_rent)
                    FROM station_snapshots
                    WHERE scrape_time >= %s AND scrape_time < %s + INTERVAL '1 hour'
                    GROUP BY station_uid
                """, (lookup, lookup))
                for uid, val in cur.fetchall():
                    lags.setdefault(uid, {})[col] = float(val) if val is not None else np.nan
        conn.close()
        return lags

    # ── inference ─────────────────────────────────────────────────────────────

    def _assemble_row(self, uid: int, time_feats: dict, weather: dict, lag: dict) -> dict:
        st = self.stations.loc[uid]
        row: dict = {c: np.nan for c in self.feature_cols}
        for k in ("lat", "lng", "bike_racks", "district"):
            row[k] = st.get(k)
        for col in self._geo_cols:
            row[col] = st.get(col, np.nan)
        row.update(time_feats)
        row.update(weather)
        row.update(lag)
        return row

    def predict_one(self, station_uid: int, target_utc: datetime) -> float:
        if station_uid not in self.stations.index:
            raise KeyError(f"station {station_uid} not found")
        time_feats = _prague_time_features(target_utc)
        weather    = self._get_weather(target_utc)
        lag        = self._get_lags_one(station_uid, target_utc)
        row        = self._assemble_row(station_uid, time_feats, weather, lag)
        pred = self.model.predict(pd.DataFrame([row])[self.feature_cols])[0]
        return max(0.0, float(pred))

    def predict_all(self, target_utc: datetime) -> list[dict]:
        """predict for every station — used by the dashboard map."""
        time_feats = _prague_time_features(target_utc)
        weather    = self._get_weather(target_utc)
        lags       = self._get_lags_all(target_utc)

        rows = [
            self._assemble_row(uid, time_feats, weather, lags.get(uid, {}))
            for uid in self.stations.index
        ]
        feat_df = pd.DataFrame(rows)[self.feature_cols]
        preds   = np.maximum(0.0, self.model.predict(feat_df))

        out = []
        for uid, pred in zip(self.stations.index, preds):
            st = self.stations.loc[uid]
            out.append({
                "station_uid": int(uid),
                "name":        st["name"],
                "lat":         float(st["lat"]),
                "lng":         float(st["lng"]),
                "predicted_avg_available": round(float(pred), 2),
            })
        return out
