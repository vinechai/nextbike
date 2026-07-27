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

DATA_DIR      = pathlib.Path(__file__).parent.parent / "data"
MODEL_PATH    = DATA_DIR / "model.lgb"
FEAT_PATH     = DATA_DIR / "feature_cols.json"
GEO_PATH      = DATA_DIR / "station_geo.parquet"
FEATURES_PATH = DATA_DIR / "features.parquet"

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
        "hour_sin":    np.sin(2 * np.pi * h / 24),
        "hour_cos":    np.cos(2 * np.pi * h / 24),
        "dow_sin":     np.sin(2 * np.pi * dow / 7),
        "dow_cos":     np.cos(2 * np.pi * dow / 7),
        "month_sin":   np.sin(2 * np.pi * month / 12),
        "month_cos":   np.cos(2 * np.pi * month / 12),
    }


def _closest_hour_avg(cur, station_uid: int | None,
                      window_start: datetime, window_end: datetime,
                      center: datetime | None = None) -> tuple:
    """find the hourly-average closest to center (or most recent if center is None).

    returns (avg_bikes, latest_scrape_in_that_hour) or (None, None) if no data found.
    station_uid=None queries all stations for a batch result.
    """
    order = (
        "date_trunc('hour', scrape_time) DESC"
        if center is None
        else "ABS(EXTRACT(EPOCH FROM (date_trunc('hour', scrape_time) - %s::timestamptz)))"
    )

    if station_uid is not None:
        params_filter = [station_uid, window_start, window_end]
    else:
        params_filter = [window_start, window_end]

    if center is not None:
        params_order = [center]
    else:
        params_order = []

    if station_uid is not None:
        # single-station: return (avg, latest_scrape)
        cur.execute(f"""
            WITH bucketed AS (
                SELECT
                    date_trunc('hour', scrape_time) AS hour_slot,
                    AVG(bikes_available_to_rent)    AS avg_bikes,
                    MAX(scrape_time)                AS latest_scrape
                FROM station_snapshots
                WHERE station_uid = %s
                  AND scrape_time >= %s AND scrape_time < %s
                GROUP BY date_trunc('hour', scrape_time)
                ORDER BY {order}
                LIMIT 1
            )
            SELECT avg_bikes, latest_scrape FROM bucketed
        """, params_filter + params_order)
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0]), row[1]
        return None, None
    else:
        # batch: return {station_uid: avg_bikes}
        cur.execute(f"""
            WITH bucketed AS (
                SELECT
                    station_uid,
                    date_trunc('hour', scrape_time) AS hour_slot,
                    AVG(bikes_available_to_rent)    AS avg_bikes,
                    ROW_NUMBER() OVER (
                        PARTITION BY station_uid
                        ORDER BY {order}
                    ) AS rn
                FROM station_snapshots
                WHERE scrape_time >= %s AND scrape_time < %s
                GROUP BY station_uid, date_trunc('hour', scrape_time)
            )
            SELECT station_uid, avg_bikes FROM bucketed WHERE rn = 1
        """, params_filter + params_order)
        return {uid: float(val) for uid, val in cur.fetchall() if val is not None}, None


class Predictor:
    def __init__(self, database_url: str) -> None:
        self.db_url = database_url
        self.model = lgb.Booster(model_file=str(MODEL_PATH))
        self.feature_cols: list[str] = json.loads(FEAT_PATH.read_text())
        self._geo_cols = [c for c in self.feature_cols if c.startswith(_GEO_PREFIXES)]
        self._load_stations()
        self._load_lag_baseline()

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

    def _load_lag_baseline(self) -> None:
        """last-resort fallback: historical median per (station, hour, dow).

        used only if scraper has been down more than 6 hours (lag_1h window)
        or more than 3 hours around the 24h / 168h targets.
        """
        if not FEATURES_PATH.exists():
            self._lag_baseline: dict = {}
            return
        df = pd.read_parquet(
            FEATURES_PATH,
            columns=["station_uid", "hour_of_day", "dow", "avg_available"],
        )
        self._lag_baseline = (
            df.groupby(["station_uid", "hour_of_day", "dow"])["avg_available"]
            .median()
            .to_dict()
        )

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
        """get lag values for one station with closest-hour logic and debug timestamps."""
        target_h = target_utc.replace(minute=0, second=0, microsecond=0)
        conn = psycopg2.connect(self.db_url)
        result: dict = {}
        with conn.cursor() as cur:
            # lag_1h: most recent hourly avg in the last 6 hours
            val, ts = _closest_hour_avg(cur, station_uid,
                                        target_h - timedelta(hours=6), target_h)
            result["lag_1h"]    = val if val is not None else np.nan
            result["lag_1h_ts"] = ts.isoformat() if ts else None

            # lag_24h: hourly avg closest to 24h ago (±3h window)
            c24 = target_h - timedelta(hours=24)
            val, ts = _closest_hour_avg(cur, station_uid,
                                        c24 - timedelta(hours=3), c24 + timedelta(hours=3),
                                        center=c24)
            result["lag_24h"]    = val if val is not None else np.nan
            result["lag_24h_ts"] = ts.isoformat() if ts else None

            # lag_168h: hourly avg closest to 168h ago (±3h window)
            c168 = target_h - timedelta(hours=168)
            val, ts = _closest_hour_avg(cur, station_uid,
                                        c168 - timedelta(hours=3), c168 + timedelta(hours=3),
                                        center=c168)
            result["lag_168h"]    = val if val is not None else np.nan
            result["lag_168h_ts"] = ts.isoformat() if ts else None
        conn.close()
        return result

    def _get_lags_all(self, target_utc: datetime) -> dict[int, dict]:
        """batch lag query for all stations:closest-hour logic, no timestamps."""
        target_h = target_utc.replace(minute=0, second=0, microsecond=0)
        conn = psycopg2.connect(self.db_url)
        lags: dict[int, dict] = {}
        with conn.cursor() as cur:
            vals, _ = _closest_hour_avg(cur, None,
                                        target_h - timedelta(hours=6), target_h)
            for uid, v in vals.items():
                lags.setdefault(uid, {})["lag_1h"] = v

            c24 = target_h - timedelta(hours=24)
            vals, _ = _closest_hour_avg(cur, None,
                                        c24 - timedelta(hours=3), c24 + timedelta(hours=3),
                                        center=c24)
            for uid, v in vals.items():
                lags.setdefault(uid, {})["lag_24h"] = v

            c168 = target_h - timedelta(hours=168)
            vals, _ = _closest_hour_avg(cur, None,
                                        c168 - timedelta(hours=3), c168 + timedelta(hours=3),
                                        center=c168)
            for uid, v in vals.items():
                lags.setdefault(uid, {})["lag_168h"] = v
        conn.close()
        return lags

    # ── inference ─────────────────────────────────────────────────────────────

    def _fill_lag_baseline(self, uid: int, row: dict, h: int, d: int) -> dict:
        """fill any remaining NaN lags with historical medians:last resort only."""
        sources = {}
        if np.isnan(row.get("lag_168h", np.nan)):
            row["lag_168h"] = self._lag_baseline.get((uid, h, d), np.nan)
            sources["lag_168h"] = "historical median"
        if np.isnan(row.get("lag_24h", np.nan)):
            d_prev = (d - 1) % 7
            row["lag_24h"] = self._lag_baseline.get((uid, h, d_prev), np.nan)
            sources["lag_24h"] = "historical median"
        if np.isnan(row.get("lag_1h", np.nan)):
            h_prev = (h - 1) % 24
            d_prev = (d - 1) % 7 if h == 0 else d
            row["lag_1h"] = self._lag_baseline.get((uid, h_prev, d_prev), np.nan)
            sources["lag_1h"] = "historical median"
        return sources

    def _assemble_row(self, uid: int, time_feats: dict, weather: dict, lag: dict) -> tuple[dict, dict]:
        """build a model input row. returns (row_dict, fallback_sources)."""
        st = self.stations.loc[uid]
        row: dict = {c: np.nan for c in self.feature_cols}
        for k in ("lat", "lng", "bike_racks", "district"):
            row[k] = st.get(k)
        for col in self._geo_cols:
            row[col] = st.get(col, np.nan)
        row.update(time_feats)
        row.update(weather)
        # strip _ts debug keys before feeding to model
        lag_feats = {k: v for k, v in lag.items() if not k.endswith("_ts")}
        row.update(lag_feats)
        sources = self._fill_lag_baseline(uid, row, int(time_feats["hour_of_day"]), int(time_feats["dow"]))
        return row, sources

    def predict_one(self, station_uid: int, target_utc: datetime) -> float:
        if station_uid not in self.stations.index:
            raise KeyError(f"station {station_uid} not found")
        time_feats = _prague_time_features(target_utc)
        weather    = self._get_weather(target_utc)
        lag        = self._get_lags_one(station_uid, target_utc)
        row, _     = self._assemble_row(station_uid, time_feats, weather, lag)
        pred = self.model.predict(pd.DataFrame([row])[self.feature_cols])[0]
        return max(0.0, float(pred))

    def predict_one_debug(self, station_uid: int, target_utc: datetime) -> dict:
        """full prediction with lag timestamps and geo info:for the debug endpoint."""
        if station_uid not in self.stations.index:
            raise KeyError(f"station {station_uid} not found")
        st = self.stations.loc[station_uid]
        time_feats = _prague_time_features(target_utc)
        weather    = self._get_weather(target_utc)
        lag        = self._get_lags_one(station_uid, target_utc)
        row, sources = self._assemble_row(station_uid, time_feats, weather, lag)
        pred = max(0.0, float(self.model.predict(pd.DataFrame([row])[self.feature_cols])[0]))

        result: dict = {
            "station_uid": station_uid,
            "name":        st["name"],
            "district":    int(st.get("district", 0)),
            "bike_racks":  int(st.get("bike_racks", 0)),
            "lat":         float(st["lat"]),
            "lng":         float(st["lng"]),
            "predicted_avg_available": round(pred, 2),
            # lag values and where they came from
            "lag_1h":       round(float(row["lag_1h"]), 2) if not np.isnan(row.get("lag_1h", np.nan)) else None,
            "lag_1h_ts":    lag.get("lag_1h_ts"),
            "lag_1h_source": sources.get("lag_1h", "live"),
            "lag_24h":      round(float(row["lag_24h"]), 2) if not np.isnan(row.get("lag_24h", np.nan)) else None,
            "lag_24h_ts":   lag.get("lag_24h_ts"),
            "lag_24h_source": sources.get("lag_24h", "live"),
            "lag_168h":     round(float(row["lag_168h"]), 2) if not np.isnan(row.get("lag_168h", np.nan)) else None,
            "lag_168h_ts":  lag.get("lag_168h_ts"),
            "lag_168h_source": sources.get("lag_168h", "live"),
            # weather
            "temperature":  weather.get("temperature"),
            "precipitation": weather.get("precipitation"),
            "windspeed":    weather.get("windspeed"),
        }
        # add geo columns
        for col in self._geo_cols:
            val = st.get(col)
            result[col] = float(val) if val is not None and not (isinstance(val, float) and np.isnan(val)) else None
        return result

    def predict_all(self, target_utc: datetime) -> list[dict]:
        """predict for every station:used by the dashboard map."""
        time_feats = _prague_time_features(target_utc)
        weather    = self._get_weather(target_utc)
        lags       = self._get_lags_all(target_utc)

        rows = []
        for uid in self.stations.index:
            row, _ = self._assemble_row(uid, time_feats, weather, lags.get(uid, {}))
            rows.append(row)

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

    def get_timeline(self, station_uid: int, hours: int = 24) -> list[dict]:
        """return raw scrape data for the last N hours for one station."""
        conn = psycopg2.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT scrape_time, bikes_available_to_rent
                FROM station_snapshots
                WHERE station_uid = %s
                  AND scrape_time >= NOW() - INTERVAL '1 hour' * %s
                ORDER BY scrape_time
            """, (station_uid, hours))
            rows = cur.fetchall()
        conn.close()
        return [
            {"scrape_time": r[0].isoformat(), "bikes_available_to_rent": int(r[1]) if r[1] is not None else None}
            for r in rows
        ]
