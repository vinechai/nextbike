"""
loads model and station metadata once at startup.
builds feature vectors on request and runs inference.
"""

import json
import re
import pathlib
from contextlib import contextmanager
import numpy as np
import pandas as pd
import lightgbm as lgb
import psycopg2
from psycopg2 import pool as pg_pool
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    _PRAGUE = ZoneInfo("Europe/Prague")
except Exception:
    import pytz
    _PRAGUE = pytz.timezone("Europe/Prague")

try:
    import holidays as _holidays_lib
    _HAS_HOLIDAYS = True
except ImportError:
    _HAS_HOLIDAYS = False

DATA_DIR      = pathlib.Path(__file__).parent.parent / "data"
MODEL_PATH    = DATA_DIR / "model.lgb"
FEAT_PATH     = DATA_DIR / "feature_cols.json"
GEO_PATH      = DATA_DIR / "station_geo.parquet"
FEATURES_PATH = DATA_DIR / "features.parquet"

_GEO_PREFIXES  = ("dist_", "n_metro", "n_tram", "n_cafe", "n_park", "n_office", "elevation")
_WEATHER_COLS  = ("temperature", "precipitation", "windspeed", "weathercode", "snowfall",
                  "shortwave_radiation", "tsun")
_REB_THRESHOLD = 3.0   # avg_bikes jump per hour that signals a rebalancing event

# ±90min for all lag anchors — matches 02_features.ipynb merge_asof tolerance
LAG_WINDOW = timedelta(minutes=90)


def _extract_district(name: str) -> int:
    m = re.match(r"^P(\d+)", name)
    return int(m.group(1)) if m else 0


def _prague_time_features(dt_utc: datetime) -> dict:
    prague = dt_utc.astimezone(_PRAGUE)
    h, dow, month = prague.hour, prague.weekday(), prague.month
    is_hol = 0
    if _HAS_HOLIDAYS:
        cz = _holidays_lib.Czechia(years=prague.year)
        is_hol = int(prague.date() in cz)
    return {
        "hour_of_day": h,
        "dow":         dow,
        "month":       month,
        "is_weekend":  int(dow >= 5),
        "is_holiday":  is_hol,
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

    params_order = [center] if center is not None else []

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
        # params_order (center) must come first because ORDER BY appears before WHERE in SQL
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
        """, params_order + params_filter)
        return {uid: float(val) for uid, val in cur.fetchall() if val is not None}, None


class Predictor:
    def __init__(self, database_url: str) -> None:
        self.db_url = database_url
        self._pool = pg_pool.ThreadedConnectionPool(1, 10, database_url)
        self.model = lgb.Booster(model_file=str(MODEL_PATH))
        self.feature_cols: list[str] = json.loads(FEAT_PATH.read_text())
        self._geo_cols = [c for c in self.feature_cols if c.startswith(_GEO_PREFIXES)]
        self._load_lag_baseline()   # sets _active_uids and _station_stats
        self._load_stations()       # uses _active_uids to mark is_active

    @contextmanager
    def _db(self):
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    # ── startup ──────────────────────────────────────────────────────────────

    def _load_stations(self) -> None:
        with self._db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT uid AS station_uid, name, lat, lng, bike_racks "
                    "FROM stations WHERE is_spot = TRUE"
                )
                cols = [d[0] for d in cur.description]
                df = pd.DataFrame(cur.fetchall(), columns=cols)

        df["district"]  = df["name"].apply(_extract_district)
        df["bike_racks"] = df["bike_racks"].fillna(0).astype(int)
        df["is_active"]  = df["station_uid"].isin(self._active_uids)

        if GEO_PATH.exists():
            geo = pd.read_parquet(GEO_PATH)
            geo_cols = [c for c in geo.columns if c not in ("station_uid", "lat", "lng")]
            for col in geo_cols:
                geo[col] = pd.to_numeric(geo[col], errors="coerce")
            df = df.merge(geo[["station_uid"] + geo_cols], on="station_uid", how="left")

        self.stations: pd.DataFrame = df.set_index("station_uid")

    def _load_lag_baseline(self) -> None:
        """load historical medians and station-level stats from features.parquet."""
        if not FEATURES_PATH.exists():
            self._lag_baseline: dict = {}
            self._station_stats: dict = {}
            self._active_uids: set = set()
            return
        # load only needed columns; fall back if optional ones are absent
        try:
            df = pd.read_parquet(
                FEATURES_PATH,
                columns=["station_uid", "hour_of_day", "dow", "avg_available",
                         "reb_per_week", "avail_std"],
            )
        except Exception:
            df = pd.read_parquet(
                FEATURES_PATH,
                columns=["station_uid", "hour_of_day", "dow", "avg_available"],
            )
            df["reb_per_week"] = np.nan
            df["avail_std"]    = np.nan

        self._lag_baseline = (
            df.groupby(["station_uid", "hour_of_day", "dow"])["avg_available"]
            .median()
            .to_dict()
        )
        # station-level stats: stable characteristics used at inference time
        agg = df.groupby("station_uid").agg(
            avail_std=("avail_std", "mean"),
            reb_per_week=("reb_per_week", "median"),
            station_mean_avail=("avg_available", "mean"),
        )
        self._station_stats: dict = agg.to_dict(orient="index")
        self._active_uids: set = set(df["station_uid"].unique())

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _get_weather(self, target_utc: datetime) -> dict:
        # convert to prague local time: weather_hourly stores local-time hours
        prague   = target_utc.astimezone(_PRAGUE)
        target_h = prague.replace(minute=0, second=0, microsecond=0, tzinfo=None)
        with self._db() as conn:
            with conn.cursor() as cur:
                # SELECT * so missing columns (shortwave_radiation, tsun) don't crash
                cur.execute("""
                    SELECT *
                    FROM weather_hourly
                    ORDER BY ABS(EXTRACT(EPOCH FROM (hour - %s::timestamptz)))
                    LIMIT 1
                """, (target_h,))
                desc = cur.description
                row  = cur.fetchone()
        if row is None or desc is None:
            return {c: np.nan for c in _WEATHER_COLS}
        available = {d[0]: (float(v) if v is not None else np.nan)
                     for d, v in zip(desc, row) if d[0] != "hour"}
        return {c: available.get(c, np.nan) for c in _WEATHER_COLS}

    def _get_lags_one(self, station_uid: int, target_utc: datetime) -> dict:
        """get lag values for one station with closest-hour logic and debug timestamps."""
        target_h = target_utc.replace(minute=0, second=0, microsecond=0)
        now_h    = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        result: dict = {}
        with self._db() as conn:
            with conn.cursor() as cur:
                # lag_1h: kept for debug display only — not a model feature
                c1 = now_h - timedelta(hours=1)
                val, ts = _closest_hour_avg(cur, station_uid,
                                            c1 - LAG_WINDOW, min(c1 + LAG_WINDOW, now_h),
                                            center=c1)
                result["lag_1h"]    = val if val is not None else np.nan
                result["lag_1h_ts"] = ts.isoformat() if ts else None

                c24 = target_h - timedelta(hours=24)
                val, ts = _closest_hour_avg(cur, station_uid,
                                            c24 - LAG_WINDOW, c24 + LAG_WINDOW,
                                            center=c24)
                result["lag_24h"]    = val if val is not None else np.nan
                result["lag_24h_ts"] = ts.isoformat() if ts else None

                c48 = target_h - timedelta(hours=48)
                val, ts = _closest_hour_avg(cur, station_uid,
                                            c48 - LAG_WINDOW, c48 + LAG_WINDOW,
                                            center=c48)
                result["lag_48h"]    = val if val is not None else np.nan
                result["lag_48h_ts"] = ts.isoformat() if ts else None

                c168 = target_h - timedelta(hours=168)
                val, ts = _closest_hour_avg(cur, station_uid,
                                            c168 - LAG_WINDOW, c168 + LAG_WINDOW,
                                            center=c168)
                result["lag_168h"]    = val if val is not None else np.nan
                result["lag_168h_ts"] = ts.isoformat() if ts else None
        return result

    def _get_lags_all(self, target_utc: datetime) -> dict[int, dict]:
        """batch lag query for all stations: closest-hour logic, no timestamps."""
        target_h = target_utc.replace(minute=0, second=0, microsecond=0)
        lags: dict[int, dict] = {}
        with self._db() as conn:
            with conn.cursor() as cur:
                c24 = target_h - timedelta(hours=24)
                vals, _ = _closest_hour_avg(cur, None,
                                            c24 - LAG_WINDOW, c24 + LAG_WINDOW,
                                            center=c24)
                for uid, v in vals.items():
                    lags.setdefault(uid, {})["lag_24h"] = v

                c48 = target_h - timedelta(hours=48)
                vals, _ = _closest_hour_avg(cur, None,
                                            c48 - LAG_WINDOW, c48 + LAG_WINDOW,
                                            center=c48)
                for uid, v in vals.items():
                    lags.setdefault(uid, {})["lag_48h"] = v

                c168 = target_h - timedelta(hours=168)
                vals, _ = _closest_hour_avg(cur, None,
                                            c168 - LAG_WINDOW, c168 + LAG_WINDOW,
                                            center=c168)
                for uid, v in vals.items():
                    lags.setdefault(uid, {})["lag_168h"] = v
        return lags

    def _get_station_dynamic(self, target_utc: datetime,
                             uid: int | None = None) -> dict:
        """compute zero_rate_3d, near_zero_rate_3d, hours_since_reb from the last 72h.

        if uid is None: returns {station_uid: {feature: value}} for all stations.
        if uid is given: returns {feature: value} for that station.
        """
        cutoff     = target_utc - timedelta(hours=72)
        uid_filter = "AND station_uid = %s" if uid is not None else ""
        params     = [cutoff] + ([uid] if uid is not None else []) + [target_utc]
        with self._db() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    WITH hourly AS (
                        SELECT station_uid,
                               date_trunc('hour', scrape_time) AS h,
                               AVG(bikes_available_to_rent)    AS avg_bikes
                        FROM station_snapshots
                        WHERE scrape_time >= %s {uid_filter}
                        GROUP BY station_uid, date_trunc('hour', scrape_time)
                    ),
                    stats AS (
                        SELECT station_uid,
                               AVG(CASE WHEN avg_bikes < 0.5 THEN 1.0 ELSE 0.0 END) AS zero_rate,
                               AVG(CASE WHEN avg_bikes < 1.5 THEN 1.0 ELSE 0.0 END) AS near_zero_rate
                        FROM hourly
                        GROUP BY station_uid
                    ),
                    with_prev AS (
                        SELECT station_uid, h, avg_bikes,
                               LAG(avg_bikes) OVER (PARTITION BY station_uid ORDER BY h) AS prev_bikes
                        FROM hourly
                    ),
                    reb_events AS (
                        SELECT station_uid, MAX(h) AS last_reb
                        FROM with_prev
                        WHERE avg_bikes - COALESCE(prev_bikes, avg_bikes) > {_REB_THRESHOLD}
                        GROUP BY station_uid
                    )
                    SELECT s.station_uid, s.zero_rate, s.near_zero_rate,
                           COALESCE(
                               EXTRACT(EPOCH FROM (%s::timestamptz - r.last_reb)) / 3600.0,
                               72.0
                           ) AS hours_since_reb
                    FROM stats s
                    LEFT JOIN reb_events r USING (station_uid)
                """, params)
                rows = cur.fetchall()

        result = {
            int(r[0]): {
                "zero_rate_3d":      float(r[1]),
                "near_zero_rate_3d": float(r[2]),
                "hours_since_reb":   float(r[3]),
            }
            for r in rows
        }
        if uid is not None:
            return result.get(uid, {"zero_rate_3d": 0.5, "near_zero_rate_3d": 0.5,
                                    "hours_since_reb": 72.0})
        return result

    # ── inference ─────────────────────────────────────────────────────────────

    def _fill_lag_baseline(self, uid: int, row: dict, h: int, d: int) -> dict:
        """fill any remaining NaN lags with historical medians: last resort only."""
        sources = {}
        if np.isnan(row.get("lag_168h", np.nan)):
            row["lag_168h"] = self._lag_baseline.get((uid, h, d), np.nan)
            sources["lag_168h"] = "historical median"
        if np.isnan(row.get("lag_48h", np.nan)):
            d_2ago = (d - 2) % 7
            row["lag_48h"] = self._lag_baseline.get((uid, h, d_2ago), np.nan)
            sources["lag_48h"] = "historical median"
        if np.isnan(row.get("lag_24h", np.nan)):
            d_prev = (d - 1) % 7
            row["lag_24h"] = self._lag_baseline.get((uid, h, d_prev), np.nan)
            sources["lag_24h"] = "historical median"
        return sources

    def _assemble_row(self, uid: int, time_feats: dict, weather: dict,
                      lag: dict, dynamic_feats: dict | None = None) -> tuple[dict, dict]:
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

        # station-level stats (stable historical characteristics)
        stats = self._station_stats.get(uid, {})
        row["avail_std"]    = stats.get("avail_std", np.nan)
        row["reb_per_week"] = stats.get("reb_per_week", np.nan)

        # dynamic features computed from recent scrape data
        if dynamic_feats:
            row["zero_rate_3d"]      = dynamic_feats.get("zero_rate_3d", np.nan)
            row["near_zero_rate_3d"] = dynamic_feats.get("near_zero_rate_3d", np.nan)
            row["hours_since_reb"]   = dynamic_feats.get("hours_since_reb", np.nan)

        # fill missing lags from historical baseline (last resort)
        sources = self._fill_lag_baseline(uid, row, int(time_feats["hour_of_day"]),
                                          int(time_feats["dow"]))

        # lag_24h_ratio: computed after lag_24h may have been filled by baseline
        mean_avail = stats.get("station_mean_avail", np.nan)
        lag_24h    = row.get("lag_24h", np.nan)
        if not np.isnan(lag_24h) and not np.isnan(mean_avail):
            row["lag_24h_ratio"] = lag_24h / (mean_avail + 0.1)

        return row, sources

    def predict_one(self, station_uid: int, target_utc: datetime) -> float:
        if station_uid not in self.stations.index:
            raise KeyError(f"station {station_uid} not found")
        time_feats = _prague_time_features(target_utc)
        weather    = self._get_weather(target_utc)
        lag        = self._get_lags_one(station_uid, target_utc)
        dyn        = self._get_station_dynamic(target_utc, uid=station_uid)
        row, _     = self._assemble_row(station_uid, time_feats, weather, lag, dyn)
        pred = self.model.predict(pd.DataFrame([row])[self.feature_cols])[0]
        return max(0.0, float(pred))

    def predict_one_debug(self, station_uid: int, target_utc: datetime) -> dict:
        """full prediction with lag timestamps and geo info: for the debug endpoint."""
        if station_uid not in self.stations.index:
            raise KeyError(f"station {station_uid} not found")
        st           = self.stations.loc[station_uid]
        time_feats   = _prague_time_features(target_utc)
        weather      = self._get_weather(target_utc)
        lag          = self._get_lags_one(station_uid, target_utc)
        dyn          = self._get_station_dynamic(target_utc, uid=station_uid)
        row, sources = self._assemble_row(station_uid, time_feats, weather, lag, dyn)
        pred = max(0.0, float(self.model.predict(pd.DataFrame([row])[self.feature_cols])[0]))

        result: dict = {
            "station_uid": station_uid,
            "name":        st["name"],
            "district":    int(st.get("district", 0)),
            "bike_racks":  int(st.get("bike_racks", 0)),
            "lat":         float(st["lat"]),
            "lng":         float(st["lng"]),
            "is_active":   bool(st.get("is_active", False)),
            "predicted_avg_available": round(pred, 2),
            # lag values and their provenance
            "lag_1h":        round(float(row["lag_1h"]), 2) if not np.isnan(row.get("lag_1h", np.nan)) else None,
            "lag_1h_ts":     lag.get("lag_1h_ts"),
            "lag_1h_source": sources.get("lag_1h", "live"),
            "lag_24h":       round(float(row["lag_24h"]), 2) if not np.isnan(row.get("lag_24h", np.nan)) else None,
            "lag_24h_ts":    lag.get("lag_24h_ts"),
            "lag_24h_source": sources.get("lag_24h", "live"),
            "lag_168h":      round(float(row["lag_168h"]), 2) if not np.isnan(row.get("lag_168h", np.nan)) else None,
            "lag_168h_ts":   lag.get("lag_168h_ts"),
            "lag_168h_source": sources.get("lag_168h", "live"),
            # weather
            "temperature":   weather.get("temperature"),
            "precipitation": weather.get("precipitation"),
            "windspeed":     weather.get("windspeed"),
        }
        for col in self._geo_cols:
            val = st.get(col)
            result[col] = float(val) if val is not None and not (isinstance(val, float) and np.isnan(val)) else None
        return result

    def predict_all(self, target_utc: datetime) -> list[dict]:
        """predict for every station: used by the dashboard map."""
        time_feats = _prague_time_features(target_utc)
        weather    = self._get_weather(target_utc)
        lags       = self._get_lags_all(target_utc)
        dynfeats   = self._get_station_dynamic(target_utc)

        rows = []
        for uid in self.stations.index:
            dyn = dynfeats.get(uid, {})
            row, _ = self._assemble_row(uid, time_feats, weather, lags.get(uid, {}), dyn)
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
                "is_active":   bool(st.get("is_active", False)),
                "predicted_avg_available": round(float(pred), 2),
            })
        return out

    def get_timeline(self, station_uid: int, hours: int = 24) -> list[dict]:
        """return raw scrape data for the last N hours for one station."""
        with self._db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT scrape_time, bikes_available_to_rent
                    FROM station_snapshots
                    WHERE station_uid = %s
                      AND scrape_time >= NOW() - INTERVAL '1 hour' * %s
                    ORDER BY scrape_time
                """, (station_uid, hours))
                rows = cur.fetchall()
        return [
            {"scrape_time": r[0].isoformat(),
             "bikes_available_to_rent": int(r[1]) if r[1] is not None else None}
            for r in rows
        ]
