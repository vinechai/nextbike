"""streamlit dashboard — nextbike prague demand forecast."""

import os
import requests
import pandas as pd
import numpy as np
import streamlit as st
import pydeck as pdk
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timezone, timedelta

try:
    from zoneinfo import ZoneInfo
    _PRAGUE_TZ = ZoneInfo("Europe/Prague")
except Exception:
    import pytz
    _PRAGUE_TZ = pytz.timezone("Europe/Prague")

# streamlit cloud: set API_BASE in the app's secrets (Settings -> Secrets)
# locally: set API_BASE env var or defaults to localhost
try:
    API_BASE = st.secrets["API_BASE"]
except (KeyError, FileNotFoundError):
    API_BASE = os.environ.get("API_BASE", "http://localhost:8000")

st.set_page_config(
    page_title="nextbike prague",
    page_icon="🚲",
    layout="wide",
)

# ── helpers ────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def fetch_predictions(hour_iso: str | None = None) -> pd.DataFrame:
    url = f"{API_BASE}/predict/all/now"
    params = {"hour": hour_iso} if hour_iso else {}
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data["predictions"])
    df["hour_utc"] = data["hour_utc"]
    return df


@st.cache_data(ttl=60)
def fetch_debug(uid: int, hour_iso: str | None = None) -> dict:
    params = {"hour": hour_iso} if hour_iso else {}
    resp = requests.get(f"{API_BASE}/predict/{uid}/debug", params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


@st.cache_data(ttl=120)
def fetch_timeline(uid: int) -> pd.DataFrame:
    resp = requests.get(f"{API_BASE}/station/{uid}/timeline", timeout=10)
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["scrape_time"] = pd.to_datetime(df["scrape_time"])
    return df.sort_values("scrape_time")


def demand_color(val: float, max_val: float) -> list[int]:
    """map predicted bikes 0..max to red (empty) → green (many) as RGBA."""
    t = min(val / max(max_val, 1), 1.0)
    r = int(255 * (1 - t))
    g = int(255 * t)
    return [r, g, 40, 200]


# ── sidebar ────────────────────────────────────────────────────────────────────

st.sidebar.title("nextbike prague")
st.sidebar.markdown("predicted bikes per station. click a dot to see details.")

now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

hour_offset = st.sidebar.slider(
    "forecast hour (offset from now)",
    min_value=0, max_value=23, value=0, step=1,
    format="+%dh",
)
target_utc = now_utc + timedelta(hours=hour_offset)
target_iso = target_utc.isoformat()

target_prague = target_utc.astimezone(_PRAGUE_TZ)
st.sidebar.markdown(f"**target:** `{target_prague.strftime('%Y-%m-%d %H:%M')}` (Prague time)")
st.sidebar.markdown("---")
st.sidebar.markdown("🔴 empty  →  🟢 many bikes")

# ── load predictions ───────────────────────────────────────────────────────────

st.title("nextbike prague — demand forecast")

try:
    df = fetch_predictions(target_iso if hour_offset > 0 else None)
except Exception as e:
    st.error(f"cannot reach API at {API_BASE} — is it running?\n\n`{e}`")
    st.code("uvicorn api.main:app --reload", language="bash")
    st.stop()

max_pred = df["predicted_avg_available"].quantile(0.95)
df["color"] = df["predicted_avg_available"].apply(lambda v: demand_color(v, max_pred))

col1, col2, col3 = st.columns(3)
col1.metric("stations", f"{len(df):,}")
col2.metric("avg predicted bikes", f"{df['predicted_avg_available'].mean():.1f}")
col3.metric("stations near empty", f"{(df['predicted_avg_available'] < 0.5).sum():,}")

# ── map ────────────────────────────────────────────────────────────────────────

layer = pdk.Layer(
    "ScatterplotLayer",
    data=df,
    get_position=["lng", "lat"],
    get_fill_color="color",
    get_radius=80,
    radius_min_pixels=4,
    radius_max_pixels=18,
    pickable=True,
    auto_highlight=True,
    id="station-layer",
)

view = pdk.ViewState(
    latitude=50.0755,
    longitude=14.4378,
    zoom=12,
    pitch=0,
)

tooltip = {
    "html": "<b>{name}</b><br/>predicted bikes: <b>{predicted_avg_available}</b>",
    "style": {
        "background": "rgba(0,0,0,0.8)",
        "color": "white",
        "fontSize": "13px",
        "padding": "6px 10px",
        "borderRadius": "4px",
    },
}

deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view,
    tooltip=tooltip,
    map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
)

event = st.pydeck_chart(deck, selection_mode="single-object", on_select="rerun", key="map")

# ── station detail panel ───────────────────────────────────────────────────────

selected_uid = None
if event and hasattr(event, "selection") and event.selection:
    objects = event.selection.get("objects", {})
    hits = objects.get("station-layer", [])
    if hits:
        selected_uid = hits[0].get("station_uid")

if selected_uid:
    st.markdown("---")

    try:
        dbg = fetch_debug(selected_uid, target_iso if hour_offset > 0 else None)
    except Exception as e:
        st.warning(f"could not load station data: {e}")
        dbg = None

    if dbg:
        st.subheader(dbg["name"])

        info_col, timeline_col = st.columns([1, 2])

        with info_col:
            st.markdown(f"**predicted bikes: {dbg['predicted_avg_available']}**")
            st.markdown(f"district: P{dbg.get('district', '?')} &nbsp;|&nbsp; capacity: {dbg.get('bike_racks', '?')} racks")
            st.markdown(f"weather: {dbg.get('temperature', '?')}°C, wind {dbg.get('windspeed', '?')} km/h, precip {dbg.get('precipitation', '?')} mm")

            st.markdown("**lag inputs used by model**")
            for lag, label in [("lag_1h", "1h ago"), ("lag_24h", "24h ago"), ("lag_168h", "7d ago")]:
                val  = dbg.get(lag)
                ts   = dbg.get(f"{lag}_ts")
                src  = dbg.get(f"{lag}_source", "live")
                ts_short = ts[:16].replace("T", " ") if ts else "no data"
                if val is not None:
                    st.markdown(f"- **{label}**: {val} bikes — from `{ts_short}` ({'live db' if src == 'live' else src})")
                else:
                    st.markdown(f"- **{label}**: not available")

            st.markdown("**nearby**")
            geo_items = [
                ("dist_metro_m",  "metro"),
                ("dist_tram_m",   "tram stop"),
                ("dist_cafe_m",   "cafe"),
                ("dist_park_m",   "park"),
                ("elevation_m",   "elevation"),
                ("n_tram_300m",   "tram lines ≤300m"),
                ("n_cafe_300m",   "cafes ≤300m"),
            ]
            for key, label in geo_items:
                val = dbg.get(key)
                if val is not None:
                    unit = "m" if key != "n_tram_300m" and key != "n_cafe_300m" else ""
                    st.markdown(f"- {label}: {val:.0f} {unit}".rstrip())

        with timeline_col:
            try:
                tl = fetch_timeline(selected_uid)
                if not tl.empty:
                    fig, ax = plt.subplots(figsize=(9, 3.5))
                    fig.patch.set_facecolor("#0e1117")
                    ax.set_facecolor("#0e1117")
                    ax.plot(tl["scrape_time"], tl["bikes_available_to_rent"],
                            color="#4ade80", linewidth=1.2, zorder=2)
                    ax.fill_between(tl["scrape_time"], tl["bikes_available_to_rent"],
                                    alpha=0.15, color="#4ade80")
                    ax.set_ylabel("bikes available", color="#aaaaaa", fontsize=9)
                    ax.set_title("last 24h — actual scraper data", color="#cccccc", fontsize=10)
                    ax.tick_params(colors="#888888", labelsize=8)
                    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
                    for spine in ax.spines.values():
                        spine.set_color("#333333")
                    ax.yaxis.set_tick_params(labelcolor="#888888")
                    plt.xticks(rotation=30)
                    plt.tight_layout()
                    st.pyplot(fig)
                    plt.close(fig)
                else:
                    st.info("no scrape data in the last 24h for this station")
            except Exception as e:
                st.warning(f"could not load timeline: {e}")
else:
    st.markdown("*click a station on the map to see details*")
