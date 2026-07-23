"""streamlit dashboard — nextbike prague demand forecast."""

import requests
import pandas as pd
import numpy as np
import streamlit as st
import pydeck as pdk
from datetime import datetime, timezone, timedelta

API_BASE = "http://localhost:8000"

st.set_page_config(
    page_title="nextbike prague — demand forecast",
    page_icon="🚲",
    layout="wide",
)

# ── helpers ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def fetch_predictions(hour_iso: str | None = None) -> pd.DataFrame:
    url = f"{API_BASE}/predict/all/now"
    params = {"hour": hour_iso} if hour_iso else {}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data["predictions"])
    df["hour_utc"] = data["hour_utc"]
    return df


@st.cache_data(ttl=300)
def fetch_stations() -> pd.DataFrame:
    resp = requests.get(f"{API_BASE}/stations", timeout=10)
    resp.raise_for_status()
    return pd.DataFrame(resp.json())


def demand_color(val: float, max_val: float) -> list[int]:
    """map demand 0..max to green (low) → red (high) as RGBA."""
    t = min(val / max(max_val, 1), 1.0)
    r = int(255 * t)
    g = int(255 * (1 - t))
    return [r, g, 40, 200]


# ── sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.title("🚲 nextbike forecast")
st.sidebar.markdown("predicted average bikes available at each station.")

now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

hour_offset = st.sidebar.slider(
    "forecast hour (offset from now)",
    min_value=0, max_value=23, value=0, step=1,
    format="+%dh",
)
target_utc = now_utc + timedelta(hours=hour_offset)
target_iso = target_utc.isoformat()

st.sidebar.markdown(f"**target:** `{target_utc.strftime('%Y-%m-%d %H:%M UTC')}`")
st.sidebar.markdown("---")
st.sidebar.markdown("color: 🟢 more bikes → 🔴 fewer bikes")

# ── main content ──────────────────────────────────────────────────────────────

st.title("nextbike prague — demand forecast")

try:
    df = fetch_predictions(target_iso if hour_offset > 0 else None)
except Exception as e:
    st.error(f"cannot reach API at {API_BASE} — is it running?\n\n`{e}`")
    st.code("uvicorn api.main:app --reload", language="bash")
    st.stop()

max_pred = df["predicted_avg_available"].quantile(0.95)
df["color"] = df["predicted_avg_available"].apply(lambda v: demand_color(v, max_pred))

# metrics row
col1, col2, col3 = st.columns(3)
col1.metric("total stations", f"{len(df):,}")
col2.metric("avg predicted bikes", f"{df['predicted_avg_available'].mean():.1f}")
col3.metric("stations with ≤1 bike", f"{(df['predicted_avg_available'] <= 1).sum():,}")

# pydeck map
layer = pdk.Layer(
    "ScatterplotLayer",
    data=df,
    get_position=["lng", "lat"],
    get_fill_color="color",
    get_radius=80,
    radius_min_pixels=5,
    radius_max_pixels=20,
    pickable=True,
    auto_highlight=True,
)

view = pdk.ViewState(
    latitude=50.0755,
    longitude=14.4378,
    zoom=12,
    pitch=0,
)

tooltip = {
    "html": "<b>{name}</b><br/>predicted bikes: <b>{predicted_avg_available}</b>",
    "style": {"background": "rgba(0,0,0,0.75)", "color": "white", "fontSize": "13px", "padding": "6px 10px"},
}

st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=view,
    tooltip=tooltip,
    map_style="mapbox://styles/mapbox/light-v10",
))

# distribution
st.subheader("predicted demand distribution")
hist_col, table_col = st.columns([2, 1])

with hist_col:
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(7, 3))
    ax.hist(df["predicted_avg_available"], bins=30, color="#3498db", edgecolor="none")
    ax.axvline(df["predicted_avg_available"].mean(), color="red", linestyle="--", linewidth=1,
               label=f"mean {df['predicted_avg_available'].mean():.1f}")
    ax.set_xlabel("predicted avg bikes")
    ax.set_ylabel("stations")
    ax.set_title(f"demand distribution — {target_utc.strftime('%H:%M UTC')}")
    ax.legend()
    st.pyplot(fig)

with table_col:
    st.markdown("**lowest predicted availability**")
    lowest = df.nsmallest(10, "predicted_avg_available")[["name", "predicted_avg_available"]]
    lowest.columns = ["station", "pred"]
    lowest["pred"] = lowest["pred"].round(1)
    st.dataframe(lowest, hide_index=True, use_container_width=True)
