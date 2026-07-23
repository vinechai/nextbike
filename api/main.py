"""fastapi prediction endpoint for nextbike demand forecasting."""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .predictor import Predictor

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://nextbike:nextbike@localhost:5432/nextbike",
)

_predictor: Predictor | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _predictor
    _predictor = Predictor(DATABASE_URL)
    yield
    _predictor = None


app = FastAPI(title="nextbike demand forecast API", version="1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _parse_hour(hour: str | None) -> datetime:
    if hour is None:
        return datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(hour)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"invalid hour: {hour!r} — use ISO8601, e.g. 2024-06-01T14:00:00Z",
        )


@app.get("/stations")
def list_stations():
    """all stations with metadata (lat, lng, name, capacity)."""
    df = _predictor.stations.reset_index()[["station_uid", "name", "lat", "lng", "bike_racks"]]
    return df.to_dict(orient="records")


@app.get("/predict/{station_uid}")
def predict_station(station_uid: int, hour: str | None = None):
    """predict avg bikes available at one station for the given hour (UTC).

    omit hour to predict for the current hour.
    """
    target = _parse_hour(hour)
    try:
        pred = _predictor.predict_one(station_uid, target)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {
        "station_uid": station_uid,
        "hour_utc":    target.replace(minute=0, second=0, microsecond=0).isoformat(),
        "predicted_avg_available": round(pred, 2),
    }


@app.get("/predict/all/now")
def predict_all_now(hour: str | None = None):
    """predict avg bikes for all stations at one hour (for the dashboard map).

    omit hour to predict for the current hour.
    """
    target = _parse_hour(hour)
    predictions = _predictor.predict_all(target)
    return {
        "hour_utc":    target.replace(minute=0, second=0, microsecond=0).isoformat(),
        "predictions": predictions,
    }


@app.get("/health")
def health():
    return {"status": "ok", "stations_loaded": len(_predictor.stations)}
