# nextbike prague — demand forecasting

End-to-end data engineering and ML project: scrape live bike-sharing data, store it in a database, explore demand patterns, train a forecasting model, serve predictions through an API, and display them on a live map.

**Data**: ~3.6 million station snapshots scraped from the nextbike Prague API (Jan–Jun 2026, every 10 minutes), plus hourly weather from Open-Meteo.

**Model**: LightGBM trained on hourly station availability with lag, time, weather, and geospatial features. Test MAE 0.118 bikes (R² 0.960).

**Live demo**: *coming soon*

## Architecture

```
github actions (every 10 min)
    └── scrape_prague.py → supabase (postgres)
                               │
                    ┌──────────┴──────────┐
                    │                     │
             notebooks/              api/main.py
             02_features.ipynb       (fastapi)
             03_model.ipynb              │
                    │              dashboard/app.py
             data/model.lgb        (streamlit)
```

Supabase holds the live data. The API queries it for recent station availability (lag features) and runs model inference. The dashboard calls the API every 60 seconds.

## Running locally

```bash
# 1. start local postgres
docker compose up -d

# 2. install dependencies
pip install -r requirements.txt

# 3. copy and fill in credentials
cp .env.example .env
# set SUPABASE_DATABASE_URL in .env

# 4. start the api
uvicorn api.main:app --reload

# 5. start the dashboard (separate terminal)
streamlit run dashboard/app.py
```

API docs at http://localhost:8000/docs, dashboard at http://localhost:8501.

The notebooks need a local postgres with imported data to run. See `ingestion/import_parquet.py` if starting from the parquet files.

## Project structure

```
scrape_prague.py            scraper — runs on github actions every 10 min
backfill_weather.py         one-time weather backfill from open-meteo archive
enrich_stations.py          one-time geospatial enrichment (osm + elevation)
ingestion/
    import_parquet.py       import historical parquet data into local postgres

notebooks/
    01_eda.ipynb            rebalancing detection, demand patterns, network analysis
    02_features.ipynb       feature engineering — hourly aggregation, lag features, weather join
    03_model.ipynb          model training, comparison, evaluation, feature importance

api/
    main.py                 fastapi endpoints
    predictor.py            model loading, lag queries, inference

dashboard/
    app.py                  streamlit app with pydeck map and station detail panel

data/
    model.lgb               trained lightgbm model
    feature_cols.json       list of features the model was trained on
    station_geo.parquet     precomputed geospatial features per station

db/schema.sql               postgres schema (4 tables)
docker-compose.yml          local postgres + pgadmin
render.yaml                 render deployment config for the api
```

## Modeling

Six models compared across different feature sets. Time-based train/val/test split — test set is the last 6 weeks, no random shuffle.

| model | features | test MAE | test R² |
|---|---|---|---|
| naive (lag 168h) | none | 1.082 | -0.116 |
| LightGBM | time + lag | **0.118** | **0.960** |
| LightGBM | + geo | 0.121 | 0.960 |
| LightGBM | + weather | 0.125 | 0.959 |
| LightGBM | + geo + weather | 0.127 | 0.959 |
| XGBoost | all features | 0.130 | 0.958 |
| Random Forest | all features | 0.129 | 0.959 |

The simplest LightGBM (time + lag features only) wins. For 1h-ahead prediction, lag_1h already carries most of the signal — the station's state 1 hour ago is the best predictor of its state now. Adding weather and geo features increases model complexity without adding signal at this horizon.

## Key findings

- **rebalancing distorts training data**: when a truck delivers 10 bikes to a station, the hourly average spikes — that's supply intervention, not organic demand. those hours are detected and excluded from training. the detection logic (batch arrivals + known uphill routes outside rush hours) flags about 8–10% of station-hours.
- **lag features dominate**: lag_24h (same hour yesterday) is the most important single feature, followed by lag_168h (same hour last week). together they explain most of the variance. time-of-day and day-of-week come next.
- **simple beats complex at 1h horizon**: weather and geospatial features matter more at longer horizons (next day, next week). for 1h-ahead, recent history is the only signal that matters.
- **scraper coverage varies a lot**: February 2026 had 7.3% coverage (GitHub Actions disabled due to 60 days of no activity). January had a smaller winter station network. training data starts from March 2026.

## Notes

Scraper runs on GitHub Actions every 10 minutes and writes to Supabase. A separate monthly keep-alive workflow prevents the 60-day auto-disable.

The model predicts organic demand — it cannot account for rebalancing truck arrivals. This is a known limitation and is how production bike-sharing forecasting systems work in practice.
