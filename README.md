# nextbike prague — demand forecasting

end-to-end data engineering and ML project: scrape live bike-sharing data, store it in a database, explore demand patterns, train a forecasting model, serve predictions through an API, and display them on a live map.

**data**: ~3.7M station-hour observations scraped from the nextbike Prague API (Jan–Aug 2026, every 10 minutes), plus hourly weather from Open-Meteo.

**model**: LightGBM trained on hourly station availability with lag, time, weather, and geospatial features. test MAE 0.89 bikes (R² 0.58) on a 6-week held-out test set. beats a naive lag-24h baseline by 11%.

**live demo**: *coming soon*

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

Supabase holds the live data. the API queries it for recent station availability (lag features) and runs model inference. the dashboard calls the API every 60 seconds.

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

the notebooks need a local postgres with imported data to run. see `ingestion/import_parquet.py` if starting from the parquet files.

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

the model predicts average bikes available at a station for a given hour, up to 24h ahead. it uses lag_24h (same hour yesterday), lag_48h (two days ago), and lag_168h (same hour last week) — no lag_1h. this means the forecast is always available regardless of how far ahead you're predicting.

walk-forward cross-validation across 4 time windows, with the last 6 weeks held out completely as the test set. hyperparameters tuned with Optuna (40 trials, val set scoring).

| model | features | test MAE | test R² |
|---|---|---|---|
| naive (lag_24h) | none | 1.009 | 0.359 |
| LightGBM | time + lag | 0.896 | 0.566 |
| LightGBM | + geo | 0.893 | 0.573 |
| LightGBM | + weather | 0.897 | 0.567 |
| LightGBM | + geo + weather (full) | **0.893** | **0.580** |
| XGBoost | all features (300k sample) | 0.973 | 0.578 |
| Random Forest | all features (300k sample) | 0.973 | 0.581 |

MAE is in the same unit as the target (bikes). 0.89 means the prediction is off by less than 1 bike on average. the model beats the naive baseline by 11% — meaningful for a 24h-ahead rebalancing tool.

## Key findings

- **lag features dominate**: lag_24h (same hour yesterday) is the most important single feature, followed by lag_168h (same hour last week). time-of-day and day-of-week come next. geo and weather add modest improvements on top.
- **760 active stations out of 1,513**: stations with mean availability below 0.5 bikes are excluded from training and shown as inactive (grey) on the map. most of these are dead or seasonal spots with essentially no demand.
- **scraper coverage varies**: february 2026 had 7% coverage (GitHub Actions 60-day auto-disable). a separate keep-alive workflow prevents this going forward. january had a smaller winter fleet. training data starts from march 2026.

## Challenges

**rebalancing detection**

bike-sharing data is noisy because operators regularly move bikes between stations by truck. a station that jumps from 2 to 15 bikes in one hour is not organic demand — it's a delivery. training on those hours would teach the model to predict truck arrivals, which is not possible from time or weather signals alone.

detecting rebalancing from raw scraper data is hard. we built four detection rules: trip-based batch arrivals (3+ bikes moving the same route in one scrape), destination-based mass arrivals (5+ bikes at the same station off-peak), slow deliveries (net rise of 8+ bikes in one hour off-peak), and availability drops (5+ bikes leaving a non-metro station in one hour). combined, these catch about 2,000 rebalancing events and exclude ~5,300 station-hours from training.

despite this, some events still slip through — especially 1–2 bike moves at night that fall below the batch threshold. when lag_24h captures a post-delivery state, the model inherits that inflated value and overpredicts the following night. this is visible as a small systematic bias at 3–5am in the residual heatmap.

a clean dataset with operator truck logs would make this much simpler. without it, heuristic detection is the only option, and it can't be perfect.

**scraper gaps**

the scraper runs on GitHub Actions and is not perfectly reliable. a 28-day outage in late June–July 2026 left fold 3 of the walk-forward CV with only 1,478 test rows (expected ~400k), so that fold was skipped. lag_168h (7-day lookback) is only 88.5% non-null because of gaps in the time series — stations with missing hours have no 7-day-ago reference to look up. the model handles missing lags via fallback logic, but accuracy degrades slightly for stations with sporadic coverage.

## Notes

scraper runs on GitHub Actions every 10 minutes and writes to Supabase. a separate monthly keep-alive workflow prevents the 60-day auto-disable.

the model predicts organic demand — it cannot account for rebalancing truck arrivals. this is a known limitation and is how production bike-sharing forecasting systems work in practice.
