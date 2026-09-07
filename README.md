# nextbike prague — demand forecasting

end-to-end ML project: scrape live bike-sharing data, build a feature pipeline, train a demand forecast model, and serve predictions through a live API and map dashboard.

**live demo**: *coming soon*

## stack

python · lightgbm · fastapi · streamlit · postgresql (supabase) · github actions

## what it does

- scrapes nextbike prague every 10 minutes via github actions, stores in supabase
- builds hourly features: lag availability, time, weather (open-meteo), geospatial (osm)
- trains a lightgbm model to predict avg bikes per station up to 24h ahead
- serves predictions via fastapi, displays them on a live pydeck map

## model

walk-forward CV, test set = last 6 weeks (2026-07-26 – 2026-08-30, 437k rows, 760 stations).

| model | features | test MAE | test R² |
|---|---|---|---|
| naive (lag_24h) | — | 1.009 | 0.359 |
| lightgbm | time + lag | 0.896 | 0.566 |
| lightgbm | + geo | 0.893 | 0.573 |
| lightgbm | + weather | 0.897 | 0.567 |
| **lightgbm** | **+ geo + weather** | **0.893** | **0.580** |
| xgboost | all features (300k sample) | 0.973 | 0.578 |
| random forest | all features (300k sample) | 0.973 | 0.581 |

beats the naive baseline by 11%. lag_24h (same hour yesterday) is the single most important feature. geo and weather add small improvements on top.

**main difficulty**: operators move bikes by truck outside of organic demand. detecting rebalancing from raw scraper data is the core data engineering challenge — we built 4 heuristic detection rules but some events still slip through and distort lag features. a clean dataset with operator truck logs would make modeling significantly easier.

## running locally

```bash
docker compose up -d           # local postgres
pip install -r requirements.txt
cp .env.example .env           # fill in SUPABASE_DATABASE_URL
uvicorn api.main:app --reload  # api at localhost:8000
streamlit run dashboard/app.py # dashboard at localhost:8501
```

## structure

```
scrape_prague.py      scraper (github actions, every 10 min)
notebooks/
  01_eda.ipynb        rebalancing detection, demand patterns
  02_features.ipynb   feature engineering
  03_model.ipynb      training, evaluation, comparison
api/
  main.py             fastapi endpoints
  predictor.py        inference: lag queries, feature assembly, model
dashboard/app.py      streamlit map + station detail
data/
  model.lgb           trained model
  features.parquet    precomputed feature table (for lag baseline + active stations)
  feature_cols.json   feature list for inference
db/schema.sql         postgres schema
```
