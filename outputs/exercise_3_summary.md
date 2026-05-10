# Coding Challenge - Exercise 3 Summary

## Dataset

- Raw input file: `C:\Users\GonzaloFigueroa\Documents\Private\BME\coding challenge\data\raw\lastfm-dataset-1K\lastfm-dataset-1K\userid-timestamp-artid-artname-traid-traname.tsv`
- Prepared Parquet dataset: `C:\Users\GonzaloFigueroa\Documents\Private\BME\coding challenge\data\processed\lastfm_events_parquet`

## Selected metric

- Metric: `number of sessions per month`
- Forecast horizon: `3 months`

## Top user

- User ID: `user_000833`
- Total sessions: `6897`
- First session start: `2005-02-20 03:42:32`
- Last session end: `2009-05-23 16:06:37`

## Forecast

- 2009-06-01: 162.80 (rounded=163)
- 2009-07-01: 191.88 (rounded=192)
- 2009-08-01: 212.29 (rounded=212)

## Model

- Approach: `linear autoregressive model with lag, trend, and month-seasonality features`
- Holdout validation MAE: `19.85`

## Output files

- `exercise_3_answer.tsv`
- `exercise_3_monthly_session_history_top_user.tsv`
- `exercise_3_forecast_next_3_months.tsv`
- `exercise_3_model_coefficients.tsv`
