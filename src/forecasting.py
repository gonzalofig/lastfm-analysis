from dataclasses import dataclass

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .sessionization import top_users_by_session_count


@dataclass
class ForecastArtifacts:
    top_user_id: str
    selected_metric: str
    history_df: pd.DataFrame
    forecast_df: pd.DataFrame
    model_coefficients_df: pd.DataFrame
    validation_mae: float | None
    train_row_count: int


def get_top_user_for_ml(session_summary_df: DataFrame) -> dict:
    row = top_users_by_session_count(session_summary_df=session_summary_df, top_user_count=1).collect()[0]
    return {
        "user_id": row["user_id"],
        "session_count": row["session_count"],
        "first_session_start": row["first_session_start"],
        "last_session_end": row["last_session_end"],
    }


def build_monthly_metric_history(
    session_summary_df: DataFrame,
    user_id: str,
    metric: str = "session_count",
) -> pd.DataFrame:
    monthly_df = (
        session_summary_df
        .filter(F.col("user_id") == user_id)
        .withColumn("month_start", F.date_trunc("month", "session_start"))
        .groupBy("month_start")
        .agg(
            F.count("*").alias("session_count"),
            F.avg("session_duration_minutes").alias("avg_session_duration_minutes"),
        )
        .orderBy("month_start")
    )

    monthly_pd = monthly_df.toPandas()
    if monthly_pd.empty:
        raise ValueError(f"No sessions found for user {user_id}")

    monthly_pd["month_start"] = pd.to_datetime(monthly_pd["month_start"])
    full_month_index = pd.date_range(
        monthly_pd["month_start"].min(),
        monthly_pd["month_start"].max(),
        freq="MS",
    )

    filled_pd = (
        monthly_pd.set_index("month_start")
        .reindex(full_month_index)
        .rename_axis("month_start")
        .reset_index()
    )

    filled_pd["session_count"] = filled_pd["session_count"].fillna(0).astype(float)
    filled_pd["avg_session_duration_minutes"] = filled_pd["avg_session_duration_minutes"].fillna(0.0)
    filled_pd["user_id"] = user_id

    if metric not in {"session_count", "avg_session_duration_minutes"}:
        raise ValueError(f"Unsupported metric: {metric}")

    filled_pd["selected_metric"] = filled_pd[metric].astype(float)
    filled_pd["metric_name"] = metric
    return filled_pd


def _build_feature_frame(history_df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    feature_df = history_df.copy().sort_values("month_start").reset_index(drop=True)
    feature_df["time_index"] = np.arange(len(feature_df), dtype=float)
    feature_df["month_number"] = feature_df["month_start"].dt.month.astype(float)
    feature_df["month_sin"] = np.sin(2.0 * np.pi * feature_df["month_number"] / 12.0)
    feature_df["month_cos"] = np.cos(2.0 * np.pi * feature_df["month_number"] / 12.0)

    for lag in (1, 2, 3):
        feature_df[f"lag_{lag}"] = feature_df[target_col].shift(lag)

    feature_df["rolling_mean_3"] = feature_df[[f"lag_{lag}" for lag in (1, 2, 3)]].mean(axis=1)
    return feature_df


def _feature_columns() -> list[str]:
    return [
        "time_index",
        "month_sin",
        "month_cos",
        "lag_1",
        "lag_2",
        "lag_3",
        "rolling_mean_3",
    ]


def _fit_linear_regression(feature_df: pd.DataFrame, target_col: str) -> tuple[np.ndarray, list[str]]:
    feature_cols = _feature_columns()
    usable_df = feature_df.dropna(subset=feature_cols + [target_col]).reset_index(drop=True)
    if usable_df.empty:
        raise ValueError("Not enough rows to train the forecasting model.")

    x = usable_df[feature_cols].to_numpy(dtype=float)
    x = np.column_stack([np.ones(len(x)), x])
    y = usable_df[target_col].to_numpy(dtype=float)
    coefficients = np.linalg.lstsq(x, y, rcond=None)[0]
    return coefficients, feature_cols


def _predict_matrix(input_df: pd.DataFrame, coefficients: np.ndarray, feature_cols: list[str]) -> np.ndarray:
    x = input_df[feature_cols].to_numpy(dtype=float)
    x = np.column_stack([np.ones(len(x)), x])
    return x @ coefficients


def _compute_validation_mae(
    feature_df: pd.DataFrame,
    target_col: str,
    holdout_months: int = 6,
) -> tuple[float | None, int]:
    feature_cols = _feature_columns()
    usable_df = feature_df.dropna(subset=feature_cols + [target_col]).reset_index(drop=True)
    if len(usable_df) <= holdout_months + 6:
        return None, len(usable_df)

    train_df = usable_df.iloc[:-holdout_months].copy()
    validation_df = usable_df.iloc[-holdout_months:].copy()
    coefficients, _ = _fit_linear_regression(train_df, target_col)
    predictions = _predict_matrix(validation_df, coefficients, feature_cols)
    mae = float(np.mean(np.abs(validation_df[target_col].to_numpy(dtype=float) - predictions)))
    return mae, len(train_df)


def _recursive_forecast(
    history_df: pd.DataFrame,
    target_col: str,
    coefficients: np.ndarray,
    feature_cols: list[str],
    periods: int = 3,
) -> pd.DataFrame:
    working_df = history_df[["month_start", target_col]].copy().sort_values("month_start").reset_index(drop=True)
    forecast_rows: list[dict] = []

    for step in range(1, periods + 1):
        next_month = working_df["month_start"].max() + pd.offsets.MonthBegin(1)
        temp_df = pd.concat(
            [
                working_df,
                pd.DataFrame([{"month_start": next_month, target_col: np.nan}]),
            ],
            ignore_index=True,
        )
        feature_df = _build_feature_frame(temp_df, target_col)
        next_row = feature_df.iloc[[-1]].copy()
        prediction = float(_predict_matrix(next_row, coefficients, feature_cols)[0])
        prediction = max(0.0, prediction)

        forecast_rows.append(
            {
                "month_start": next_month,
                "predicted_value": prediction,
                "predicted_value_rounded": int(round(prediction)),
                "forecast_step": step,
            }
        )
        working_df = pd.concat(
            [
                working_df,
                pd.DataFrame([{"month_start": next_month, target_col: prediction}]),
            ],
            ignore_index=True,
        )

    return pd.DataFrame(forecast_rows)


def run_monthly_forecast_for_top_user(
    session_summary_df: DataFrame,
    metric: str = "session_count",
    forecast_periods: int = 3,
) -> ForecastArtifacts:
    top_user = get_top_user_for_ml(session_summary_df)
    history_df = build_monthly_metric_history(session_summary_df, top_user["user_id"], metric=metric)
    feature_df = _build_feature_frame(history_df, "selected_metric")
    validation_mae, train_row_count = _compute_validation_mae(feature_df, "selected_metric")
    coefficients, feature_cols = _fit_linear_regression(feature_df, "selected_metric")
    forecast_df = _recursive_forecast(
        history_df,
        "selected_metric",
        coefficients,
        feature_cols,
        periods=forecast_periods,
    )

    forecast_df["user_id"] = top_user["user_id"]
    forecast_df["metric_name"] = metric

    coefficients_df = pd.DataFrame(
        {
            "feature_name": ["intercept"] + feature_cols,
            "coefficient": coefficients,
        }
    )

    history_df = history_df.copy()
    history_df["top_user_session_count"] = top_user["session_count"]
    history_df["last_available_month"] = history_df["month_start"].max()

    return ForecastArtifacts(
        top_user_id=top_user["user_id"],
        selected_metric=metric,
        history_df=history_df,
        forecast_df=forecast_df,
        model_coefficients_df=coefficients_df,
        validation_mae=validation_mae,
        train_row_count=train_row_count,
    )
