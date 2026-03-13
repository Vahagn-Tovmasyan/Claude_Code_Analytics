"""
Predictive analytics: time-series forecasting and anomaly detection.

Uses lightweight methods (linear regression, rolling statistics) by default,
with optional Prophet integration for more advanced forecasting.
"""

import logging
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

from src.db.repository import AnalyticsRepository

logger = logging.getLogger(__name__)


def forecast_costs(repo: AnalyticsRepository, forecast_days: int = 14) -> pd.DataFrame:
    """
    Forecast daily costs using polynomial regression.

    Falls back to linear regression if data is insufficient.

    Returns DataFrame with columns: ds, y (actual), yhat (predicted),
        yhat_lower, yhat_upper (confidence bounds)
    """
    df = repo.get_daily_cost_series()
    if df.empty or len(df) < 7:
        logger.warning("Insufficient data for forecasting (need >= 7 days)")
        return pd.DataFrame()

    df["ds"] = pd.to_datetime(df["ds"])
    df = df.sort_values("ds")

    # Create numeric feature (day index)
    df["day_idx"] = (df["ds"] - df["ds"].min()).dt.days

    X = df[["day_idx"]].values
    y = df["y"].values

    # Use degree-2 polynomial for flexibility
    degree = min(2, max(1, len(df) // 10))
    poly = PolynomialFeatures(degree=degree)
    X_poly = poly.fit_transform(X)

    model = LinearRegression()
    model.fit(X_poly, y)

    # Predict on existing data
    df["yhat"] = model.predict(X_poly)
    df["is_forecast"] = False

    # Forecast future days
    last_day = int(df["day_idx"].max())
    last_date = df["ds"].max()

    future_days = np.arange(last_day + 1, last_day + 1 + forecast_days).reshape(-1, 1)
    future_dates = [last_date + pd.Timedelta(days=int(d - last_day)) for d in future_days.flatten()]
    X_future_poly = poly.transform(future_days)
    future_pred = model.predict(X_future_poly)

    # Confidence bounds (using residual std)
    residuals = y - model.predict(X_poly)
    residual_std = residuals.std()

    future_df = pd.DataFrame({
        "ds": future_dates,
        "y": np.nan,
        "day_idx": future_days.flatten(),
        "yhat": future_pred,
        "is_forecast": True,
    })

    result = pd.concat([df, future_df], ignore_index=True)
    result["yhat_lower"] = result["yhat"] - 1.96 * residual_std
    result["yhat_upper"] = result["yhat"] + 1.96 * residual_std

    # Clamp lower bound at 0
    result["yhat_lower"] = result["yhat_lower"].clip(lower=0)
    result["yhat"] = result["yhat"].clip(lower=0)

    return result[["ds", "y", "yhat", "yhat_lower", "yhat_upper", "is_forecast"]]


def detect_anomalies(repo: AnalyticsRepository, z_threshold: float = 2.0) -> pd.DataFrame:
    """
    Detect anomalous days using rolling z-scores.

    A day is flagged as anomalous if its cost deviates more than
    z_threshold standard deviations from the rolling mean.

    Returns DataFrame with anomaly flags and z-scores.
    """
    df = repo.get_daily_cost_series()
    if df.empty or len(df) < 7:
        return pd.DataFrame()

    df["ds"] = pd.to_datetime(df["ds"])
    df = df.sort_values("ds")

    # Rolling stats (7-day window)
    window = min(7, len(df))
    df["rolling_mean"] = df["y"].rolling(window=window, min_periods=3, center=True).mean()
    df["rolling_std"] = df["y"].rolling(window=window, min_periods=3, center=True).std()

    # Z-score
    df["z_score"] = (df["y"] - df["rolling_mean"]) / df["rolling_std"].clip(lower=0.001)
    df["is_anomaly"] = df["z_score"].abs() > z_threshold
    df["anomaly_type"] = df.apply(
        lambda row: "spike" if row["z_score"] > z_threshold
        else ("drop" if row["z_score"] < -z_threshold else "normal"),
        axis=1
    )

    return df


def forecast_usage_growth(repo: AnalyticsRepository) -> dict:
    """
    Estimate usage growth rate and project future costs.
    """
    df = repo.get_daily_cost_series()
    if df.empty or len(df) < 14:
        return {}

    df["ds"] = pd.to_datetime(df["ds"])
    df = df.sort_values("ds")

    # Split into two halves
    mid = len(df) // 2
    first_half_avg = df["y"].iloc[:mid].mean()
    second_half_avg = df["y"].iloc[mid:].mean()

    growth_rate = (second_half_avg - first_half_avg) / max(0.01, first_half_avg) * 100

    # Weekly averages
    df["week"] = df["ds"].dt.isocalendar().week
    weekly = df.groupby("week")["y"].sum()

    return {
        "daily_avg_cost": round(df["y"].mean(), 2),
        "first_half_daily_avg": round(first_half_avg, 2),
        "second_half_daily_avg": round(second_half_avg, 2),
        "growth_rate_pct": round(growth_rate, 1),
        "projected_monthly_cost": round(df["y"].mean() * 30, 2),
        "weekly_avg_cost": round(weekly.mean(), 2),
    }


def try_prophet_forecast(repo: AnalyticsRepository, forecast_days: int = 14) -> pd.DataFrame | None:
    """
    Attempt Prophet-based forecasting. Returns None if Prophet is not available.
    """
    try:
        from prophet import Prophet

        df = repo.get_daily_cost_series()
        if df.empty or len(df) < 14:
            return None

        df["ds"] = pd.to_datetime(df["ds"])

        m = Prophet(
            yearly_seasonality=False,
            weekly_seasonality=True,
            daily_seasonality=False,
            changepoint_prior_scale=0.05,
        )
        m.fit(df[["ds", "y"]])

        future = m.make_future_dataframe(periods=forecast_days)
        forecast = m.predict(future)

        result = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
        result = result.merge(df[["ds", "y"]], on="ds", how="left")
        result["is_forecast"] = result["y"].isna()

        return result

    except ImportError:
        logger.info("Prophet not installed — using polynomial regression instead")
        return None
    except Exception as e:
        logger.warning(f"Prophet forecasting failed: {e}")
        return None
