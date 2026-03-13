"""
API routes for analytics endpoints.
"""

import math

from fastapi import APIRouter, HTTPException, Query
from src.db.repository import AnalyticsRepository
from src.analytics import core_metrics, user_patterns, tool_analysis, predictions

router = APIRouter()


def get_repo():
    return AnalyticsRepository()


def sanitize_records(records: list[dict]) -> list[dict]:
    """Replace NaN/Inf values with None for JSON serialization."""
    for record in records:
        for key, value in record.items():
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                record[key] = None
    return records


# -----------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------

@router.get("/metrics/summary")
def get_summary():
    """Get high-level KPIs and summary statistics."""
    repo = get_repo()
    return core_metrics.get_overview_kpis(repo)


# -----------------------------------------------------------------------
# Cost & Token Analysis
# -----------------------------------------------------------------------

@router.get("/metrics/costs/daily")
def get_daily_costs():
    """Get daily cost trends."""
    repo = get_repo()
    df = core_metrics.get_cost_trends(repo)
    if df.empty:
        return []
    return df.to_dict(orient="records")


@router.get("/metrics/costs/by-model")
def get_costs_by_model():
    """Cost breakdown by model."""
    repo = get_repo()
    df = core_metrics.get_model_comparison(repo)
    if df.empty:
        return []
    return df.to_dict(orient="records")


@router.get("/metrics/costs/by-practice")
def get_costs_by_practice():
    """Cost breakdown by engineering practice."""
    repo = get_repo()
    df = repo.get_cost_by_practice()
    if df.empty:
        return []
    return df.to_dict(orient="records")


@router.get("/metrics/costs/by-level")
def get_costs_by_level():
    """Cost breakdown by seniority level."""
    repo = get_repo()
    df = user_patterns.get_level_comparison(repo)
    if df.empty:
        return []
    return df.to_dict(orient="records")


# -----------------------------------------------------------------------
# Tool Usage
# -----------------------------------------------------------------------

@router.get("/metrics/tools")
def get_tool_stats():
    """Tool usage, acceptance rates, and success rates."""
    repo = get_repo()
    df = tool_analysis.get_tool_overview(repo)
    if df.empty:
        return []
    return df.to_dict(orient="records")


@router.get("/metrics/tools/efficiency")
def get_tool_efficiency():
    """Tool efficiency scores."""
    repo = get_repo()
    df = tool_analysis.get_tool_efficiency(repo)
    if df.empty:
        return []
    return df.to_dict(orient="records")


# -----------------------------------------------------------------------
# User Analytics
# -----------------------------------------------------------------------

@router.get("/users")
def get_users():
    """Get all users with activity summaries."""
    repo = get_repo()
    df = repo.get_user_activity()
    if df.empty:
        return []
    return df.to_dict(orient="records")


@router.get("/users/top")
def get_top_users(n: int = Query(default=10, ge=1, le=100)):
    """Get top-N power users by cost."""
    repo = get_repo()
    df = user_patterns.get_power_users(repo, top_n=n)
    if df.empty:
        return []
    return df.to_dict(orient="records")


@router.get("/users/distribution")
def get_user_distribution():
    """Get usage distribution statistics (Gini, Pareto)."""
    repo = get_repo()
    return user_patterns.get_usage_distribution(repo)


@router.get("/users/by-practice")
def get_users_by_practice():
    """Usage comparison by engineering practice."""
    repo = get_repo()
    df = user_patterns.get_practice_comparison(repo)
    if df.empty:
        return []
    return df.to_dict(orient="records")


# -----------------------------------------------------------------------
# Time Patterns
# -----------------------------------------------------------------------

@router.get("/metrics/activity/hourly")
def get_hourly_activity():
    """Activity distribution by hour of day."""
    repo = get_repo()
    df = repo.get_hourly_activity()
    if df.empty:
        return []
    return df.to_dict(orient="records")


@router.get("/metrics/activity/daily")
def get_daily_activity():
    """Activity distribution by day of week."""
    repo = get_repo()
    df = repo.get_daily_activity_by_dow()
    if df.empty:
        return []
    return df.to_dict(orient="records")


# -----------------------------------------------------------------------
# Error Analysis
# -----------------------------------------------------------------------

@router.get("/metrics/errors")
def get_errors():
    """Error distribution by type."""
    repo = get_repo()
    df = repo.get_error_summary()
    if df.empty:
        return []
    return df.to_dict(orient="records")


# -----------------------------------------------------------------------
# Predictions
# -----------------------------------------------------------------------

@router.get("/predictions/forecast")
def get_forecast(days: int = Query(default=14, ge=1, le=60)):
    """Forecast daily costs."""
    repo = get_repo()
    df = predictions.forecast_costs(repo, forecast_days=days)
    if df.empty:
        return []
    return sanitize_records(df.to_dict(orient="records"))


@router.get("/predictions/anomalies")
def get_anomalies(threshold: float = Query(default=2.0, ge=1.0, le=4.0)):
    """Detect anomalous days in cost data."""
    repo = get_repo()
    df = predictions.detect_anomalies(repo, z_threshold=threshold)
    if df.empty:
        return []
    return sanitize_records(df.to_dict(orient="records"))


@router.get("/predictions/growth")
def get_growth():
    """Usage growth rate and projections."""
    repo = get_repo()
    return predictions.forecast_usage_growth(repo)
