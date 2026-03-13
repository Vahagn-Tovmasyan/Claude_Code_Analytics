"""
Core metrics analytics: token trends, costs, session statistics.
"""

import pandas as pd
from src.db.repository import AnalyticsRepository


def get_overview_kpis(repo: AnalyticsRepository) -> dict:
    """
    Calculate key performance indicators for the overview dashboard.

    Returns dict with:
    - total_users, total_sessions, total_cost, total_tokens,
    - avg_cost_per_session, avg_cost_per_user, avg_tokens_per_session,
    - error_rate, date_range
    """
    summary = repo.get_summary_stats()

    total_tokens = summary["total_input_tokens"] + summary["total_output_tokens"]
    sessions = max(1, summary["total_sessions"])
    users = max(1, summary["total_users"])

    return {
        **summary,
        "total_tokens": total_tokens,
        "avg_cost_per_session": summary["total_cost_usd"] / sessions,
        "avg_cost_per_user": summary["total_cost_usd"] / users,
        "avg_tokens_per_session": total_tokens / sessions,
        "avg_sessions_per_user": sessions / users,
        "error_rate": (
            summary["total_errors"] /
            max(1, summary["total_api_requests"])
            * 100
        ),
    }


def get_cost_trends(repo: AnalyticsRepository) -> pd.DataFrame:
    """
    Get daily cost trends with moving averages.

    Returns DataFrame with columns:
    - date, total_cost, request_count, avg_cost_per_request,
    - cost_7d_ma, cost_14d_ma (moving averages)
    """
    df = repo.get_daily_costs()
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # Add moving averages
    df["cost_7d_ma"] = df["total_cost"].rolling(window=7, min_periods=1).mean()
    df["cost_14d_ma"] = df["total_cost"].rolling(window=14, min_periods=1).mean()

    # Cumulative cost
    df["cumulative_cost"] = df["total_cost"].cumsum()

    return df


def get_token_trends(repo: AnalyticsRepository) -> pd.DataFrame:
    """
    Get daily token consumption trends.
    """
    df = repo.get_daily_costs()
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df["total_tokens"] = df["total_input_tokens"] + df["total_output_tokens"]
    df["tokens_7d_ma"] = df["total_tokens"].rolling(window=7, min_periods=1).mean()

    return df


def get_model_comparison(repo: AnalyticsRepository) -> pd.DataFrame:
    """
    Compare models by cost, usage, and efficiency.
    """
    df = repo.get_cost_by_model()
    if df.empty:
        return df

    total_cost = df["total_cost"].sum()
    df["cost_share_pct"] = (df["total_cost"] / total_cost * 100).round(1)
    df["total_tokens"] = df["total_input_tokens"] + df["total_output_tokens"]
    df["cost_per_1k_tokens"] = (
        df["total_cost"] / (df["total_tokens"] / 1000)
    ).round(4)

    return df


def get_session_duration_stats(repo: AnalyticsRepository) -> dict:
    """Session duration statistics."""
    df = repo.get_session_stats()
    if df.empty:
        return {}

    durations = df["duration_seconds"].dropna()
    return {
        "mean_duration_min": round(durations.mean() / 60, 1),
        "median_duration_min": round(durations.median() / 60, 1),
        "p90_duration_min": round(durations.quantile(0.9) / 60, 1),
        "max_duration_min": round(durations.max() / 60, 1),
        "total_sessions": len(df),
    }
