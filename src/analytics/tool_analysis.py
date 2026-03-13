"""
Tool usage analytics: popularity, success rates, execution patterns.
"""

import pandas as pd
from src.db.repository import AnalyticsRepository


def get_tool_overview(repo: AnalyticsRepository) -> pd.DataFrame:
    """
    Comprehensive tool usage overview with acceptance and success rates.
    """
    df = repo.get_tool_popularity()
    if df.empty:
        return df

    # Calculate rates
    df["acceptance_rate"] = (
        df["accepted"] / df["decisions"].clip(lower=1) * 100
    ).round(1)
    df["success_rate"] = (
        df["successes"] / df["results"].clip(lower=1) * 100
    ).round(1)

    return df


def get_tool_efficiency(repo: AnalyticsRepository) -> pd.DataFrame:
    """
    Tool efficiency analysis combining duration and success rate.
    """
    durations = repo.get_tool_durations()
    success = repo.get_tool_success_rates()

    if durations.empty or success.empty:
        return durations

    merged = pd.merge(durations, success, on="tool_name", how="outer")
    merged = merged.fillna(0)

    # Efficiency score: higher success rate + lower duration = better
    max_dur = merged["avg_duration_ms"].max()
    if max_dur > 0:
        merged["efficiency_score"] = (
            merged["success_rate"] *
            (1 - merged["avg_duration_ms"] / max_dur)
        ).round(1)
    else:
        merged["efficiency_score"] = merged["success_rate"]

    return merged.sort_values("efficiency_score", ascending=False)


def get_tool_usage_by_practice(repo: AnalyticsRepository) -> pd.DataFrame:
    """
    Tool usage broken down by engineering practice.
    """
    query = """
        SELECT te.tool_name, e.practice,
               COUNT(*) as usage_count
        FROM tool_events te
        JOIN employees e ON te.user_email = e.email
        WHERE te.event_type = 'result'
        GROUP BY te.tool_name, e.practice
        ORDER BY usage_count DESC
    """
    return repo._read_sql(query)


def get_decision_source_distribution(repo: AnalyticsRepository) -> pd.DataFrame:
    """
    Distribution of how tool decisions are made.
    """
    query = """
        SELECT decision_source, decision,
               COUNT(*) as count
        FROM tool_events
        WHERE event_type = 'decision'
        GROUP BY decision_source, decision
        ORDER BY count DESC
    """
    return repo._read_sql(query)
