"""
User pattern analytics: per-user, per-role, per-level analysis.
"""

import pandas as pd
from src.db.repository import AnalyticsRepository


def get_power_users(repo: AnalyticsRepository, top_n: int = 10) -> pd.DataFrame:
    """
    Identify top-N power users by total cost.

    Returns DataFrame ranked by total_cost with user metadata.
    """
    df = repo.get_user_activity()
    if df.empty:
        return df

    df = df.sort_values("total_cost", ascending=False).head(top_n)
    df["total_tokens"] = df["total_input_tokens"] + df["total_output_tokens"]
    return df


def get_usage_distribution(repo: AnalyticsRepository) -> dict:
    """
    Analyze the distribution of usage across users.

    Returns stats about user activity distribution:
    - Gini coefficient for cost inequality
    - % of users generating 80% of cost
    - Active vs inactive user counts
    """
    df = repo.get_user_activity()
    if df.empty:
        return {}

    costs = df["total_cost"].sort_values().values

    # Gini coefficient
    n = len(costs)
    cumulative = costs.cumsum()
    total = costs.sum()
    if total == 0:
        gini = 0.0
    else:
        gini = (2 * sum((i + 1) * c for i, c in enumerate(costs)) / (n * total)) - (n + 1) / n
        gini = round(max(0, gini), 3)

    # Users generating 80% of cost (Pareto)
    if total > 0:
        threshold = total * 0.8
        cumsum = 0
        pareto_count = 0
        for c in sorted(costs, reverse=True):
            cumsum += c
            pareto_count += 1
            if cumsum >= threshold:
                break
        pareto_pct = round(pareto_count / n * 100, 1)
    else:
        pareto_pct = 0.0

    # Active users (at least 1 session)
    active = int((df["session_count"] > 0).sum())

    return {
        "total_users": n,
        "active_users": active,
        "inactive_users": n - active,
        "gini_coefficient": gini,
        "pareto_pct_users_for_80_cost": pareto_pct,
        "avg_cost_per_active_user": round(total / max(1, active), 2),
    }


def get_practice_comparison(repo: AnalyticsRepository) -> pd.DataFrame:
    """
    Compare engineering practices by usage intensity.
    """
    df = repo.get_usage_by_practice()
    if df.empty:
        return df

    df["cost_per_user"] = (df["total_cost"] / df["user_count"].clip(lower=1)).round(2)
    df["sessions_per_user"] = (df["session_count"] / df["user_count"].clip(lower=1)).round(1)
    return df


def get_level_comparison(repo: AnalyticsRepository) -> pd.DataFrame:
    """
    Compare seniority levels by usage patterns.
    """
    df = repo.get_cost_by_level()
    if df.empty:
        return df

    # Sort levels naturally (L1, L2, ..., L10)
    level_order = [f"L{i}" for i in range(1, 11)]
    df["level_rank"] = df["level"].map({l: i for i, l in enumerate(level_order)})
    df = df.sort_values("level_rank").drop(columns=["level_rank"])

    df["cost_per_user"] = (
        df["total_cost"] / df["unique_users"].clip(lower=1)
    ).round(2)
    return df


def get_location_comparison(repo: AnalyticsRepository) -> pd.DataFrame:
    """
    Compare locations by usage.
    """
    df = repo.get_usage_by_location()
    if df.empty:
        return df

    df["cost_per_user"] = (df["total_cost"] / df["user_count"].clip(lower=1)).round(2)
    return df
