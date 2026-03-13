"""
Tests for the analytics layer.
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics import core_metrics, user_patterns, tool_analysis, predictions


# -----------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------

@pytest.fixture
def mock_repo():
    """Create a mock AnalyticsRepository."""
    repo = MagicMock()

    # Summary stats
    repo.get_summary_stats.return_value = {
        "total_users": 10,
        "total_sessions": 100,
        "total_api_requests": 1000,
        "total_cost_usd": 500.0,
        "total_input_tokens": 50000,
        "total_output_tokens": 25000,
        "total_tool_events": 800,
        "total_prompts": 200,
        "total_errors": 5,
        "date_range_start": "2026-01-01",
        "date_range_end": "2026-01-30",
    }

    # Daily costs
    dates = pd.date_range("2026-01-01", periods=30, freq="D")
    repo.get_daily_costs.return_value = pd.DataFrame({
        "date": dates.strftime("%Y-%m-%d"),
        "request_count": np.random.randint(10, 100, 30),
        "total_cost": np.random.uniform(5, 30, 30),
        "total_input_tokens": np.random.randint(1000, 5000, 30),
        "total_output_tokens": np.random.randint(500, 2000, 30),
        "avg_cost_per_request": np.random.uniform(0.01, 0.1, 30),
        "avg_duration_ms": np.random.uniform(3000, 15000, 30),
    })

    # Cost by model
    repo.get_cost_by_model.return_value = pd.DataFrame({
        "model": ["claude-haiku-4-5", "claude-opus-4-6", "claude-sonnet-4-5"],
        "request_count": [500, 300, 200],
        "total_cost": [50, 250, 200],
        "avg_cost": [0.1, 0.83, 1.0],
        "total_input_tokens": [25000, 15000, 10000],
        "total_output_tokens": [10000, 8000, 7000],
        "avg_duration_ms": [5000, 10000, 12000],
    })

    # User activity
    repo.get_user_activity.return_value = pd.DataFrame({
        "email": [f"user{i}@example.com" for i in range(10)],
        "full_name": [f"User {i}" for i in range(10)],
        "practice": ["Backend Engineering"] * 5 + ["Frontend Engineering"] * 5,
        "level": [f"L{i+1}" for i in range(10)],
        "location": ["United States"] * 10,
        "session_count": list(range(5, 55, 5)),
        "total_cost": [10, 20, 30, 50, 80, 100, 120, 150, 200, 300],
        "total_input_tokens": [1000 * i for i in range(1, 11)],
        "total_output_tokens": [500 * i for i in range(1, 11)],
        "total_events": [100 * i for i in range(1, 11)],
    })

    # Tool popularity
    repo.get_tool_popularity.return_value = pd.DataFrame({
        "tool_name": ["Read", "Bash", "Edit", "Grep"],
        "total_events": [400, 350, 200, 100],
        "decisions": [200, 180, 100, 50],
        "accepted": [195, 170, 98, 49],
        "rejected": [5, 10, 2, 1],
        "results": [200, 170, 100, 50],
        "successes": [198, 155, 99, 50],
    })

    # Tool success rates
    repo.get_tool_success_rates.return_value = pd.DataFrame({
        "tool_name": ["Read", "Bash", "Edit"],
        "total_results": [200, 170, 100],
        "successes": [198, 155, 99],
        "success_rate": [99.0, 91.2, 99.0],
    })

    # Tool durations
    repo.get_tool_durations.return_value = pd.DataFrame({
        "tool_name": ["Read", "Bash", "Edit"],
        "executions": [200, 170, 100],
        "avg_duration_ms": [34, 5169, 1817],
        "max_duration_ms": [500, 30000, 10000],
    })

    # Daily cost series for predictions
    repo.get_daily_cost_series.return_value = pd.DataFrame({
        "ds": pd.date_range("2026-01-01", periods=30, freq="D").strftime("%Y-%m-%d"),
        "y": np.random.uniform(10, 30, 30),
    })

    # Cost by level
    repo.get_cost_by_level.return_value = pd.DataFrame({
        "level": [f"L{i}" for i in range(1, 6)],
        "request_count": [100, 200, 300, 400, 500],
        "total_cost": [10, 30, 60, 100, 150],
        "avg_cost_per_request": [0.1, 0.15, 0.2, 0.25, 0.3],
        "unique_users": [2, 3, 4, 5, 6],
    })

    # Usage by practice
    repo.get_usage_by_practice.return_value = pd.DataFrame({
        "practice": ["Backend Engineering", "Frontend Engineering"],
        "user_count": [5, 5],
        "session_count": [80, 60],
        "total_cost": [300, 200],
        "avg_session_cost": [3.75, 3.33],
    })

    # Usage by location
    repo.get_usage_by_location.return_value = pd.DataFrame({
        "location": ["United States", "Germany"],
        "user_count": [5, 5],
        "session_count": [80, 50],
        "total_cost": [350, 150],
    })

    # Session stats
    repo.get_session_stats.return_value = pd.DataFrame({
        "session_id": [f"sess-{i}" for i in range(10)],
        "user_email": [f"user{i}@example.com" for i in range(10)],
        "started_at": pd.date_range("2026-01-01", periods=10, freq="D"),
        "ended_at": pd.date_range("2026-01-01 01:00", periods=10, freq="D"),
        "event_count": [50] * 10,
        "total_cost_usd": [20.0] * 10,
        "total_input_tokens": [2000] * 10,
        "total_output_tokens": [1000] * 10,
        "practice": ["Backend Engineering"] * 10,
        "level": ["L5"] * 10,
        "location": ["United States"] * 10,
        "duration_seconds": [3600] * 10,
    })

    return repo


# -----------------------------------------------------------------------
# Core Metrics Tests
# -----------------------------------------------------------------------

class TestCoreMetrics:
    def test_overview_kpis(self, mock_repo):
        """Test KPI calculation."""
        kpis = core_metrics.get_overview_kpis(mock_repo)

        assert kpis["total_users"] == 10
        assert kpis["total_sessions"] == 100
        assert kpis["total_cost_usd"] == 500.0
        assert kpis["total_tokens"] == 75000
        assert kpis["avg_cost_per_session"] == 5.0
        assert kpis["avg_cost_per_user"] == 50.0

    def test_cost_trends(self, mock_repo):
        """Test cost trend calculation with moving averages."""
        df = core_metrics.get_cost_trends(mock_repo)

        assert not df.empty
        assert "cost_7d_ma" in df.columns
        assert "cost_14d_ma" in df.columns
        assert "cumulative_cost" in df.columns
        assert df["cumulative_cost"].is_monotonic_increasing

    def test_model_comparison(self, mock_repo):
        """Test model comparison metrics."""
        df = core_metrics.get_model_comparison(mock_repo)

        assert not df.empty
        assert "cost_share_pct" in df.columns
        assert "cost_per_1k_tokens" in df.columns
        assert abs(df["cost_share_pct"].sum() - 100.0) < 0.1

    def test_session_duration_stats(self, mock_repo):
        """Test session duration statistics."""
        stats = core_metrics.get_session_duration_stats(mock_repo)

        assert stats["mean_duration_min"] == 60.0
        assert stats["total_sessions"] == 10


# -----------------------------------------------------------------------
# User Patterns Tests
# -----------------------------------------------------------------------

class TestUserPatterns:
    def test_power_users(self, mock_repo):
        """Test power user identification."""
        df = user_patterns.get_power_users(mock_repo, top_n=5)

        assert len(df) == 5
        assert df["total_cost"].iloc[0] >= df["total_cost"].iloc[-1]

    def test_usage_distribution(self, mock_repo):
        """Test usage distribution calculation."""
        dist = user_patterns.get_usage_distribution(mock_repo)

        assert "gini_coefficient" in dist
        assert "pareto_pct_users_for_80_cost" in dist
        assert 0 <= dist["gini_coefficient"] <= 1
        assert dist["active_users"] == 10

    def test_practice_comparison(self, mock_repo):
        """Test practice comparison."""
        df = user_patterns.get_practice_comparison(mock_repo)

        assert not df.empty
        assert "cost_per_user" in df.columns
        assert "sessions_per_user" in df.columns

    def test_level_comparison(self, mock_repo):
        """Test level comparison with natural sorting."""
        df = user_patterns.get_level_comparison(mock_repo)

        assert not df.empty
        assert df["level"].iloc[0] == "L1"


# -----------------------------------------------------------------------
# Tool Analysis Tests
# -----------------------------------------------------------------------

class TestToolAnalysis:
    def test_tool_overview(self, mock_repo):
        """Test tool overview with rates."""
        df = tool_analysis.get_tool_overview(mock_repo)

        assert not df.empty
        assert "acceptance_rate" in df.columns
        assert "success_rate" in df.columns

    def test_tool_efficiency(self, mock_repo):
        """Test tool efficiency scoring."""
        df = tool_analysis.get_tool_efficiency(mock_repo)

        assert not df.empty
        assert "efficiency_score" in df.columns


# -----------------------------------------------------------------------
# Predictions Tests
# -----------------------------------------------------------------------

class TestPredictions:
    def test_forecast_costs(self, mock_repo):
        """Test cost forecasting."""
        df = predictions.forecast_costs(mock_repo, forecast_days=7)

        assert not df.empty
        assert "yhat" in df.columns
        assert "is_forecast" in df.columns
        forecast_rows = df[df["is_forecast"]]
        assert len(forecast_rows) == 7

    def test_detect_anomalies(self, mock_repo):
        """Test anomaly detection."""
        df = predictions.detect_anomalies(mock_repo)

        assert not df.empty
        assert "is_anomaly" in df.columns
        assert "z_score" in df.columns

    def test_forecast_usage_growth(self, mock_repo):
        """Test usage growth estimation."""
        growth = predictions.forecast_usage_growth(mock_repo)

        assert "daily_avg_cost" in growth
        assert "growth_rate_pct" in growth
        assert "projected_monthly_cost" in growth
