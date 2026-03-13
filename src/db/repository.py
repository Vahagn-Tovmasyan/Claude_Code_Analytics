"""
Repository layer: query functions for analytics and API.

Provides a clean data access interface using pandas for aggregated results.
All DB query logic is isolated here (Repository Pattern).
"""

import pandas as pd
from sqlalchemy import create_engine, text

from src.config import DB_URL


class AnalyticsRepository:
    """Repository for all analytics queries."""

    def __init__(self, db_url: str | None = None):
        self.db_url = db_url or DB_URL
        self.engine = create_engine(self.db_url, echo=False)

    def _read_sql(self, query: str, params: dict | None = None) -> pd.DataFrame:
        """Execute SQL query and return DataFrame."""
        with self.engine.connect() as conn:
            return pd.read_sql(text(query), conn, params=params or {})

    # -----------------------------------------------------------------------
    # Summary / Overview Metrics
    # -----------------------------------------------------------------------

    def get_summary_stats(self) -> dict:
        """Get high-level summary statistics."""
        results = {}

        df = self._read_sql("SELECT COUNT(*) as cnt FROM employees")
        results["total_users"] = int(df["cnt"].iloc[0])

        df = self._read_sql("SELECT COUNT(*) as cnt FROM sessions")
        results["total_sessions"] = int(df["cnt"].iloc[0])

        df = self._read_sql("""
            SELECT COUNT(*) as cnt,
                   COALESCE(SUM(cost_usd), 0) as total_cost,
                   COALESCE(SUM(input_tokens), 0) as total_input_tokens,
                   COALESCE(SUM(output_tokens), 0) as total_output_tokens
            FROM api_requests
        """)
        results["total_api_requests"] = int(df["cnt"].iloc[0])
        results["total_cost_usd"] = float(df["total_cost"].iloc[0])
        results["total_input_tokens"] = int(df["total_input_tokens"].iloc[0])
        results["total_output_tokens"] = int(df["total_output_tokens"].iloc[0])

        df = self._read_sql("SELECT COUNT(*) as cnt FROM tool_events")
        results["total_tool_events"] = int(df["cnt"].iloc[0])

        df = self._read_sql("SELECT COUNT(*) as cnt FROM user_prompts")
        results["total_prompts"] = int(df["cnt"].iloc[0])

        df = self._read_sql("SELECT COUNT(*) as cnt FROM api_errors")
        results["total_errors"] = int(df["cnt"].iloc[0])

        df = self._read_sql("""
            SELECT MIN(started_at) as min_date, MAX(ended_at) as max_date
            FROM sessions
        """)
        results["date_range_start"] = df["min_date"].iloc[0]
        results["date_range_end"] = df["max_date"].iloc[0]

        return results

    # -----------------------------------------------------------------------
    # Cost & Token Analysis
    # -----------------------------------------------------------------------

    def get_daily_costs(self) -> pd.DataFrame:
        """Daily cost and token usage aggregates."""
        return self._read_sql("""
            SELECT DATE(timestamp) as date,
                   COUNT(*) as request_count,
                   SUM(cost_usd) as total_cost,
                   SUM(input_tokens) as total_input_tokens,
                   SUM(output_tokens) as total_output_tokens,
                   AVG(cost_usd) as avg_cost_per_request,
                   AVG(duration_ms) as avg_duration_ms
            FROM api_requests
            GROUP BY DATE(timestamp)
            ORDER BY date
        """)

    def get_cost_by_model(self) -> pd.DataFrame:
        """Cost and usage breakdown by model."""
        return self._read_sql("""
            SELECT model,
                   COUNT(*) as request_count,
                   SUM(cost_usd) as total_cost,
                   AVG(cost_usd) as avg_cost,
                   SUM(input_tokens) as total_input_tokens,
                   SUM(output_tokens) as total_output_tokens,
                   AVG(duration_ms) as avg_duration_ms
            FROM api_requests
            GROUP BY model
            ORDER BY total_cost DESC
        """)

    def get_cost_by_practice(self) -> pd.DataFrame:
        """Cost breakdown by engineering practice."""
        return self._read_sql("""
            SELECT e.practice,
                   COUNT(*) as request_count,
                   SUM(a.cost_usd) as total_cost,
                   AVG(a.cost_usd) as avg_cost,
                   COUNT(DISTINCT a.user_email) as unique_users
            FROM api_requests a
            JOIN employees e ON a.user_email = e.email
            GROUP BY e.practice
            ORDER BY total_cost DESC
        """)

    def get_cost_by_level(self) -> pd.DataFrame:
        """Cost breakdown by seniority level."""
        return self._read_sql("""
            SELECT e.level,
                   COUNT(*) as request_count,
                   SUM(a.cost_usd) as total_cost,
                   AVG(a.cost_usd) as avg_cost_per_request,
                   COUNT(DISTINCT a.user_email) as unique_users
            FROM api_requests a
            JOIN employees e ON a.user_email = e.email
            GROUP BY e.level
            ORDER BY e.level
        """)

    # -----------------------------------------------------------------------
    # Tool Usage
    # -----------------------------------------------------------------------

    def get_tool_popularity(self) -> pd.DataFrame:
        """Tool usage frequency and acceptance rates."""
        return self._read_sql("""
            SELECT tool_name,
                   COUNT(*) as total_events,
                   SUM(CASE WHEN event_type = 'decision' THEN 1 ELSE 0 END) as decisions,
                   SUM(CASE WHEN event_type = 'decision' AND decision = 'accept' THEN 1 ELSE 0 END) as accepted,
                   SUM(CASE WHEN event_type = 'decision' AND decision = 'reject' THEN 1 ELSE 0 END) as rejected,
                   SUM(CASE WHEN event_type = 'result' THEN 1 ELSE 0 END) as results,
                   SUM(CASE WHEN event_type = 'result' AND success = 1 THEN 1 ELSE 0 END) as successes
            FROM tool_events
            GROUP BY tool_name
            ORDER BY total_events DESC
        """)

    def get_tool_success_rates(self) -> pd.DataFrame:
        """Tool success rates from result events."""
        return self._read_sql("""
            SELECT tool_name,
                   COUNT(*) as total_results,
                   SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successes,
                   ROUND(100.0 * SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) as success_rate
            FROM tool_events
            WHERE event_type = 'result'
            GROUP BY tool_name
            ORDER BY success_rate ASC
        """)

    def get_tool_durations(self) -> pd.DataFrame:
        """Average tool execution durations."""
        return self._read_sql("""
            SELECT tool_name,
                   COUNT(*) as executions,
                   AVG(duration_ms) as avg_duration_ms,
                   MAX(duration_ms) as max_duration_ms
            FROM tool_events
            WHERE event_type = 'result' AND duration_ms IS NOT NULL
            GROUP BY tool_name
            ORDER BY avg_duration_ms DESC
        """)

    # -----------------------------------------------------------------------
    # User Patterns
    # -----------------------------------------------------------------------

    def get_user_activity(self) -> pd.DataFrame:
        """Per-user activity summary."""
        return self._read_sql("""
            SELECT e.email, e.full_name, e.practice, e.level, e.location,
                   COUNT(DISTINCT s.session_id) as session_count,
                   COALESCE(SUM(s.total_cost_usd), 0) as total_cost,
                   COALESCE(SUM(s.total_input_tokens), 0) as total_input_tokens,
                   COALESCE(SUM(s.total_output_tokens), 0) as total_output_tokens,
                   COALESCE(SUM(s.event_count), 0) as total_events
            FROM employees e
            LEFT JOIN sessions s ON e.email = s.user_email
            GROUP BY e.email, e.full_name, e.practice, e.level, e.location
            ORDER BY total_cost DESC
        """)

    def get_usage_by_practice(self) -> pd.DataFrame:
        """Aggregated usage by engineering practice."""
        return self._read_sql("""
            SELECT e.practice,
                   COUNT(DISTINCT e.email) as user_count,
                   COUNT(DISTINCT s.session_id) as session_count,
                   COALESCE(SUM(s.total_cost_usd), 0) as total_cost,
                   COALESCE(AVG(s.total_cost_usd), 0) as avg_session_cost
            FROM employees e
            LEFT JOIN sessions s ON e.email = s.user_email
            GROUP BY e.practice
            ORDER BY total_cost DESC
        """)

    def get_usage_by_location(self) -> pd.DataFrame:
        """Aggregated usage by location."""
        return self._read_sql("""
            SELECT e.location,
                   COUNT(DISTINCT e.email) as user_count,
                   COUNT(DISTINCT s.session_id) as session_count,
                   COALESCE(SUM(s.total_cost_usd), 0) as total_cost
            FROM employees e
            LEFT JOIN sessions s ON e.email = s.user_email
            GROUP BY e.location
            ORDER BY total_cost DESC
        """)

    # -----------------------------------------------------------------------
    # Time Patterns
    # -----------------------------------------------------------------------

    def get_hourly_activity(self) -> pd.DataFrame:
        """Activity distribution by hour of day."""
        return self._read_sql("""
            SELECT CAST(strftime('%H', timestamp) AS INTEGER) as hour,
                   COUNT(*) as request_count,
                   SUM(cost_usd) as total_cost
            FROM api_requests
            GROUP BY hour
            ORDER BY hour
        """)

    def get_daily_activity_by_dow(self) -> pd.DataFrame:
        """Activity distribution by day of week (0=Sunday)."""
        return self._read_sql("""
            SELECT CAST(strftime('%w', timestamp) AS INTEGER) as day_of_week,
                   COUNT(*) as request_count,
                   SUM(cost_usd) as total_cost
            FROM api_requests
            GROUP BY day_of_week
            ORDER BY day_of_week
        """)

    # -----------------------------------------------------------------------
    # Error Analysis
    # -----------------------------------------------------------------------

    def get_error_summary(self) -> pd.DataFrame:
        """Error distribution by type and status code."""
        return self._read_sql("""
            SELECT error, status_code,
                   COUNT(*) as error_count
            FROM api_errors
            GROUP BY error, status_code
            ORDER BY error_count DESC
        """)

    def get_daily_errors(self) -> pd.DataFrame:
        """Daily error counts."""
        return self._read_sql("""
            SELECT DATE(timestamp) as date,
                   COUNT(*) as error_count
            FROM api_errors
            GROUP BY DATE(timestamp)
            ORDER BY date
        """)

    # -----------------------------------------------------------------------
    # Session Analysis
    # -----------------------------------------------------------------------

    def get_session_stats(self) -> pd.DataFrame:
        """Session-level statistics."""
        return self._read_sql("""
            SELECT s.session_id, s.user_email, s.started_at, s.ended_at,
                   s.event_count, s.total_cost_usd,
                   s.total_input_tokens, s.total_output_tokens,
                   e.practice, e.level, e.location,
                   ROUND((julianday(s.ended_at) - julianday(s.started_at)) * 86400, 0) as duration_seconds
            FROM sessions s
            JOIN employees e ON s.user_email = e.email
            ORDER BY s.started_at
        """)

    # -----------------------------------------------------------------------
    # Raw data for ML / predictions
    # -----------------------------------------------------------------------

    def get_daily_cost_series(self) -> pd.DataFrame:
        """Daily cost time series for forecasting."""
        return self._read_sql("""
            SELECT DATE(timestamp) as ds,
                   SUM(cost_usd) as y
            FROM api_requests
            GROUP BY DATE(timestamp)
            ORDER BY ds
        """)

    def get_daily_usage_series(self) -> pd.DataFrame:
        """Daily usage (request count) time series."""
        return self._read_sql("""
            SELECT DATE(timestamp) as ds,
                   COUNT(*) as y
            FROM api_requests
            GROUP BY DATE(timestamp)
            ORDER BY ds
        """)

    def get_model_daily_usage(self) -> pd.DataFrame:
        """Daily usage breakdown by model."""
        return self._read_sql("""
            SELECT DATE(timestamp) as date, model,
                   COUNT(*) as request_count,
                   SUM(cost_usd) as total_cost
            FROM api_requests
            GROUP BY DATE(timestamp), model
            ORDER BY date, model
        """)
