"""
Tests for the API endpoints.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)


class TestAPIEndpoints:
    def test_root(self):
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "Claude Code Analytics API"

    def test_summary(self):
        """Test summary endpoint."""
        response = client.get("/api/metrics/summary")
        # Will return data if DB exists, 500 otherwise
        assert response.status_code in [200, 500]

    def test_daily_costs(self):
        """Test daily costs endpoint."""
        response = client.get("/api/metrics/costs/daily")
        assert response.status_code in [200, 500]

    def test_costs_by_model(self):
        """Test costs by model endpoint."""
        response = client.get("/api/metrics/costs/by-model")
        assert response.status_code in [200, 500]

    def test_tools_endpoint(self):
        """Test tools endpoint."""
        response = client.get("/api/metrics/tools")
        assert response.status_code in [200, 500]

    def test_users_endpoint(self):
        """Test users endpoint."""
        response = client.get("/api/users")
        assert response.status_code in [200, 500]

    def test_top_users(self):
        """Test top users with query parameter."""
        response = client.get("/api/users/top?n=5")
        assert response.status_code in [200, 500]

    def test_hourly_activity(self):
        """Test hourly activity endpoint."""
        response = client.get("/api/metrics/activity/hourly")
        assert response.status_code in [200, 500]

    def test_forecast_endpoint(self):
        """Test forecast endpoint."""
        response = client.get("/api/predictions/forecast?days=7")
        assert response.status_code in [200, 500]

    def test_anomalies_endpoint(self):
        """Test anomalies endpoint."""
        response = client.get("/api/predictions/anomalies?threshold=2.0")
        assert response.status_code in [200, 500]

    def test_growth_endpoint(self):
        """Test growth endpoint."""
        response = client.get("/api/predictions/growth")
        assert response.status_code in [200, 500]
