"""
Tests for the ETL pipeline: ingestion, validation, transformation.
"""

import json
import tempfile
import os
import sys
from pathlib import Path

import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl.ingest import read_telemetry_jsonl, read_employees_csv
from src.etl.validate import validate_event, parse_timestamp, coerce_int, coerce_float, coerce_bool


# -----------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------

@pytest.fixture
def sample_event():
    """A minimal valid api_request event."""
    return {
        "body": "claude_code.api_request",
        "attributes": {
            "event.timestamp": "2026-01-15T10:30:45.123Z",
            "session.id": "test-session-1",
            "user.email": "test@example.com",
            "model": "claude-sonnet-4-5-20250929",
            "input_tokens": "100",
            "output_tokens": "50",
            "cost_usd": "0.005",
            "duration_ms": "3000",
            "event.name": "api_request",
        },
        "scope": {"name": "com.anthropic.claude_code.events", "version": "2.1.50"},
        "resource": {"host.arch": "arm64", "os.type": "darwin"},
    }


@pytest.fixture
def sample_batch(sample_event):
    """A single log batch containing one event."""
    return {
        "messageType": "DATA_MESSAGE",
        "logEvents": [
            {
                "id": "12345",
                "timestamp": 1736940645123,
                "message": json.dumps(sample_event),
            }
        ],
    }


@pytest.fixture
def sample_employees_csv():
    """Create a temporary employees CSV file."""
    content = "email,full_name,practice,level,location\ntest@example.com,Test User,Backend Engineering,L5,United States\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
        f.write(content)
        return f.name


# -----------------------------------------------------------------------
# Ingestion Tests
# -----------------------------------------------------------------------

class TestIngestion:
    def test_read_telemetry_jsonl(self, sample_batch):
        """Test reading a valid JSONL file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write(json.dumps(sample_batch) + "\n")
            f.flush()
            events = list(read_telemetry_jsonl(f.name))
        os.unlink(f.name)

        assert len(events) == 1
        assert events[0]["body"] == "claude_code.api_request"

    def test_read_telemetry_missing_file(self):
        """Test FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            list(read_telemetry_jsonl("/nonexistent/file.jsonl"))

    def test_read_telemetry_malformed_json(self):
        """Test handling of malformed JSON lines."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write("not-valid-json\n")
            f.flush()
            events = list(read_telemetry_jsonl(f.name))
        os.unlink(f.name)

        assert len(events) == 0

    def test_read_employees_csv(self, sample_employees_csv):
        """Test reading employee CSV."""
        employees = read_employees_csv(sample_employees_csv)
        os.unlink(sample_employees_csv)

        assert len(employees) == 1
        assert employees[0]["email"] == "test@example.com"
        assert employees[0]["practice"] == "Backend Engineering"

    def test_read_employees_missing_file(self):
        """Test FileNotFoundError for missing CSV."""
        with pytest.raises(FileNotFoundError):
            read_employees_csv("/nonexistent/employees.csv")


# -----------------------------------------------------------------------
# Validation Tests
# -----------------------------------------------------------------------

class TestValidation:
    def test_valid_api_request(self, sample_event):
        """Test validation of a valid api_request event."""
        is_valid, error = validate_event(sample_event)
        assert is_valid is True
        assert error is None

    def test_missing_body(self):
        """Test validation fails without body."""
        is_valid, error = validate_event({"attributes": {}})
        assert is_valid is False
        assert "body" in error.lower()

    def test_missing_attributes(self):
        """Test validation fails without attributes."""
        is_valid, error = validate_event({"body": "claude_code.api_request"})
        assert is_valid is False

    def test_missing_required_field(self, sample_event):
        """Test validation fails when required field is missing."""
        del sample_event["attributes"]["model"]
        is_valid, error = validate_event(sample_event)
        assert is_valid is False
        assert "model" in error

    def test_invalid_timestamp(self, sample_event):
        """Test validation fails with bad timestamp."""
        sample_event["attributes"]["event.timestamp"] = "not-a-date"
        is_valid, error = validate_event(sample_event)
        assert is_valid is False

    def test_unknown_event_type(self):
        """Test validation rejects unknown event types."""
        event = {
            "body": "claude_code.unknown_event",
            "attributes": {"event.timestamp": "2026-01-15T10:30:45.123Z"},
        }
        is_valid, error = validate_event(event)
        assert is_valid is False

    def test_valid_tool_decision(self):
        """Test validation of a tool_decision event."""
        event = {
            "body": "claude_code.tool_decision",
            "attributes": {
                "event.timestamp": "2026-01-15T10:30:45.123Z",
                "session.id": "test-session",
                "user.email": "test@example.com",
                "tool_name": "Read",
                "decision": "accept",
                "source": "config",
                "event.name": "tool_decision",
            },
        }
        is_valid, error = validate_event(event)
        assert is_valid is True


# -----------------------------------------------------------------------
# Type Coercion Tests
# -----------------------------------------------------------------------

class TestCoercion:
    def test_parse_timestamp(self):
        ts = parse_timestamp("2026-01-15T10:30:45.123Z")
        assert ts.year == 2026
        assert ts.month == 1
        assert ts.hour == 10

    def test_coerce_int(self):
        assert coerce_int("42") == 42
        assert coerce_int("abc") == 0
        assert coerce_int(None) == 0
        assert coerce_int("", 5) == 5

    def test_coerce_float(self):
        assert coerce_float("3.14") == 3.14
        assert coerce_float("abc") == 0.0
        assert coerce_float(None, 1.0) == 1.0

    def test_coerce_bool(self):
        assert coerce_bool("true") is True
        assert coerce_bool("false") is False
        assert coerce_bool(True) is True
        assert coerce_bool("abc") is False
