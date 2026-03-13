"""
Data validation: schema checks and type coercion for telemetry events.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Required fields per event type
REQUIRED_FIELDS = {
    "claude_code.api_request": [
        "event.timestamp", "session.id", "user.email",
        "model", "input_tokens", "output_tokens", "cost_usd", "duration_ms",
    ],
    "claude_code.tool_decision": [
        "event.timestamp", "session.id", "user.email",
        "tool_name", "decision", "source",
    ],
    "claude_code.tool_result": [
        "event.timestamp", "session.id", "user.email",
        "tool_name", "success", "duration_ms",
    ],
    "claude_code.user_prompt": [
        "event.timestamp", "session.id", "user.email",
        "prompt_length",
    ],
    "claude_code.api_error": [
        "event.timestamp", "session.id", "user.email",
        "error", "status_code",
    ],
}


def validate_event(event: dict) -> tuple[bool, str | None]:
    """
    Validate a single telemetry event.

    Args:
        event: Parsed event dict with body, attributes, scope, resource

    Returns:
        (is_valid, error_message)
    """
    body = event.get("body")
    if not body:
        return False, "Missing 'body' field"

    attributes = event.get("attributes")
    if not attributes or not isinstance(attributes, dict):
        return False, f"Missing or invalid 'attributes' for {body}"

    # Check required fields
    required = REQUIRED_FIELDS.get(body)
    if required is None:
        return False, f"Unknown event type: {body}"

    for field in required:
        if field not in attributes:
            return False, f"Missing required field '{field}' in {body}"

    # Validate timestamp format
    timestamp_str = attributes.get("event.timestamp", "")
    try:
        parse_timestamp(timestamp_str)
    except (ValueError, TypeError):
        return False, f"Invalid timestamp format: {timestamp_str}"

    return True, None


def parse_timestamp(timestamp_str: str) -> datetime:
    """Parse event timestamp string to datetime."""
    # Format: "2026-01-15T10:30:45.123Z"
    return datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")


def coerce_int(value, default=0) -> int:
    """Safely convert a value to int."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def coerce_float(value, default=0.0) -> float:
    """Safely convert a value to float."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def coerce_bool(value, default=False) -> bool:
    """Safely convert a value to bool."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return default


def validate_and_count(events: list[dict]) -> tuple[list[dict], dict]:
    """
    Validate a list of events and return valid ones with stats.

    Returns:
        (valid_events, stats_dict)
    """
    valid = []
    stats = {"total": 0, "valid": 0, "invalid": 0, "errors": {}}

    for event in events:
        stats["total"] += 1
        is_valid, error = validate_event(event)
        if is_valid:
            stats["valid"] += 1
            valid.append(event)
        else:
            stats["invalid"] += 1
            stats["errors"][error] = stats["errors"].get(error, 0) + 1
            logger.debug(f"Invalid event: {error}")

    logger.info(
        f"Validation: {stats['valid']}/{stats['total']} valid "
        f"({stats['invalid']} rejected)"
    )
    return valid, stats
