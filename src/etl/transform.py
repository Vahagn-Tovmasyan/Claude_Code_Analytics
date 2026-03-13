"""
Data transformation: flatten events, enrich, and load into database.
"""

import logging
from collections import defaultdict
from datetime import datetime

from sqlalchemy.orm import Session as DbSession

from src.db.models import Employee, Session, ApiRequest, ToolEvent, UserPrompt, ApiError
from src.etl.validate import parse_timestamp, coerce_int, coerce_float, coerce_bool

logger = logging.getLogger(__name__)


def transform_and_load(events: list[dict], employees: list[dict], db_session: DbSession):
    """
    Transform raw events and load into the database.

    Steps:
    1. Load employees
    2. Classify events by type
    3. Derive sessions from events
    4. Insert all records via bulk operations
    """
    # Step 1: Load employees
    _load_employees(employees, db_session)

    # Step 2: Classify events by type
    classified = _classify_events(events)

    # Step 3: Derive and load sessions
    _load_sessions(events, db_session)

    # Step 4: Load each event type
    _load_api_requests(classified.get("claude_code.api_request", []), db_session)
    _load_tool_decisions(classified.get("claude_code.tool_decision", []), db_session)
    _load_tool_results(classified.get("claude_code.tool_result", []), db_session)
    _load_user_prompts(classified.get("claude_code.user_prompt", []), db_session)
    _load_api_errors(classified.get("claude_code.api_error", []), db_session)

    db_session.commit()
    logger.info("All data loaded and committed successfully")


def _load_employees(employees: list[dict], db_session: DbSession):
    """Bulk insert employee records."""
    records = [
        Employee(
            email=emp["email"],
            full_name=emp["full_name"],
            practice=emp["practice"],
            level=emp["level"],
            location=emp["location"],
        )
        for emp in employees
    ]
    db_session.bulk_save_objects(records)
    db_session.flush()
    logger.info(f"Loaded {len(records)} employees")


def _classify_events(events: list[dict]) -> dict[str, list[dict]]:
    """Group events by their body/type."""
    classified = defaultdict(list)
    for event in events:
        body = event.get("body", "unknown")
        classified[body].append(event)

    for body, evts in classified.items():
        logger.info(f"  {body}: {len(evts)} events")

    return dict(classified)


def _load_sessions(events: list[dict], db_session: DbSession):
    """Derive session records from events and insert."""
    session_data = defaultdict(lambda: {
        "start": None, "end": None, "count": 0,
        "user_email": None, "cost": 0.0,
        "input_tokens": 0, "output_tokens": 0,
    })

    for event in events:
        attrs = event.get("attributes", {})
        sid = attrs.get("session.id")
        if not sid:
            continue

        ts = parse_timestamp(attrs["event.timestamp"])
        sd = session_data[sid]
        sd["count"] += 1
        sd["user_email"] = attrs.get("user.email")

        if sd["start"] is None or ts < sd["start"]:
            sd["start"] = ts
        if sd["end"] is None or ts > sd["end"]:
            sd["end"] = ts

        # Accumulate costs and tokens from API requests
        if event.get("body") == "claude_code.api_request":
            sd["cost"] += coerce_float(attrs.get("cost_usd", 0))
            sd["input_tokens"] += coerce_int(attrs.get("input_tokens", 0))
            sd["output_tokens"] += coerce_int(attrs.get("output_tokens", 0))

    records = [
        Session(
            session_id=sid,
            user_email=data["user_email"],
            started_at=data["start"],
            ended_at=data["end"],
            event_count=data["count"],
            total_cost_usd=round(data["cost"], 6),
            total_input_tokens=data["input_tokens"],
            total_output_tokens=data["output_tokens"],
        )
        for sid, data in session_data.items()
        if data["user_email"] is not None
    ]
    db_session.bulk_save_objects(records)
    db_session.flush()
    logger.info(f"Loaded {len(records)} sessions")


def _load_api_requests(events: list[dict], db_session: DbSession):
    """Load API request events."""
    records = []
    for event in events:
        attrs = event["attributes"]
        records.append(ApiRequest(
            session_id=attrs["session.id"],
            user_email=attrs["user.email"],
            timestamp=parse_timestamp(attrs["event.timestamp"]),
            model=attrs.get("model", "unknown"),
            input_tokens=coerce_int(attrs.get("input_tokens")),
            output_tokens=coerce_int(attrs.get("output_tokens")),
            cache_read_tokens=coerce_int(attrs.get("cache_read_tokens")),
            cache_creation_tokens=coerce_int(attrs.get("cache_creation_tokens")),
            cost_usd=coerce_float(attrs.get("cost_usd")),
            duration_ms=coerce_int(attrs.get("duration_ms")),
        ))
    db_session.bulk_save_objects(records)
    db_session.flush()
    logger.info(f"Loaded {len(records)} API request events")


def _load_tool_decisions(events: list[dict], db_session: DbSession):
    """Load tool decision events."""
    records = []
    for event in events:
        attrs = event["attributes"]
        records.append(ToolEvent(
            session_id=attrs["session.id"],
            user_email=attrs["user.email"],
            timestamp=parse_timestamp(attrs["event.timestamp"]),
            event_type="decision",
            tool_name=attrs.get("tool_name", "unknown"),
            decision=attrs.get("decision"),
            decision_source=attrs.get("source"),
            success=None,
            duration_ms=None,
        ))
    db_session.bulk_save_objects(records)
    db_session.flush()
    logger.info(f"Loaded {len(records)} tool decision events")


def _load_tool_results(events: list[dict], db_session: DbSession):
    """Load tool result events."""
    records = []
    for event in events:
        attrs = event["attributes"]
        records.append(ToolEvent(
            session_id=attrs["session.id"],
            user_email=attrs["user.email"],
            timestamp=parse_timestamp(attrs["event.timestamp"]),
            event_type="result",
            tool_name=attrs.get("tool_name", "unknown"),
            decision=attrs.get("decision_type"),
            decision_source=attrs.get("decision_source"),
            success=coerce_bool(attrs.get("success")),
            duration_ms=coerce_int(attrs.get("duration_ms")),
        ))
    db_session.bulk_save_objects(records)
    db_session.flush()
    logger.info(f"Loaded {len(records)} tool result events")


def _load_user_prompts(events: list[dict], db_session: DbSession):
    """Load user prompt events."""
    records = []
    for event in events:
        attrs = event["attributes"]
        records.append(UserPrompt(
            session_id=attrs["session.id"],
            user_email=attrs["user.email"],
            timestamp=parse_timestamp(attrs["event.timestamp"]),
            prompt_length=coerce_int(attrs.get("prompt_length")),
        ))
    db_session.bulk_save_objects(records)
    db_session.flush()
    logger.info(f"Loaded {len(records)} user prompt events")


def _load_api_errors(events: list[dict], db_session: DbSession):
    """Load API error events."""
    records = []
    for event in events:
        attrs = event["attributes"]
        records.append(ApiError(
            session_id=attrs["session.id"],
            user_email=attrs["user.email"],
            timestamp=parse_timestamp(attrs["event.timestamp"]),
            model=attrs.get("model"),
            error=attrs.get("error"),
            status_code=attrs.get("status_code"),
            attempt=coerce_int(attrs.get("attempt", 1)),
            duration_ms=coerce_int(attrs.get("duration_ms")),
        ))
    db_session.bulk_save_objects(records)
    db_session.flush()
    logger.info(f"Loaded {len(records)} API error events")
