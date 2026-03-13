"""
Data ingestion: read raw telemetry JSONL and employee CSV files.
"""

import csv
import json
import logging
from pathlib import Path
from typing import Generator

logger = logging.getLogger(__name__)


def read_telemetry_jsonl(filepath: str | Path) -> Generator[dict, None, None]:
    """
    Read telemetry JSONL file and yield individual events.

    Each line is a CloudWatch-style log batch containing multiple logEvents.
    We parse each batch and yield the flattened event messages.

    Yields:
        dict: Parsed event with body, attributes, scope, resource
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Telemetry file not found: {filepath}")

    line_count = 0
    event_count = 0
    error_count = 0

    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            line_count += 1

            try:
                batch = json.loads(line)
            except json.JSONDecodeError as e:
                logger.warning(f"Malformed JSON at line {line_num}: {e}")
                error_count += 1
                continue

            # Extract logEvents from the batch
            log_events = batch.get("logEvents", [])
            if not log_events:
                logger.debug(f"Empty batch at line {line_num}")
                continue

            for log_event in log_events:
                message_str = log_event.get("message", "")
                if not message_str:
                    continue

                try:
                    event = json.loads(message_str)
                    event_count += 1
                    yield event
                except json.JSONDecodeError as e:
                    logger.warning(f"Malformed event message at line {line_num}: {e}")
                    error_count += 1

    logger.info(
        f"Ingestion complete: {line_count} batches, "
        f"{event_count} events extracted, {error_count} errors"
    )


def read_employees_csv(filepath: str | Path) -> list[dict]:
    """
    Read employee CSV file.

    Returns:
        list[dict]: List of employee records with keys:
            email, full_name, practice, level, location
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Employee file not found: {filepath}")

    employees = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            employees.append({
                "email": row.get("email", "").strip(),
                "full_name": row.get("full_name", "").strip(),
                "practice": row.get("practice", "").strip(),
                "level": row.get("level", "").strip(),
                "location": row.get("location", "").strip(),
            })

    logger.info(f"Loaded {len(employees)} employees from {filepath}")
    return employees
