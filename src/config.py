"""
Central configuration for the analytics platform.
"""

import os
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).parent.parent

# Data paths
OUTPUT_DIR = PROJECT_ROOT / "output"
TELEMETRY_FILE = OUTPUT_DIR / "telemetry_logs.jsonl"
EMPLOYEES_FILE = OUTPUT_DIR / "employees.csv"

# Database
DB_PATH = OUTPUT_DIR / "analytics.db"
DB_URL = f"sqlite:///{DB_PATH}"

# Data generation defaults
DEFAULT_NUM_USERS = 100
DEFAULT_NUM_SESSIONS = 5000
DEFAULT_DAYS = 60
DEFAULT_SEED = 42

# Event types
EVENT_TYPES = {
    "api_request": "claude_code.api_request",
    "tool_decision": "claude_code.tool_decision",
    "tool_result": "claude_code.tool_result",
    "user_prompt": "claude_code.user_prompt",
    "api_error": "claude_code.api_error",
}

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)
