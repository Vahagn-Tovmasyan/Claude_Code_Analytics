#!/usr/bin/env python3
"""
Pipeline runner: orchestrates data generation, ETL, and database loading.

Usage:
    python run_pipeline.py                    # Full pipeline
    python run_pipeline.py --skip-generate    # Skip data generation
    python run_pipeline.py --num-users 100 --num-sessions 5000 --days 60
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.config import (
    OUTPUT_DIR, TELEMETRY_FILE, EMPLOYEES_FILE, DB_PATH,
    DEFAULT_NUM_USERS, DEFAULT_NUM_SESSIONS, DEFAULT_DAYS, DEFAULT_SEED,
)
from src.db.schema import get_engine, create_tables, drop_tables, get_session
from src.etl.ingest import read_telemetry_jsonl, read_employees_csv
from src.etl.validate import validate_event
from src.etl.transform import transform_and_load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


def run_data_generation(num_users: int, num_sessions: int, days: int, seed: int):
    """Run the data generator script."""
    import subprocess
    logger.info("=== Step 1: Generating synthetic data ===")
    cmd = [
        sys.executable, "generate_fake_data.py",
        "--num-users", str(num_users),
        "--num-sessions", str(num_sessions),
        "--days", str(days),
        "--output-dir", str(OUTPUT_DIR),
        "--seed", str(seed),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Data generation failed:\n{result.stderr}")
        sys.exit(1)
    logger.info(result.stdout)


def run_etl():
    """Run the ETL pipeline: ingest, validate, transform, load."""
    logger.info("=== Step 2: Running ETL pipeline ===")

    # Ingest
    t0 = time.time()
    logger.info(f"Reading telemetry from {TELEMETRY_FILE}")
    events = list(read_telemetry_jsonl(TELEMETRY_FILE))
    logger.info(f"  Ingested {len(events)} events in {time.time()-t0:.1f}s")

    logger.info(f"Reading employees from {EMPLOYEES_FILE}")
    employees = read_employees_csv(EMPLOYEES_FILE)
    logger.info(f"  Loaded {len(employees)} employees")

    # Validate
    t1 = time.time()
    valid_events = []
    invalid_count = 0
    for event in events:
        is_valid, error = validate_event(event)
        if is_valid:
            valid_events.append(event)
        else:
            invalid_count += 1
    logger.info(
        f"  Validated: {len(valid_events)} valid, {invalid_count} invalid "
        f"in {time.time()-t1:.1f}s"
    )

    # Create database
    t2 = time.time()
    logger.info(f"Creating database at {DB_PATH}")
    engine = get_engine()
    drop_tables(engine)  # Fresh start
    create_tables(engine)

    # Transform and load
    db_session = get_session(engine)
    try:
        transform_and_load(valid_events, employees, db_session)
        logger.info(f"  ETL complete in {time.time()-t2:.1f}s")
    except Exception as e:
        db_session.rollback()
        logger.error(f"ETL failed: {e}")
        raise
    finally:
        db_session.close()


def verify_database():
    """Quick verification that data was loaded correctly."""
    logger.info("=== Step 3: Verifying database ===")
    from src.db.repository import AnalyticsRepository
    repo = AnalyticsRepository()
    stats = repo.get_summary_stats()
    for key, value in stats.items():
        logger.info(f"  {key}: {value}")


def main():
    parser = argparse.ArgumentParser(description="Run the analytics pipeline")
    parser.add_argument("--skip-generate", action="store_true",
                        help="Skip data generation (use existing files)")
    parser.add_argument("--num-users", type=int, default=DEFAULT_NUM_USERS)
    parser.add_argument("--num-sessions", type=int, default=DEFAULT_NUM_SESSIONS)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    total_start = time.time()

    # Step 1: Generate data
    if not args.skip_generate:
        run_data_generation(args.num_users, args.num_sessions, args.days, args.seed)
    else:
        logger.info("Skipping data generation (--skip-generate)")

    # Step 2: ETL
    run_etl()

    # Step 3: Verify
    verify_database()

    elapsed = time.time() - total_start
    logger.info(f"\n{'='*50}")
    logger.info(f"Pipeline complete in {elapsed:.1f}s")
    logger.info(f"Database: {DB_PATH}")
    logger.info(f"Launch dashboard: streamlit run dashboard/app.py")
    logger.info(f"Launch API: uvicorn src.api.main:app --reload")


if __name__ == "__main__":
    main()
