"""
Database schema initialization and engine management.
"""

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from src.config import DB_URL
from src.db.models import Base


def get_engine(db_url=None):
    """Create and return a SQLAlchemy engine."""
    url = db_url or DB_URL
    engine = create_engine(url, echo=False)

    # Enable WAL mode and foreign keys for SQLite
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return engine


def create_tables(engine=None):
    """Create all tables defined in models."""
    if engine is None:
        engine = get_engine()
    Base.metadata.create_all(engine)
    return engine


def get_session(engine=None):
    """Create a new database session."""
    if engine is None:
        engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def drop_tables(engine=None):
    """Drop all tables (useful for re-running pipeline)."""
    if engine is None:
        engine = get_engine()
    Base.metadata.drop_all(engine)
    return engine
