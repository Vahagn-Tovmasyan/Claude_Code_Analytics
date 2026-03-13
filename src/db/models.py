"""
SQLAlchemy ORM models for the analytics database.

Tables:
- employees: User/employee metadata
- sessions: Coding session aggregates
- api_requests: LLM API call events
- tool_events: Tool decision + result events
- user_prompts: User prompt events
- api_errors: API error events
"""

from sqlalchemy import (
    Column, Integer, Text, Float, Boolean, DateTime, Index,
    create_engine, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


class Employee(Base):
    __tablename__ = "employees"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(Text, unique=True, nullable=False, index=True)
    full_name = Column(Text, nullable=False)
    practice = Column(Text, nullable=False)
    level = Column(Text, nullable=False)
    location = Column(Text, nullable=False)

    # Relationships
    sessions = relationship("Session", back_populates="employee")

    def __repr__(self):
        return f"<Employee(email='{self.email}', practice='{self.practice}', level='{self.level}')>"


class Session(Base):
    __tablename__ = "sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Text, unique=True, nullable=False, index=True)
    user_email = Column(Text, ForeignKey("employees.email"), nullable=False)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=False)
    event_count = Column(Integer, default=0)
    total_cost_usd = Column(Float, default=0.0)
    total_input_tokens = Column(Integer, default=0)
    total_output_tokens = Column(Integer, default=0)

    # Relationships
    employee = relationship("Employee", back_populates="sessions")

    __table_args__ = (
        Index("ix_sessions_user_email", "user_email"),
        Index("ix_sessions_started_at", "started_at"),
    )

    def __repr__(self):
        return f"<Session(session_id='{self.session_id}', user='{self.user_email}')>"


class ApiRequest(Base):
    __tablename__ = "api_requests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Text, ForeignKey("sessions.session_id"), nullable=False)
    user_email = Column(Text, ForeignKey("employees.email"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    model = Column(Text, nullable=False)
    input_tokens = Column(Integer, default=0)
    output_tokens = Column(Integer, default=0)
    cache_read_tokens = Column(Integer, default=0)
    cache_creation_tokens = Column(Integer, default=0)
    cost_usd = Column(Float, default=0.0)
    duration_ms = Column(Integer, default=0)

    __table_args__ = (
        Index("ix_api_requests_timestamp", "timestamp"),
        Index("ix_api_requests_model", "model"),
        Index("ix_api_requests_user_email", "user_email"),
        Index("ix_api_requests_session_id", "session_id"),
    )

    def __repr__(self):
        return f"<ApiRequest(model='{self.model}', cost={self.cost_usd:.4f})>"


class ToolEvent(Base):
    __tablename__ = "tool_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Text, ForeignKey("sessions.session_id"), nullable=False)
    user_email = Column(Text, ForeignKey("employees.email"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    event_type = Column(Text, nullable=False)  # 'decision' or 'result'
    tool_name = Column(Text, nullable=False)
    decision = Column(Text, nullable=True)       # accept/reject (for decisions)
    decision_source = Column(Text, nullable=True) # config/user_temporary/etc
    success = Column(Boolean, nullable=True)      # for results
    duration_ms = Column(Integer, nullable=True)   # for results

    __table_args__ = (
        Index("ix_tool_events_timestamp", "timestamp"),
        Index("ix_tool_events_tool_name", "tool_name"),
        Index("ix_tool_events_event_type", "event_type"),
        Index("ix_tool_events_session_id", "session_id"),
    )

    def __repr__(self):
        return f"<ToolEvent(type='{self.event_type}', tool='{self.tool_name}')>"


class UserPrompt(Base):
    __tablename__ = "user_prompts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Text, ForeignKey("sessions.session_id"), nullable=False)
    user_email = Column(Text, ForeignKey("employees.email"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    prompt_length = Column(Integer, default=0)

    __table_args__ = (
        Index("ix_user_prompts_timestamp", "timestamp"),
        Index("ix_user_prompts_session_id", "session_id"),
    )

    def __repr__(self):
        return f"<UserPrompt(length={self.prompt_length})>"


class ApiError(Base):
    __tablename__ = "api_errors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Text, ForeignKey("sessions.session_id"), nullable=False)
    user_email = Column(Text, ForeignKey("employees.email"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    model = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    status_code = Column(Text, nullable=True)
    attempt = Column(Integer, default=1)
    duration_ms = Column(Integer, default=0)

    __table_args__ = (
        Index("ix_api_errors_timestamp", "timestamp"),
        Index("ix_api_errors_status_code", "status_code"),
    )

    def __repr__(self):
        return f"<ApiError(error='{self.error[:30]}...', status={self.status_code})>"
