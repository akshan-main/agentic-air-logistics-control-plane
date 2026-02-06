# app/db/engine.py
"""
Database engine and session management.

Connects to PostgreSQL with pgvector support.
"""

import os
from contextlib import contextmanager
from typing import Generator

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session, declarative_base

# Database URL from environment or default
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://forwarder:forwarder@localhost:5432/postgres"
)

# Create engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Check connection health
    pool_size=10,
    max_overflow=20,
)

# Session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

# Base class for ORM models
Base = declarative_base()


def get_engine():
    """Get SQLAlchemy engine."""
    return engine


def get_session() -> Generator[Session, None, None]:
    """
    Get database session.

    Yields a session that auto-closes on context exit.
    For use with FastAPI Depends or as context manager.
    """
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.

    Usage:
        with session_scope() as session:
            session.execute(...)
            session.commit()
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_connection() -> bool:
    """
    Check database connection.

    Returns:
        True if connection successful
    """
    try:
        with session_scope() as session:
            session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def check_pgvector_version() -> str:
    """
    Get pgvector extension version.

    Returns:
        Version string or "NOT_INSTALLED"
    """
    try:
        with session_scope() as session:
            result = session.execute(
                text("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
            )
            row = result.fetchone()
            return row[0] if row else "NOT_INSTALLED"
    except Exception:
        return "NOT_INSTALLED"


def get_next_trace_seq(case_id, session: Session) -> int:
    """
    Get the next trace event sequence number for a case.

    CENTRALIZED SEQ GENERATION - All trace event writes should use this
    to ensure consistent ordering for replay and auditing.

    FIXED: Uses advisory lock to prevent race conditions where two concurrent
    writers could get the same sequence number. The lock is held until
    transaction commit.

    Args:
        case_id: Case UUID
        session: Database session

    Returns:
        Next sequence number (max + 1, or 1 if no events exist)
    """
    # Use advisory lock on case_id hash to prevent concurrent seq generation
    # pg_advisory_xact_lock is released automatically at transaction end
    # We hash the case_id to get a bigint for the lock
    session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:case_id_str))"),
        {"case_id_str": str(case_id)}
    )

    result = session.execute(
        text("""
            SELECT COALESCE(MAX(seq), 0) + 1 FROM trace_event
            WHERE case_id = :case_id
        """),
        {"case_id": case_id}
    )
    return result.scalar()
