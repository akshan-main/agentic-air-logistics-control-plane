# tests/conftest.py
"""
Pytest configuration and fixtures.

IMPORTANT: Tests should NOT load .env to avoid accidentally using production DB.
Use TEST_DATABASE_URL environment variable explicitly.
"""

import os
import pytest
from uuid import uuid4
from datetime import datetime, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

# Test database URL - NEVER fall back to DATABASE_URL (which may be production)
# Must be explicitly set via TEST_DATABASE_URL environment variable
TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://forwarder:forwarder@localhost:5432/aalcp_test"  # Default test DB
)

# Safety check: warn if TEST_DATABASE_URL looks like production
if "aalcp_test" not in TEST_DATABASE_URL and os.environ.get("ALLOW_PRODUCTION_DB_TESTS") != "true":
    import warnings
    warnings.warn(
        f"TEST_DATABASE_URL does not contain 'test' in name: {TEST_DATABASE_URL}. "
        f"Set ALLOW_PRODUCTION_DB_TESTS=true to override.",
        RuntimeWarning
    )


@pytest.fixture(scope="session")
def engine():
    """Create test database engine."""
    return create_engine(TEST_DATABASE_URL, echo=False)


@pytest.fixture(scope="session")
def setup_database(engine):
    """
    Set up test database schema.

    Checks if schema exists, skips migrations if already applied.
    """
    with engine.connect() as conn:
        # Check if schema already exists
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'node'
            )
        """))
        schema_exists = result.scalar()

        if schema_exists:
            # Schema already exists, skip migrations
            pass
        else:
            # Need to run migrations - but this should be done via setup.sh
            raise RuntimeError(
                "Database schema not found. Run migrations first:\n"
                "  make migrate\n"
                "Or run setup.sh which handles this automatically."
            )

    yield engine


@pytest.fixture
def session(setup_database, engine) -> Session:
    """
    Create a test session with rollback after each test.
    """
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    session.rollback()
    session.close()


@pytest.fixture
def clean_session(setup_database, engine) -> Session:
    """
    Create a clean session that commits changes.
    Use for tests that need to verify trigger behavior.
    """
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    session.close()


@pytest.fixture
def sample_node_id(session) -> str:
    """Create a sample node and return its ID."""
    node_id = uuid4()
    session.execute(
        text("""
            INSERT INTO node (id, type, identifier)
            VALUES (:id, 'AIRPORT', 'TEST')
            ON CONFLICT DO NOTHING
        """),
        {"id": node_id}
    )
    session.commit()
    return str(node_id)


@pytest.fixture
def sample_evidence_id(session) -> str:
    """Create sample evidence and return its ID."""
    evidence_id = uuid4()
    session.execute(
        text("""
            INSERT INTO evidence (id, source_system, source_ref, content_type, payload_sha256, raw_path)
            VALUES (:id, 'TEST', 'test-ref', 'application/json', :sha256, '/test/path')
        """),
        {
            "id": evidence_id,
            "sha256": "a" * 64,
        }
    )
    session.commit()
    return str(evidence_id)


@pytest.fixture
def sample_case_id(session) -> str:
    """Create sample case and return its ID."""
    case_id = uuid4()
    session.execute(
        text("""
            INSERT INTO "case" (id, case_type, scope, status)
            VALUES (:id, 'AIRPORT_DISRUPTION', :scope, 'OPEN')
        """),
        {
            "id": case_id,
            "scope": {"airport": "TEST"},
        }
    )
    session.commit()
    return str(case_id)


# ============================================================
# AUTO-MARKER FOR DATABASE TESTS
# ============================================================
# Automatically apply @pytest.mark.requires_db to tests that use
# database fixtures. This allows running "pytest -m 'not requires_db'"
# to skip all DB tests.

DB_FIXTURES = {"session", "clean_session", "sample_node_id", "sample_evidence_id", "sample_case_id", "engine", "setup_database"}


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests that use database fixtures."""
    requires_db_marker = pytest.mark.requires_db

    for item in items:
        # Check if test uses any DB fixture
        if hasattr(item, "fixturenames"):
            if any(fixture in DB_FIXTURES for fixture in item.fixturenames):
                # Add requires_db marker if not already present
                if not any(mark.name == "requires_db" for mark in item.iter_markers()):
                    item.add_marker(requires_db_marker)
