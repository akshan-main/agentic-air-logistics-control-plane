# tests/conftest.py
"""
Pytest configuration and fixtures.

DB tests require a Postgres connection. Set TEST_DATABASE_URL explicitly,
or let conftest fall back to DATABASE_URL from .env. If neither is
reachable the tests are skipped (not failed).
"""

import os
import pytest
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

# Load .env so DATABASE_URL is available as a fallback
from dotenv import load_dotenv
load_dotenv()

# Prefer explicit TEST_DATABASE_URL; fall back to DATABASE_URL from .env.
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")

if not TEST_DATABASE_URL:
    TEST_DATABASE_URL = "postgresql://forwarder:forwarder@localhost:5432/aalcp_test"

# Safety check: warn if the URL looks like production (no 'test' in the DB name)
if "test" not in TEST_DATABASE_URL.split("?")[0].lower() and os.environ.get("ALLOW_PRODUCTION_DB_TESTS") != "true":
    import warnings
    warnings.warn(
        "TEST_DATABASE_URL does not contain 'test' in name. "
        "Set ALLOW_PRODUCTION_DB_TESTS=true to suppress this warning.",
        RuntimeWarning,
    )


def _try_connect(url: str):
    """Attempt a connection; return (engine, None) or (None, error_msg)."""
    try:
        eng = create_engine(url, echo=False, pool_pre_ping=True)
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng, None
    except Exception as exc:
        return None, str(exc)


@pytest.fixture(scope="session")
def engine():
    """Create test database engine. Skips the session if unreachable."""
    eng, err = _try_connect(TEST_DATABASE_URL)
    if eng is None:
        pytest.skip(f"Database not reachable ({err}). Set TEST_DATABASE_URL or DATABASE_URL.")
    return eng


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

        if not schema_exists:
            pytest.skip(
                "Database schema not found. Run migrations first: make migrate"
            )

    yield engine


@pytest.fixture
def session(setup_database, engine) -> Session:
    """
    Create a test session with rollback after each test.
    """
    SessionFactory = sessionmaker(bind=engine)
    session = SessionFactory()

    yield session

    session.rollback()
    session.close()


@pytest.fixture
def clean_session(setup_database, engine) -> Session:
    """
    Create a clean session that commits changes.
    Use for tests that need to verify trigger behavior.
    """
    SessionFactory = sessionmaker(bind=engine)
    session = SessionFactory()

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
