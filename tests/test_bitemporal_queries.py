# tests/test_bitemporal_queries.py
"""
Test bi-temporal query behavior.

Verifies that "as of time T" queries return correct historical data.
"""

from uuid import uuid4
from datetime import datetime, timezone, timedelta
from sqlalchemy import text

from app.graph.visibility import edge_visible_at, node_version_visible_at


class TestBitemporalQueries:
    """Tests for bi-temporal query behavior."""

    def test_bitemporal_queries_differ_by_ingest_time(self, session, sample_node_id):
        """Queries at different ingest times return different results."""
        # Create destination node
        dst_id = uuid4()
        session.execute(
            text("""
                INSERT INTO node (id, type, identifier)
                VALUES (:id, 'DISRUPTION', 'GROUND_STOP')
            """),
            {"id": dst_id}
        )
        session.commit()

        now = datetime.now(timezone.utc)
        past = now - timedelta(hours=1)
        future = now + timedelta(hours=1)

        # Create edge with specific ingest time
        edge_id = uuid4()
        session.execute(
            text("""
                INSERT INTO edge (id, src, dst, type, status, source_system, ingested_at, event_time_start)
                VALUES (:id, :src, :dst, 'HAS_DISRUPTION', 'DRAFT', 'TEST', :ingested_at, :event_time)
            """),
            {
                "id": edge_id,
                "src": sample_node_id,
                "dst": dst_id,
                "ingested_at": now,
                "event_time": now,
            }
        )
        session.commit()

        # Query at time before ingest - should NOT see edge
        visibility = edge_visible_at(now, past)
        result = session.execute(
            text(f"""
                SELECT id FROM edge
                WHERE src = :src AND {visibility}
            """),
            {"src": sample_node_id, "at_event_time": now, "at_ingest_time": past}
        )
        assert result.fetchone() is None

        # Query at time after ingest - should see edge
        visibility = edge_visible_at(now, future)
        result = session.execute(
            text(f"""
                SELECT id FROM edge
                WHERE src = :src AND {visibility}
            """),
            {"src": sample_node_id, "at_event_time": now, "at_ingest_time": future}
        )
        assert result.fetchone() is not None

    def test_visibility_predicate_consistency(self, session):
        """All queries use canonical visibility predicate."""
        # The visibility predicate should be consistent
        now = datetime.now(timezone.utc)

        edge_pred = edge_visible_at(now, now)
        node_pred = node_version_visible_at(now)

        # Both predicates should contain essential temporal checks
        assert "event_time_start" in edge_pred
        assert "event_time_end" in edge_pred
        assert "ingested_at" in edge_pred
        assert "valid_from" in edge_pred or "valid_to" in edge_pred

        assert "valid_from" in node_pred
        assert "valid_to" in node_pred

    def test_event_time_window(self, session, sample_node_id):
        """Events outside their event_time window are not visible."""
        dst_id = uuid4()
        session.execute(
            text("""
                INSERT INTO node (id, type, identifier)
                VALUES (:id, 'DISRUPTION', 'TIME_WINDOW_TEST')
            """),
            {"id": dst_id}
        )
        session.commit()

        now = datetime.now(timezone.utc)
        event_start = now - timedelta(hours=2)
        event_end = now - timedelta(hours=1)  # Event ended 1 hour ago

        # Create edge with event window in the past
        edge_id = uuid4()
        session.execute(
            text("""
                INSERT INTO edge (id, src, dst, type, status, source_system,
                                  ingested_at, event_time_start, event_time_end)
                VALUES (:id, :src, :dst, 'HAD_DISRUPTION', 'DRAFT', 'TEST',
                        :ingested_at, :event_start, :event_end)
            """),
            {
                "id": edge_id,
                "src": sample_node_id,
                "dst": dst_id,
                "ingested_at": now - timedelta(hours=2),
                "event_start": event_start,
                "event_end": event_end,
            }
        )
        session.commit()

        # Query at current time - should NOT see edge (event ended)
        visibility = edge_visible_at(now, now)
        result = session.execute(
            text(f"""
                SELECT id FROM edge
                WHERE id = :id AND {visibility}
            """),
            {"id": edge_id, "at_event_time": now, "at_ingest_time": now}
        )
        assert result.fetchone() is None

        # Query at time during event - should see edge
        during_event = event_start + timedelta(minutes=30)
        visibility = edge_visible_at(during_event, now)
        result = session.execute(
            text(f"""
                SELECT id FROM edge
                WHERE id = :id AND {visibility}
            """),
            {"id": edge_id, "at_event_time": during_event, "at_ingest_time": now}
        )
        assert result.fetchone() is not None


class TestNodeVersioning:
    """Tests for node version history."""

    def test_node_version_history(self, session):
        """Node versions create proper history."""
        node_id = uuid4()

        # Create node
        session.execute(
            text("""
                INSERT INTO node (id, type, identifier)
                VALUES (:id, 'AIRPORT', 'VERSION_TEST')
            """),
            {"id": node_id}
        )
        session.commit()

        now = datetime.now(timezone.utc)
        past = now - timedelta(hours=1)

        # Create first version
        v1_id = uuid4()
        session.execute(
            text("""
                INSERT INTO node_version (id, node_id, attrs, valid_from)
                VALUES (:id, :node_id, :attrs, :valid_from)
            """),
            {"id": v1_id, "node_id": node_id, "attrs": {"status": "OPEN"}, "valid_from": past}
        )
        session.commit()

        # Create second version (superseding first)
        v2_id = uuid4()
        session.execute(
            text("""
                UPDATE node_version SET valid_to = :valid_to WHERE id = :id
            """),
            {"id": v1_id, "valid_to": now}
        )
        session.execute(
            text("""
                INSERT INTO node_version (id, node_id, attrs, valid_from, supersedes_id)
                VALUES (:id, :node_id, :attrs, :valid_from, :supersedes)
            """),
            {
                "id": v2_id,
                "node_id": node_id,
                "attrs": {"status": "CLOSED"},
                "valid_from": now,
                "supersedes": v1_id,
            }
        )
        session.commit()

        # Query at past time - should get v1 attrs
        result = session.execute(
            text("""
                SELECT attrs FROM node_version
                WHERE node_id = :node_id
                  AND valid_from <= :at_time
                  AND (valid_to IS NULL OR valid_to > :at_time)
            """),
            {"node_id": node_id, "at_time": past + timedelta(minutes=30)}
        )
        row = result.fetchone()
        assert row[0]["status"] == "OPEN"

        # Query at current time - should get v2 attrs
        result = session.execute(
            text("""
                SELECT attrs FROM node_version
                WHERE node_id = :node_id
                  AND valid_from <= :at_time
                  AND (valid_to IS NULL OR valid_to > :at_time)
            """),
            {"node_id": node_id, "at_time": now + timedelta(minutes=1)}
        )
        row = result.fetchone()
        assert row[0]["status"] == "CLOSED"
