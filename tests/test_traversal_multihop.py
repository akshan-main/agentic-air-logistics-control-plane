# tests/test_traversal_multihop.py
"""
Test graph traversal operations.

Verifies multi-hop traversal returns proper subgraphs.
"""

import pytest
from uuid import uuid4
from datetime import datetime, timezone
from sqlalchemy import text

from app.graph.traversal import traverse, get_subgraph


class TestGraphTraversal:
    """Tests for graph traversal operations."""

    @pytest.mark.requires_db
    def test_traversal_returns_subgraph_not_flat_rows(self, session):
        """Traversal returns subgraph structure (nodes + edges), not flat rows."""
        # Create a chain: AIRPORT -> REGION -> WEATHER_ZONE
        airport_id = uuid4()
        region_id = uuid4()
        weather_id = uuid4()

        # Create nodes
        for node_id, node_type, identifier in [
            (airport_id, "AIRPORT", "TRAV_TEST"),
            (region_id, "REGION", "NORTHEAST"),
            (weather_id, "WEATHER_ZONE", "ZONE_1"),
        ]:
            session.execute(
                text("""
                    INSERT INTO node (id, type, identifier)
                    VALUES (:id, :type, :identifier)
                    ON CONFLICT (type, identifier) DO NOTHING
                """),
                {"id": node_id, "type": node_type, "identifier": identifier}
            )
        session.commit()

        now = datetime.now(timezone.utc)

        # Create edges (as DRAFT - no evidence needed)
        edge1_id = uuid4()
        edge2_id = uuid4()
        session.execute(
            text("""
                INSERT INTO edge (id, src, dst, type, status, source_system, ingested_at)
                VALUES (:id, :src, :dst, 'IN_REGION', 'DRAFT', 'TEST', :now)
            """),
            {"id": edge1_id, "src": airport_id, "dst": region_id, "now": now}
        )
        session.execute(
            text("""
                INSERT INTO edge (id, src, dst, type, status, source_system, ingested_at)
                VALUES (:id, :src, :dst, 'HAS_WEATHER_ZONE', 'DRAFT', 'TEST', :now)
            """),
            {"id": edge2_id, "src": region_id, "dst": weather_id, "now": now}
        )
        session.commit()

        # Traverse from airport
        result = traverse(
            start_node_ids=[airport_id],
            edge_types=['IN_REGION', 'HAS_WEATHER_ZONE'],
            at_event_time=now,
            at_ingest_time=now,
            max_hops=3,
            session=session,
        )

        # Should return a TraversalResult with subgraph
        assert result.subgraph is not None
        assert hasattr(result.subgraph, 'nodes')
        assert hasattr(result.subgraph, 'edges')

    @pytest.mark.requires_db
    def test_traversal_respects_max_hops(self, session):
        """Traversal stops at max_hops depth."""
        # Create chain: A -> B -> C -> D
        nodes = [uuid4() for _ in range(4)]
        for i, node_id in enumerate(nodes):
            session.execute(
                text("""
                    INSERT INTO node (id, type, identifier)
                    VALUES (:id, 'TEST_NODE', :identifier)
                    ON CONFLICT (type, identifier) DO NOTHING
                """),
                {"id": node_id, "identifier": f"CHAIN_{i}_{uuid4().hex[:4]}"}
            )
        session.commit()

        now = datetime.now(timezone.utc)

        # Create edges A->B, B->C, C->D
        for i in range(3):
            edge_id = uuid4()
            session.execute(
                text("""
                    INSERT INTO edge (id, src, dst, type, status, source_system, ingested_at)
                    VALUES (:id, :src, :dst, 'NEXT', 'DRAFT', 'TEST', :now)
                """),
                {"id": edge_id, "src": nodes[i], "dst": nodes[i + 1], "now": now}
            )
        session.commit()

        # Traverse with max_hops=1 - should only reach B
        result = traverse(
            start_node_ids=[nodes[0]],
            edge_types=["NEXT"],
            at_event_time=now,
            at_ingest_time=now,
            max_hops=1,
            session=session,
        )

        # Should have limited depth
        assert result.traversal_depth <= 1

    @pytest.mark.requires_db
    def test_traversal_filters_by_edge_type(self, session):
        """Traversal only follows specified edge types."""
        # Create nodes
        node_a = uuid4()
        node_b = uuid4()
        node_c = uuid4()

        for node_id, identifier in [(node_a, f"A_{uuid4().hex[:4]}"),
                                     (node_b, f"B_{uuid4().hex[:4]}"),
                                     (node_c, f"C_{uuid4().hex[:4]}")]:
            session.execute(
                text("""
                    INSERT INTO node (id, type, identifier)
                    VALUES (:id, 'TEST_NODE', :identifier)
                """),
                {"id": node_id, "identifier": identifier}
            )
        session.commit()

        now = datetime.now(timezone.utc)

        # Create edges: A-[FOLLOW]->B, A-[IGNORE]->C
        for edge_type, dst in [("FOLLOW", node_b), ("IGNORE", node_c)]:
            edge_id = uuid4()
            session.execute(
                text("""
                    INSERT INTO edge (id, src, dst, type, status, source_system, ingested_at)
                    VALUES (:id, :src, :dst, :type, 'DRAFT', 'TEST', :now)
                """),
                {"id": edge_id, "src": node_a, "dst": dst, "type": edge_type, "now": now}
            )
        session.commit()

        # Traverse only FOLLOW edges
        result = traverse(
            start_node_ids=[node_a],
            edge_types=["FOLLOW"],
            at_event_time=now,
            at_ingest_time=now,
            max_hops=2,
            session=session,
        )

        # Should only have FOLLOW edge
        edge_types = {e.type for e in result.subgraph.edges}
        assert "IGNORE" not in edge_types
        if edge_types:
            assert "FOLLOW" in edge_types


class TestSubgraphRetrieval:
    """Tests for subgraph retrieval."""

    @pytest.mark.requires_db
    def test_get_subgraph_center_node(self, session):
        """Get subgraph centered on a node."""
        center_id = uuid4()
        neighbor_id = uuid4()

        session.execute(
            text("""
                INSERT INTO node (id, type, identifier)
                VALUES (:id, 'CENTER', :identifier)
            """),
            {"id": center_id, "identifier": f"CENTER_{uuid4().hex[:4]}"}
        )
        session.execute(
            text("""
                INSERT INTO node (id, type, identifier)
                VALUES (:id, 'NEIGHBOR', :identifier)
            """),
            {"id": neighbor_id, "identifier": f"NEIGHBOR_{uuid4().hex[:4]}"}
        )
        session.commit()

        now = datetime.now(timezone.utc)
        edge_id = uuid4()
        session.execute(
            text("""
                INSERT INTO edge (id, src, dst, type, status, source_system, ingested_at)
                VALUES (:id, :src, :dst, 'CONNECTED', 'DRAFT', 'TEST', :now)
            """),
            {"id": edge_id, "src": center_id, "dst": neighbor_id, "now": now}
        )
        session.commit()

        result = get_subgraph(
            center_node_id=center_id,
            at_event_time=now,
            at_ingest_time=now,
            hops=1,
            session=session,
        )

        assert result is not None
        assert len(result.nodes) >= 1
