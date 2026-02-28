# tests/test_audit_trail.py
"""
Test audit trail with supersedes links.

Verifies that claims and edges maintain proper audit trail
through supersedes links for reconciliation.
"""

from uuid import uuid4
from sqlalchemy import text


class TestClaimSupersedes:
    """Tests for claim supersedes audit trail."""

    def test_claim_supersedes(self, session, sample_node_id, sample_evidence_id):
        """New claim with supersedes_claim_id correctly links to old claim."""
        # Create first claim
        claim_1_id = uuid4()
        session.execute(
            text("""
                INSERT INTO claim (id, text, subject_node_id, confidence, status)
                VALUES (:id, 'Original claim', :node_id, 0.8, 'DRAFT')
            """),
            {"id": claim_1_id, "node_id": sample_node_id}
        )
        session.commit()

        # Create superseding claim
        claim_2_id = uuid4()
        session.execute(
            text("""
                INSERT INTO claim (id, text, subject_node_id, confidence, status, supersedes_claim_id)
                VALUES (:id, 'Updated claim', :node_id, 0.9, 'DRAFT', :supersedes)
            """),
            {"id": claim_2_id, "node_id": sample_node_id, "supersedes": claim_1_id}
        )
        session.commit()

        # Verify link
        result = session.execute(
            text("SELECT supersedes_claim_id FROM claim WHERE id = :id"),
            {"id": claim_2_id}
        )
        assert str(result.fetchone()[0]) == str(claim_1_id)

    def test_claim_chain(self, session, sample_node_id):
        """Can create chain of superseding claims."""
        claim_ids = []

        # Create chain of 3 claims
        for i in range(3):
            claim_id = uuid4()
            supersedes = claim_ids[-1] if claim_ids else None

            session.execute(
                text("""
                    INSERT INTO claim (id, text, subject_node_id, confidence, status, supersedes_claim_id)
                    VALUES (:id, :text, :node_id, :confidence, 'DRAFT', :supersedes)
                """),
                {
                    "id": claim_id,
                    "text": f"Claim version {i + 1}",
                    "node_id": sample_node_id,
                    "confidence": 0.5 + (i * 0.1),
                    "supersedes": supersedes,
                }
            )
            claim_ids.append(claim_id)

        session.commit()

        # Verify chain
        for i, claim_id in enumerate(claim_ids):
            result = session.execute(
                text("SELECT supersedes_claim_id FROM claim WHERE id = :id"),
                {"id": claim_id}
            )
            row = result.fetchone()

            if i == 0:
                assert row[0] is None  # First claim has no predecessor
            else:
                assert str(row[0]) == str(claim_ids[i - 1])


class TestContradictionResolution:
    """Tests for contradiction resolution creating superseding claims."""

    def test_contradiction_resolution_creates_superseding_claim(self, session, sample_node_id):
        """Resolving contradiction creates new claim that supersedes both."""
        # Create two contradicting claims
        claim_a_id = uuid4()
        claim_b_id = uuid4()

        session.execute(
            text("""
                INSERT INTO claim (id, text, subject_node_id, confidence, status)
                VALUES (:id, 'Airport is open', :node_id, 0.7, 'DRAFT')
            """),
            {"id": claim_a_id, "node_id": sample_node_id}
        )
        session.execute(
            text("""
                INSERT INTO claim (id, text, subject_node_id, confidence, status)
                VALUES (:id, 'Airport is closed', :node_id, 0.8, 'DRAFT')
            """),
            {"id": claim_b_id, "node_id": sample_node_id}
        )
        session.commit()

        # Create resolution claim
        resolution_claim_id = uuid4()
        session.execute(
            text("""
                INSERT INTO claim (id, text, subject_node_id, confidence, status)
                VALUES (:id, 'Airport has partial closure', :node_id, 0.9, 'DRAFT')
            """),
            {"id": resolution_claim_id, "node_id": sample_node_id}
        )
        session.commit()

        # Create contradiction with resolution
        contradiction_id = uuid4()
        session.execute(
            text("""
                INSERT INTO contradiction (id, claim_a, claim_b, resolution_status, resolution_claim_id)
                VALUES (:id, :claim_a, :claim_b, 'RESOLVED', :resolution)
            """),
            {
                "id": contradiction_id,
                "claim_a": claim_a_id,
                "claim_b": claim_b_id,
                "resolution": resolution_claim_id,
            }
        )
        session.commit()

        # Verify
        result = session.execute(
            text("SELECT resolution_claim_id FROM contradiction WHERE id = :id"),
            {"id": contradiction_id}
        )
        assert str(result.fetchone()[0]) == str(resolution_claim_id)

    def test_contradiction_without_resolution(self, session, sample_node_id):
        """Contradiction can exist without resolution (OPEN status)."""
        claim_a_id = uuid4()
        claim_b_id = uuid4()

        for claim_id, text_val in [(claim_a_id, "Claim A"), (claim_b_id, "Claim B")]:
            session.execute(
                text("""
                    INSERT INTO claim (id, text, subject_node_id, confidence, status)
                    VALUES (:id, :text, :node_id, 0.7, 'DRAFT')
                """),
                {"id": claim_id, "text": text_val, "node_id": sample_node_id}
            )
        session.commit()

        # Create OPEN contradiction (no resolution yet)
        contradiction_id = uuid4()
        session.execute(
            text("""
                INSERT INTO contradiction (id, claim_a, claim_b, resolution_status)
                VALUES (:id, :claim_a, :claim_b, 'OPEN')
            """),
            {"id": contradiction_id, "claim_a": claim_a_id, "claim_b": claim_b_id}
        )
        session.commit()

        # Verify OPEN status
        result = session.execute(
            text("SELECT resolution_status, resolution_claim_id FROM contradiction WHERE id = :id"),
            {"id": contradiction_id}
        )
        row = result.fetchone()
        assert row[0] == "OPEN"
        assert row[1] is None


class TestEdgeSupersedes:
    """Tests for edge supersedes audit trail."""

    def test_edge_supersedes(self, session, sample_node_id):
        """New edge with supersedes_edge_id correctly links to old edge."""
        # Create destination node
        dst_id = uuid4()
        session.execute(
            text("""
                INSERT INTO node (id, type, identifier)
                VALUES (:id, 'DISRUPTION', 'SUPERSEDES_TEST')
            """),
            {"id": dst_id}
        )
        session.commit()

        # Create first edge
        edge_1_id = uuid4()
        session.execute(
            text("""
                INSERT INTO edge (id, src, dst, type, status, source_system, attrs)
                VALUES (:id, :src, :dst, 'HAS_DISRUPTION', 'DRAFT', 'TEST', :attrs)
            """),
            {
                "id": edge_1_id,
                "src": sample_node_id,
                "dst": dst_id,
                "attrs": {"severity": "LOW"},
            }
        )
        session.commit()

        # Create superseding edge with updated info
        edge_2_id = uuid4()
        session.execute(
            text("""
                INSERT INTO edge (id, src, dst, type, status, source_system, attrs, supersedes_edge_id)
                VALUES (:id, :src, :dst, 'HAS_DISRUPTION', 'DRAFT', 'TEST', :attrs, :supersedes)
            """),
            {
                "id": edge_2_id,
                "src": sample_node_id,
                "dst": dst_id,
                "attrs": {"severity": "HIGH"},  # Updated severity
                "supersedes": edge_1_id,
            }
        )
        session.commit()

        # Verify link
        result = session.execute(
            text("SELECT supersedes_edge_id FROM edge WHERE id = :id"),
            {"id": edge_2_id}
        )
        assert str(result.fetchone()[0]) == str(edge_1_id)
