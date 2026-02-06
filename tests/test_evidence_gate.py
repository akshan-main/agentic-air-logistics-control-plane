# tests/test_evidence_gate.py
"""
Test evidence binding gates.

Database triggers enforce that FACT claims/edges must have evidence.
"""

import pytest
from uuid import uuid4
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError


class TestClaimEvidenceGate:
    """Tests for claim evidence binding trigger."""

    def test_claim_draft_insert_ok(self, session, sample_node_id):
        """INSERT claim as DRAFT succeeds without evidence."""
        claim_id = uuid4()

        # Should succeed - DRAFT doesn't need evidence
        session.execute(
            text("""
                INSERT INTO claim (id, text, subject_node_id, confidence, status)
                VALUES (:id, 'Test claim', :node_id, 0.8, 'DRAFT')
            """),
            {"id": claim_id, "node_id": sample_node_id}
        )
        session.commit()

        # Verify inserted
        result = session.execute(
            text("SELECT status FROM claim WHERE id = :id"),
            {"id": claim_id}
        )
        assert result.fetchone()[0] == "DRAFT"

    def test_claim_fact_promotion_needs_evidence(self, clean_session, sample_node_id):
        """UPDATE claim to FACT without claim_evidence fails."""
        claim_id = uuid4()

        # Insert as DRAFT
        clean_session.execute(
            text("""
                INSERT INTO claim (id, text, subject_node_id, confidence, status)
                VALUES (:id, 'Test claim', :node_id, 0.8, 'DRAFT')
            """),
            {"id": claim_id, "node_id": sample_node_id}
        )
        clean_session.commit()

        # Try to promote to FACT without evidence - should fail
        with pytest.raises(Exception) as exc_info:
            clean_session.execute(
                text("UPDATE claim SET status = 'FACT' WHERE id = :id"),
                {"id": claim_id}
            )
            clean_session.commit()

        assert "evidence" in str(exc_info.value).lower()

    def test_claim_fact_promotion_with_evidence(self, clean_session, sample_node_id, sample_evidence_id):
        """INSERT claim_evidence then UPDATE to FACT succeeds."""
        claim_id = uuid4()

        # Insert as DRAFT
        clean_session.execute(
            text("""
                INSERT INTO claim (id, text, subject_node_id, confidence, status)
                VALUES (:id, 'Test claim', :node_id, 0.8, 'DRAFT')
            """),
            {"id": claim_id, "node_id": sample_node_id}
        )
        clean_session.commit()

        # Add evidence binding
        clean_session.execute(
            text("""
                INSERT INTO claim_evidence (claim_id, evidence_id)
                VALUES (:claim_id, :evidence_id)
            """),
            {"claim_id": claim_id, "evidence_id": sample_evidence_id}
        )
        clean_session.commit()

        # Now promote to FACT - should succeed
        clean_session.execute(
            text("UPDATE claim SET status = 'FACT' WHERE id = :id"),
            {"id": claim_id}
        )
        clean_session.commit()

        # Verify promoted
        result = clean_session.execute(
            text("SELECT status FROM claim WHERE id = :id"),
            {"id": claim_id}
        )
        assert result.fetchone()[0] == "FACT"


class TestEdgeEvidenceGate:
    """Tests for edge evidence binding trigger."""

    def test_edge_draft_insert_ok(self, session, sample_node_id):
        """INSERT edge as DRAFT succeeds without evidence."""
        # Create another node for destination
        dst_id = uuid4()
        session.execute(
            text("""
                INSERT INTO node (id, type, identifier)
                VALUES (:id, 'AIRPORT', 'TEST2')
            """),
            {"id": dst_id}
        )
        session.commit()

        edge_id = uuid4()

        # Should succeed - DRAFT doesn't need evidence
        session.execute(
            text("""
                INSERT INTO edge (id, src, dst, type, status, source_system)
                VALUES (:id, :src, :dst, 'HAS_ROUTE', 'DRAFT', 'TEST')
            """),
            {"id": edge_id, "src": sample_node_id, "dst": dst_id}
        )
        session.commit()

        # Verify inserted
        result = session.execute(
            text("SELECT status FROM edge WHERE id = :id"),
            {"id": edge_id}
        )
        assert result.fetchone()[0] == "DRAFT"

    def test_edge_fact_promotion_needs_evidence(self, clean_session, sample_node_id):
        """UPDATE edge to FACT without edge_evidence fails."""
        # Create another node
        dst_id = uuid4()
        clean_session.execute(
            text("""
                INSERT INTO node (id, type, identifier)
                VALUES (:id, 'AIRPORT', 'TEST3')
            """),
            {"id": dst_id}
        )
        clean_session.commit()

        edge_id = uuid4()

        # Insert as DRAFT
        clean_session.execute(
            text("""
                INSERT INTO edge (id, src, dst, type, status, source_system)
                VALUES (:id, :src, :dst, 'HAS_DISRUPTION', 'DRAFT', 'TEST')
            """),
            {"id": edge_id, "src": sample_node_id, "dst": dst_id}
        )
        clean_session.commit()

        # Try to promote to FACT without evidence - should fail
        with pytest.raises(Exception) as exc_info:
            clean_session.execute(
                text("UPDATE edge SET status = 'FACT' WHERE id = :id"),
                {"id": edge_id}
            )
            clean_session.commit()

        assert "evidence" in str(exc_info.value).lower()


class TestNodeImmutability:
    """Tests for node immutability trigger."""

    def test_node_immutability(self, clean_session):
        """Node UPDATE blocked, must use node_version."""
        node_id = uuid4()

        # Insert node
        clean_session.execute(
            text("""
                INSERT INTO node (id, type, identifier)
                VALUES (:id, 'AIRPORT', 'IMMUT_TEST')
            """),
            {"id": node_id}
        )
        clean_session.commit()

        # Try to update node - should fail
        with pytest.raises(Exception) as exc_info:
            clean_session.execute(
                text("UPDATE node SET identifier = 'CHANGED' WHERE id = :id"),
                {"id": node_id}
            )
            clean_session.commit()

        assert "immutable" in str(exc_info.value).lower()


class TestAuditTrail:
    """Tests for audit trail with supersedes links."""

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
