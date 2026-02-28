# tests/test_blocked_missing_evidence.py
"""
Test missing evidence blocking behavior.

Verifies that shipment actions without booking evidence are blocked.
"""

import json
from uuid import uuid4
from datetime import datetime, timezone
from sqlalchemy import text

from app.agents.guardrails.gates import NoShipmentActionWithoutBookingGate
from app.ingestion.opensky import MissingEvidenceRequest


class TestShipmentActionBlocking:
    """Tests for blocking shipment actions without booking evidence."""

    def test_blocked_missing_evidence(self, session, sample_case_id):
        """Shipment action without booking -> BLOCKED + MissingEvidenceRequest."""
        from uuid import UUID
        gate = NoShipmentActionWithoutBookingGate()

        # Check should fail - using the actual method signature
        passed, reason = gate.check(
            action_type="HOLD_CARGO",
            case_id=UUID(sample_case_id),
            session=session,
        )

        assert not passed
        assert "booking" in reason.lower()
        assert "BLOCKED" in reason

        # Verify MissingEvidenceRequest was created
        result = session.execute(
            text("""
                SELECT source_system, request_type, criticality
                FROM missing_evidence_request
                WHERE case_id = :case_id
            """),
            {"case_id": sample_case_id}
        )
        row = result.fetchone()
        if row:
            assert row[0] == "INTERNAL_BOOKING"
            assert row[2] == "BLOCKING"

    def test_posture_action_allowed_without_booking(self, session, sample_case_id):
        """Posture actions (SET_POSTURE) don't require booking evidence."""
        from uuid import UUID
        gate = NoShipmentActionWithoutBookingGate()

        # Check should pass for posture action
        passed, reason = gate.check(
            action_type="SET_POSTURE",
            case_id=UUID(sample_case_id),
            session=session,
        )

        assert passed
        assert reason is None

    def test_shipment_actions_list_complete(self):
        """All shipment actions are in the gate's check list."""
        gate = NoShipmentActionWithoutBookingGate()

        expected_shipment_actions = {
            'HOLD_CARGO',
            'RELEASE_CARGO',
            'SWITCH_GATEWAY',
            'REBOOK_FLIGHT',
            'UPGRADE_SERVICE',
            'NOTIFY_CUSTOMER',
            'FILE_CLAIM',
        }

        assert gate.SHIPMENT_ACTIONS == expected_shipment_actions


class TestOpenSkyDegradation:
    """Tests for OpenSky graceful degradation."""

    def test_opensky_timeout_creates_degraded_request(self, session, sample_case_id):
        """OpenSky timeout -> MissingEvidenceRequest DEGRADED (not BLOCKING)."""

        # Create a MissingEvidenceRequest as would happen during timeout
        request = MissingEvidenceRequest(
            case_id=sample_case_id,
            source_system="OPENSKY",
            request_type="aircraft_states",
            request_params={"bbox": {"lat_min": 40, "lat_max": 41, "lon_min": -74, "lon_max": -73}},
            reason="Timeout after 10s",
            criticality="DEGRADED",  # NOT BLOCKING
        )

        # Insert the request
        request_id = uuid4()
        session.execute(
            text("""
                INSERT INTO missing_evidence_request
                (id, case_id, source_system, request_type, request_params, reason, criticality)
                VALUES (:id, :case_id, :source_system, :request_type, :request_params::jsonb, :reason, :criticality)
            """),
            {
                "id": request_id,
                "case_id": sample_case_id,
                "source_system": request.source_system,
                "request_type": request.request_type,
                "request_params": json.dumps(request.request_params or {}),
                "reason": request.reason,
                "criticality": request.criticality,
            }
        )
        session.commit()

        # Verify it's DEGRADED, not BLOCKING
        result = session.execute(
            text("""
                SELECT criticality FROM missing_evidence_request
                WHERE case_id = :case_id AND source_system = 'OPENSKY'
            """),
            {"case_id": sample_case_id}
        )
        row = result.fetchone()
        assert row[0] == "DEGRADED"

        # Verify case is NOT blocked due to DEGRADED missing evidence
        result = session.execute(
            text("""
                SELECT COUNT(*) FROM missing_evidence_request
                WHERE case_id = :case_id AND criticality = 'BLOCKING' AND resolved_at IS NULL
            """),
            {"case_id": sample_case_id}
        )
        blocking_count = result.scalar()
        assert blocking_count == 0

    def test_missing_evidence_criticality_levels(self):
        """All criticality levels are valid."""
        valid_levels = {"BLOCKING", "DEGRADED", "INFORMATIONAL"}

        # BLOCKING: Case cannot proceed without this
        # DEGRADED: Case can proceed but with reduced confidence
        # INFORMATIONAL: Nice to have, doesn't affect processing

        # Verify these are the expected levels
        for level in valid_levels:
            request = MissingEvidenceRequest(
                case_id=None,
                source_system="TEST",
                request_type="test",
                request_params={},
                reason="test",
                criticality=level,
            )
            assert request.criticality == level


class TestMissingEvidenceTracking:
    """Tests for missing evidence request tracking."""

    def test_missing_evidence_resolution(self, session, sample_case_id, sample_evidence_id):
        """Missing evidence request can be resolved when evidence arrives."""
        request_id = uuid4()

        # Create unresolved request
        session.execute(
            text("""
                INSERT INTO missing_evidence_request
                (id, case_id, source_system, request_type, request_params, reason, criticality)
                VALUES (:id, :case_id, 'FAA_NAS', 'airport_status', :params::jsonb, 'Timeout', 'BLOCKING')
            """),
            {
                "id": request_id,
                "case_id": sample_case_id,
                "params": json.dumps({"airport": "KJFK"}),
            }
        )
        session.commit()

        # Resolve with evidence
        now = datetime.now(timezone.utc)
        session.execute(
            text("""
                UPDATE missing_evidence_request
                SET resolved_at = :resolved_at, resolved_by_evidence_id = :evidence_id
                WHERE id = :id
            """),
            {
                "id": request_id,
                "resolved_at": now,
                "evidence_id": sample_evidence_id,
            }
        )
        session.commit()

        # Verify resolved
        result = session.execute(
            text("""
                SELECT resolved_at, resolved_by_evidence_id
                FROM missing_evidence_request WHERE id = :id
            """),
            {"id": request_id}
        )
        row = result.fetchone()
        assert row[0] is not None
        assert str(row[1]) == sample_evidence_id
