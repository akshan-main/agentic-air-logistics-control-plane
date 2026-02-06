# app/agents/guardrails/gates.py
"""
Guardrail gates - hard fail safety checks.

These gates enforce critical invariants:
1. EvidenceBindingGate: No FACT claim/edge without evidence
2. NoShipmentActionWithoutBookingGate: No shipment actions without booking
3. NonWorkflowGate: Verify non-workflow behavior
4. MissingEvidenceBlocker: Track and block on missing evidence
"""

import json
from typing import Tuple, Optional, List, Set
from uuid import UUID, uuid4
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from ...db.engine import SessionLocal
from ..state_graph import BeliefState


class EvidenceBindingGate:
    """
    Gate: No FACT claim without claim_evidence.

    Enforced at DB level via trigger, but also checked in code.
    """

    def check_claim(
        self,
        claim_id: UUID,
        status: str,
        session: Session,
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if claim can be promoted to FACT.

        Args:
            claim_id: Claim ID
            status: Target status
            session: Database session

        Returns:
            (pass, error_message)
        """
        if status != "FACT":
            return True, None

        # Check for evidence binding
        result = session.execute(
            text("SELECT COUNT(*) FROM claim_evidence WHERE claim_id = :id"),
            {"id": claim_id}
        )

        if result.scalar() == 0:
            return False, "Cannot promote claim to FACT without evidence binding"

        return True, None

    def check_edge(
        self,
        edge_id: UUID,
        status: str,
        session: Session,
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if edge can be promoted to FACT.

        Args:
            edge_id: Edge ID
            status: Target status
            session: Database session

        Returns:
            (pass, error_message)
        """
        if status != "FACT":
            return True, None

        result = session.execute(
            text("SELECT COUNT(*) FROM edge_evidence WHERE edge_id = :id"),
            {"id": edge_id}
        )

        if result.scalar() == 0:
            return False, "Cannot promote edge to FACT without evidence binding"

        return True, None


class NoShipmentActionWithoutBookingGate:
    """
    Gate: Block shipment actions without booking evidence.

    Shipment actions require proof that we have the booking.
    """

    SHIPMENT_ACTIONS: Set[str] = {
        'HOLD_CARGO', 'RELEASE_CARGO', 'SWITCH_GATEWAY',
        'REBOOK_FLIGHT', 'UPGRADE_SERVICE', 'NOTIFY_CUSTOMER', 'FILE_CLAIM'
    }

    def check(
        self,
        action_type: str,
        case_id: UUID,
        session: Session,
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if shipment action is allowed.

        Args:
            action_type: Type of action
            case_id: Case ID
            session: Database session

        Returns:
            (pass, error_message or MissingEvidenceRequest ID)
        """
        if action_type not in self.SHIPMENT_ACTIONS:
            return True, None

        # Check for booking evidence
        result = session.execute(
            text("""
                SELECT COUNT(*) FROM evidence e
                WHERE e.source_system = 'BOOKING'
                  AND e.id IN (
                      SELECT ref_id::uuid FROM trace_event
                      WHERE case_id = :case_id AND ref_type = 'evidence'
                  )
            """),
            {"case_id": case_id}
        )

        if result.scalar() == 0:
            # Create MissingEvidenceRequest
            mer_id = uuid4()
            session.execute(
                text("""
                    INSERT INTO missing_evidence_request
                    (id, case_id, source_system, request_type, request_params,
                     reason, criticality, created_at)
                    VALUES
                    (:id, :case_id, 'INTERNAL_BOOKING', 'booking_lookup',
                     CAST(:params AS jsonb), :reason, 'BLOCKING', :created_at)
                """),
                {
                    "id": mer_id,
                    "case_id": case_id,
                    "params": json.dumps({"action_type": action_type}),
                    "reason": f"Shipment action {action_type} requires booking evidence",
                    "created_at": datetime.now(timezone.utc),
                }
            )

            # Update case status to BLOCKED
            session.execute(
                text('UPDATE "case" SET status = \'BLOCKED\' WHERE id = :id'),
                {"id": case_id}
            )
            session.commit()

            return False, f"BLOCKED: {action_type} requires booking evidence (MER: {mer_id})"

        return True, None


class NonWorkflowGate:
    """
    Gate: Verify different cases produce different uncertainty resolution paths.

    Stronger than checking tool sequence - checks that the agent
    reasoned differently based on different evidence.
    """

    def verify_non_workflow(
        self,
        case_a: UUID,
        case_b: UUID,
        session: Session,
    ) -> bool:
        """
        Verify different cases have different uncertainty resolution paths.

        Args:
            case_a: First case ID
            case_b: Second case ID
            session: Database session

        Returns:
            True if cases have different resolution paths
        """
        trace_a = self._extract_uncertainty_resolutions(case_a, session)
        trace_b = self._extract_uncertainty_resolutions(case_b, session)

        # Different cases should resolve uncertainties differently
        return trace_a != trace_b

    def _extract_uncertainty_resolutions(
        self,
        case_id: UUID,
        session: Session,
    ) -> List[Tuple[str, str]]:
        """
        Extract sequence of (uncertainty_type, resolution_evidence_id).

        Args:
            case_id: Case ID
            session: Database session

        Returns:
            List of (uncertainty_type, evidence_id) tuples
        """
        result = session.execute(
            text("""
                SELECT
                    t.meta->>'uncertainty_type' as uncertainty_type,
                    t.meta->>'evidence_id' as evidence_id
                FROM trace_event t
                WHERE t.case_id = :case_id
                  AND t.event_type = 'TOOL_RESULT'
                  AND t.meta->>'uncertainty_resolved' = 'true'
                ORDER BY t.seq
            """),
            {"case_id": case_id}
        )

        return [(row[0], row[1]) for row in result if row[0]]


class MissingEvidenceBlocker:
    """
    Gate: Track and block on missing evidence.

    When critical evidence is missing (OpenSky timeout, etc.),
    emit MissingEvidenceRequest and block case if BLOCKING criticality.
    """

    def handle_missing_evidence(
        self,
        case_id: UUID,
        source: str,
        request_type: str,
        reason: str,
        criticality: str = "DEGRADED",
        session: Optional[Session] = None,
    ) -> UUID:
        """
        Record missing evidence and potentially block case.

        Args:
            case_id: Case ID
            source: Source system that failed
            request_type: Type of request that failed
            reason: Why it failed
            criticality: BLOCKING, DEGRADED, or INFORMATIONAL
            session: Optional database session

        Returns:
            MissingEvidenceRequest ID
        """
        if session is None:
            session = SessionLocal()
            owns_session = True
        else:
            owns_session = False

        try:
            mer_id = uuid4()

            session.execute(
                text("""
                    INSERT INTO missing_evidence_request
                    (id, case_id, source_system, request_type, request_params,
                     reason, criticality, created_at)
                    VALUES
                    (:id, :case_id, :source, :request_type, CAST(:params AS jsonb),
                     :reason, :criticality, :created_at)
                """),
                {
                    "id": mer_id,
                    "case_id": case_id,
                    "source": source,
                    "request_type": request_type,
                    "params": json.dumps({}),
                    "reason": reason,
                    "criticality": criticality,
                    "created_at": datetime.now(timezone.utc),
                }
            )

            # Block case if BLOCKING criticality
            if criticality == "BLOCKING":
                session.execute(
                    text('UPDATE "case" SET status = \'BLOCKED\' WHERE id = :id'),
                    {"id": case_id}
                )

            session.commit()
            return mer_id

        finally:
            if owns_session:
                session.close()

    def check_blocking(
        self,
        case_id: UUID,
        session: Session,
    ) -> bool:
        """
        Check if case has blocking missing evidence.

        Args:
            case_id: Case ID
            session: Database session

        Returns:
            True if there's unresolved blocking missing evidence
        """
        result = session.execute(
            text("""
                SELECT COUNT(*) FROM missing_evidence_request
                WHERE case_id = :case_id
                  AND criticality = 'BLOCKING'
                  AND resolved_at IS NULL
            """),
            {"case_id": case_id}
        )

        return result.scalar() > 0
