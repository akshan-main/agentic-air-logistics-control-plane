# app/agents/roles/comms.py
"""
Communications Agent - drafts notifications and messages.

Responsible for:
- Drafting customer notifications
- Creating internal alerts
- Formatting decision summaries

Note: Uses templates, not LLM - actual notification delivery
requires user integration (email, Slack webhooks, etc.)
"""

import json
from typing import List, Dict, Any, Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..state_graph import BeliefState, Posture
from ...logging import get_logger
from ...db.engine import get_next_trace_seq

logger = get_logger(__name__)


class CommsAgent:
    """
    Drafts communications for proposed actions.

    Creates structured messages for:
    - Customer notifications
    - Internal operations alerts
    - Escalation messages
    """

    def __init__(self, case_id: UUID, session: Session):
        self.case_id = case_id
        self.session = session

    def draft_communications(
        self,
        belief_state: BeliefState,
        proposed_actions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Draft communications for proposed actions.

        Args:
            belief_state: Current belief state
            proposed_actions: Actions requiring notifications

        Returns:
            List of drafted communications
        """
        logger.info(
            "drafting_communications",
            case_id=str(self.case_id),
            action_count=len(proposed_actions),
            posture=belief_state.current_posture.value if belief_state.current_posture else None,
        )

        communications = []

        for action in proposed_actions:
            if action.get("requires_notification"):
                comm = self._draft_for_action(action, belief_state)
                if comm:
                    communications.append(comm)

        # Add escalation message if needed
        if belief_state.current_posture == Posture.ESCALATE:
            communications.append(self._draft_escalation(belief_state))

        # Log communications drafted
        self._log_communications(communications)

        logger.info(
            "communications_drafted",
            case_id=str(self.case_id),
            comm_count=len(communications),
            types=[c.get("type") for c in communications],
        )

        return communications

    def _draft_for_action(
        self,
        action: Dict[str, Any],
        belief_state: BeliefState,
    ) -> Optional[Dict[str, Any]]:
        """Draft communication for a single action."""
        action_type = action.get("type", "")
        args = action.get("args", {})

        if action_type == "NOTIFY_CUSTOMER":
            return self._draft_customer_notification(args, belief_state)
        elif action_type == "PUBLISH_GATEWAY_ADVISORY":
            return self._draft_gateway_advisory(args, belief_state)
        elif action_type == "ESCALATE_OPS":
            return self._draft_ops_escalation(args, belief_state)

        return None

    def _draft_customer_notification(
        self,
        args: Dict[str, Any],
        belief_state: BeliefState,
    ) -> Dict[str, Any]:
        """Draft customer notification."""
        posture = belief_state.current_posture
        disruption_summary = self._get_disruption_summary(belief_state)

        if posture == Posture.HOLD:
            subject = "Important: Shipment Hold Notice"
            body = (
                f"Your shipment may be affected by current operational conditions.\n\n"
                f"Situation: {disruption_summary}\n\n"
                f"Status: We are holding your shipment pending further evaluation.\n\n"
                f"Next Steps: We will notify you within 2 hours with an update."
            )
        elif posture == Posture.RESTRICT:
            subject = "Service Update: Temporary Restrictions"
            body = (
                f"We are currently experiencing operational constraints.\n\n"
                f"Situation: {disruption_summary}\n\n"
                f"Impact: Some service options may be temporarily limited.\n\n"
                f"We appreciate your patience."
            )
        else:
            subject = "Service Update"
            body = f"Current situation: {disruption_summary}"

        return {
            "type": "CUSTOMER_NOTIFICATION",
            "channel": "email",
            "subject": subject,
            "body": body,
            "priority": "HIGH" if posture in (Posture.HOLD, Posture.ESCALATE) else "NORMAL",
            "drafted_at": datetime.now(timezone.utc).isoformat(),
        }

    def _draft_gateway_advisory(
        self,
        args: Dict[str, Any],
        belief_state: BeliefState,
    ) -> Dict[str, Any]:
        """Draft gateway advisory for downstream systems."""
        posture = belief_state.current_posture
        airport = args.get("airport", "UNKNOWN")

        return {
            "type": "GATEWAY_ADVISORY",
            "channel": "internal_api",
            "airport": airport,
            "posture": posture.value,
            "effective_at": datetime.now(timezone.utc).isoformat(),
            "reason": self._get_disruption_summary(belief_state),
            "evidence_count": belief_state.evidence_count,
            "confidence": self._calculate_confidence(belief_state),
        }

    def _draft_ops_escalation(
        self,
        args: Dict[str, Any],
        belief_state: BeliefState,
    ) -> Dict[str, Any]:
        """Draft operations escalation."""
        return {
            "type": "OPS_ESCALATION",
            "channel": "slack",
            "priority": "URGENT",
            "subject": f"Escalation Required: {args.get('reason', 'Manual review needed')}",
            "body": (
                f"Automated system requests human review.\n\n"
                f"Situation: {self._get_disruption_summary(belief_state)}\n\n"
                f"Uncertainties: {belief_state.uncertainty_count}\n"
                f"Contradictions: {belief_state.contradiction_count}\n\n"
                f"Please review and approve/reject proposed actions."
            ),
            "escalation_reason": args.get("reason"),
            "drafted_at": datetime.now(timezone.utc).isoformat(),
        }

    def _draft_escalation(self, belief_state: BeliefState) -> Dict[str, Any]:
        """Draft escalation message for ESCALATE posture."""
        return {
            "type": "ESCALATION",
            "channel": "slack",
            "priority": "URGENT",
            "subject": "Gateway Posture Escalation Required",
            "body": (
                f"System has determined ESCALATE posture is required.\n\n"
                f"Disruption: {self._get_disruption_summary(belief_state)}\n\n"
                f"Evidence Sources: {belief_state.evidence_count}\n"
                f"Open Contradictions: {belief_state.contradiction_count}\n\n"
                f"Manual intervention required."
            ),
            "drafted_at": datetime.now(timezone.utc).isoformat(),
        }

    def _get_disruption_summary(self, belief_state: BeliefState) -> str:
        """Get summary of disruptions from hypotheses."""
        if not belief_state.hypotheses:
            return "No specific disruption identified"

        # Take top hypothesis
        top = max(belief_state.hypotheses, key=lambda h: h.confidence)
        return top.text

    def _calculate_confidence(self, belief_state: BeliefState) -> float:
        """Calculate overall confidence."""
        if not belief_state.hypotheses:
            return 0.5

        # Average confidence of hypotheses
        total = sum(h.confidence for h in belief_state.hypotheses)
        return total / len(belief_state.hypotheses)

    def _log_communications(self, communications: List[Dict[str, Any]]):
        """Log drafted communications to trace."""
        for comm in communications:
            seq = get_next_trace_seq(self.case_id, self.session)
            self.session.execute(
                text("""
                    INSERT INTO trace_event
                    (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                    VALUES (:id, :case_id, :seq, 'TOOL_RESULT', 'communication', NULL, CAST(:meta AS jsonb), :created_at)
                """),
                {
                    "id": uuid4(),
                    "case_id": self.case_id,
                    "seq": seq,
                    "meta": json.dumps({
                        "type": comm.get("type"),
                        "channel": comm.get("channel"),
                        "subject": comm.get("subject"),
                        "priority": comm.get("priority"),
                    }),
                    "created_at": datetime.now(timezone.utc),
                }
            )
        self.session.commit()
