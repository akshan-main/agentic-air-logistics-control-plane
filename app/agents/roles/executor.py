# app/agents/roles/executor.py
"""
Executor Agent - executes approved actions.

Responsible for:
- Executing approved actions
- Tracking action outcomes
- Handling failures and rollbacks
- Firing webhooks on posture changes (the system DOES something)
"""

import json
from typing import List, Dict, Any, Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from ...logging import get_logger
from ...webhooks import WebhookExecutor

logger = get_logger(__name__)


class ExecutorAgent:
    """
    Executes approved actions and tracks outcomes.

    Actions go through state machine:
    PROPOSED -> PENDING_APPROVAL -> APPROVED -> EXECUTING -> COMPLETED/FAILED
    """

    def __init__(
        self,
        case_id: UUID,
        session: Session,
        webhook_executor: Optional[WebhookExecutor] = None,
    ):
        self.case_id = case_id
        self.session = session
        self.webhook_executor = webhook_executor or WebhookExecutor()
        self._previous_posture: Optional[str] = None
        self._belief_state_context: Dict[str, Any] = {}

    def set_context(
        self,
        confidence: Optional[float] = None,
        evidence_count: Optional[int] = None,
        risk_level: Optional[str] = None,
        previous_posture: Optional[str] = None,
    ):
        """
        Set context for webhook payloads.

        Called by orchestrator before execute() to provide
        belief state info for webhook notifications.
        """
        self._belief_state_context = {
            "confidence": confidence,
            "evidence_count": evidence_count,
            "risk_level": risk_level,
        }
        self._previous_posture = previous_posture

    def execute(self, proposed_actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute list of proposed actions.

        Args:
            proposed_actions: Actions to execute

        Returns:
            List of execution outcomes
        """
        outcomes = []

        for action_spec in proposed_actions:
            # Create action record
            action_id = self._create_action(action_spec)

            # Check if approval required
            if action_spec.get("requires_approval"):
                # Leave in PENDING_APPROVAL state
                self._update_action_state(action_id, "PENDING_APPROVAL")
                outcomes.append({
                    "action_id": action_id,
                    "status": "PENDING_APPROVAL",
                    "requires_approval": True,
                })
                continue

            # Execute action
            try:
                self._update_action_state(action_id, "EXECUTING")
                result = self._execute_action(action_spec)

                # Record success
                self._update_action_state(action_id, "COMPLETED")
                outcome_id = self._create_outcome(action_id, True, result)

                outcomes.append({
                    "action_id": action_id,
                    "outcome_id": outcome_id,
                    "status": "COMPLETED",
                    "success": True,
                    "result": result,
                })

            except Exception as e:
                # Record failure
                self._update_action_state(action_id, "FAILED")
                outcome_id = self._create_outcome(action_id, False, {"error": str(e)})

                outcomes.append({
                    "action_id": action_id,
                    "outcome_id": outcome_id,
                    "status": "FAILED",
                    "success": False,
                    "error": str(e),
                })

        return outcomes

    def _create_action(self, action_spec: Dict[str, Any]) -> UUID:
        """Create action record in database."""
        action_id = uuid4()

        self.session.execute(
            text("""
                INSERT INTO action
                (id, case_id, type, args, state, risk_level, requires_approval, created_at)
                VALUES
                (:id, :case_id, :type, CAST(:args AS jsonb), :state, :risk_level, :requires_approval, :created_at)
            """),
            {
                "id": action_id,
                "case_id": self.case_id,
                "type": action_spec.get("type"),
                "args": json.dumps(action_spec.get("args", {})),
                "state": "PROPOSED",
                "risk_level": action_spec.get("risk_level", "MEDIUM"),
                "requires_approval": action_spec.get("requires_approval", False),
                "created_at": datetime.now(timezone.utc),
            }
        )
        self.session.commit()
        return action_id

    def _update_action_state(self, action_id: UUID, state: str):
        """Update action state."""
        self.session.execute(
            text("UPDATE action SET state = :state WHERE id = :id"),
            {"id": action_id, "state": state}
        )
        self.session.commit()

    def _create_outcome(
        self,
        action_id: UUID,
        success: bool,
        payload: Dict[str, Any],
    ) -> UUID:
        """Create outcome record."""
        outcome_id = uuid4()

        self.session.execute(
            text("""
                INSERT INTO outcome (id, action_id, success, payload, created_at)
                VALUES (:id, :action_id, :success, CAST(:payload AS jsonb), :created_at)
            """),
            {
                "id": outcome_id,
                "action_id": action_id,
                "success": success,
                "payload": json.dumps(payload),
                "created_at": datetime.now(timezone.utc),
            }
        )
        self.session.commit()
        return outcome_id

    def _execute_action(self, action_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single action.

        In production, this would integrate with external systems.
        """
        action_type = action_spec.get("type")
        args = action_spec.get("args", {})

        # Posture actions
        if action_type == "SET_POSTURE":
            return self._execute_set_posture(args)

        # Advisory actions
        elif action_type == "PUBLISH_GATEWAY_ADVISORY":
            return self._execute_publish_advisory(args)

        elif action_type == "UPDATE_BOOKING_RULES":
            return self._execute_update_rules(args)

        elif action_type == "TRIGGER_REEVALUATION":
            return self._execute_trigger_reevaluation(args)

        elif action_type == "ESCALATE_OPS":
            return self._execute_escalate(args)

        # Shipment actions (require booking evidence - enforced by policy)
        elif action_type in ("HOLD_CARGO", "RELEASE_CARGO", "SWITCH_GATEWAY",
                            "REBOOK_FLIGHT", "UPGRADE_SERVICE", "NOTIFY_CUSTOMER", "FILE_CLAIM"):
            return self._execute_shipment_action(action_type, args)

        else:
            raise ValueError(f"Unknown action type: {action_type}")

    def _execute_set_posture(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute SET_POSTURE action.

        This is the core action that fires webhooks to external systems.
        When a posture change happens, registered webhooks receive
        HTTP POST notifications.
        """
        posture = args.get("posture")
        airport = args.get("airport")

        logger.info(
            "executing_set_posture",
            case_id=str(self.case_id),
            airport=airport,
            new_posture=posture,
            previous_posture=self._previous_posture,
        )

        # Fire webhooks for posture change
        # This is the "system DOES something" feature
        deliveries = self.webhook_executor.fire_posture_change(
            case_id=str(self.case_id),
            airport=airport,
            new_posture=posture,
            previous_posture=self._previous_posture,
            confidence=self._belief_state_context.get("confidence"),
            evidence_count=self._belief_state_context.get("evidence_count"),
            risk_level=self._belief_state_context.get("risk_level"),
        )

        # Log webhook delivery results
        successful_deliveries = [d for d in deliveries if d.success]
        failed_deliveries = [d for d in deliveries if not d.success]

        if deliveries:
            logger.info(
                "posture_webhooks_fired",
                case_id=str(self.case_id),
                airport=airport,
                posture=posture,
                total_webhooks=len(deliveries),
                successful=len(successful_deliveries),
                failed=len(failed_deliveries),
            )

        return {
            "executed": True,
            "posture": posture,
            "airport": airport,
            "effective_at": datetime.now(timezone.utc).isoformat(),
            "webhooks_fired": len(deliveries),
            "webhooks_succeeded": len(successful_deliveries),
            "webhooks_failed": len(failed_deliveries),
            "webhook_delivery_ids": [str(d.delivery_id) for d in successful_deliveries],
        }

    def _execute_publish_advisory(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute PUBLISH_GATEWAY_ADVISORY action."""
        # In production, would publish to message queue
        return {
            "executed": True,
            "advisory_published": True,
            "channel": "gateway_advisory_queue",
        }

    def _execute_update_rules(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute UPDATE_BOOKING_RULES action."""
        # In production, would update rules engine
        return {
            "executed": True,
            "rules_updated": True,
        }

    def _execute_trigger_reevaluation(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute TRIGGER_REEVALUATION action."""
        # In production, would queue reevaluation job
        return {
            "executed": True,
            "reevaluation_queued": True,
        }

    def _execute_escalate(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute ESCALATE_OPS action."""
        # In production, would send to escalation system
        return {
            "executed": True,
            "escalation_sent": True,
            "channel": "ops_escalation",
        }

    def _execute_shipment_action(
        self,
        action_type: str,
        args: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute shipment-level action."""
        # In production, would integrate with TMS/WMS
        return {
            "executed": True,
            "action_type": action_type,
            "shipment_id": args.get("shipment_id"),
        }

    def execute_approved_action(self, action_id: UUID) -> Dict[str, Any]:
        """
        Execute a single action that has been approved.

        Called after manual approval via API endpoint.

        Args:
            action_id: UUID of approved action

        Returns:
            Execution outcome
        """
        # Get action details
        result = self.session.execute(
            text("""
                SELECT type, args, state, case_id
                FROM action WHERE id = :id
            """),
            {"id": action_id}
        )
        row = result.fetchone()
        if not row:
            raise ValueError(f"Action not found: {action_id}")

        action_type = row[0]
        args = row[1] if isinstance(row[1], dict) else {}
        state = row[2]

        if state != "APPROVED":
            raise ValueError(f"Action is not approved (state: {state})")

        action_spec = {
            "type": action_type,
            "args": args,
        }

        # Execute the action
        try:
            self._update_action_state(action_id, "EXECUTING")
            result = self._execute_action(action_spec)

            self._update_action_state(action_id, "COMPLETED")
            outcome_id = self._create_outcome(action_id, True, result)

            return {
                "action_id": str(action_id),
                "outcome_id": str(outcome_id),
                "status": "COMPLETED",
                "success": True,
                "result": result,
            }

        except Exception as e:
            self._update_action_state(action_id, "FAILED")
            outcome_id = self._create_outcome(action_id, False, {"error": str(e)})

            return {
                "action_id": str(action_id),
                "outcome_id": str(outcome_id),
                "status": "FAILED",
                "success": False,
                "error": str(e),
            }
