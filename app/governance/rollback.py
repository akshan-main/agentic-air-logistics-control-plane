# app/governance/rollback.py
"""
Rollback management for failed actions.
"""

import json
from typing import Optional, Tuple
from uuid import UUID, uuid4
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from .models import ActionState
from .state_machine import ActionStateMachine
from ..db.engine import get_next_trace_seq


# Actions that can be rolled back
ROLLBACKABLE_ACTIONS = {
    "SET_POSTURE",
    "PUBLISH_GATEWAY_ADVISORY",
    "UPDATE_BOOKING_RULES",
    "TRIGGER_REEVALUATION",
    "HOLD_CARGO",  # Can release
}


class RollbackManager:
    """
    Manages rollback of failed actions.

    Handles:
    - Determining if action can be rolled back
    - Executing rollback logic
    - Recording rollback outcome
    """

    def __init__(self, session: Session):
        self.session = session
        self.state_machine = ActionStateMachine(session)

    def can_rollback(self, action_id: UUID) -> Tuple[bool, Optional[str]]:
        """
        Check if action can be rolled back.

        Args:
            action_id: Action ID

        Returns:
            (can_rollback, reason)
        """
        result = self.session.execute(
            text("SELECT type, state FROM action WHERE id = :id"),
            {"id": action_id}
        )
        row = result.fetchone()

        if not row:
            return False, "Action not found"

        action_type, state = row

        # Check state
        if state != "FAILED":
            return False, f"Only FAILED actions can be rolled back (current: {state})"

        # Check action type
        if action_type not in ROLLBACKABLE_ACTIONS:
            return False, f"Action type {action_type} cannot be rolled back"

        return True, None

    def rollback(
        self,
        action_id: UUID,
        rolled_back_by: str,
        reason: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Execute rollback for an action.

        Args:
            action_id: Action ID
            rolled_back_by: Who is rolling back
            reason: Optional reason

        Returns:
            (success, error_message)
        """
        # Check if can rollback
        can_rb, error = self.can_rollback(action_id)
        if not can_rb:
            return False, error

        # Get action details
        result = self.session.execute(
            text("SELECT type, args, case_id FROM action WHERE id = :id"),
            {"id": action_id}
        )
        row = result.fetchone()
        action_type, args, case_id = row

        # Execute rollback logic
        try:
            rollback_result = self._execute_rollback(action_type, args)

            # Transition to ROLLED_BACK
            self.state_machine.transition(
                action_id,
                ActionState.ROLLED_BACK,
                reason=reason or "Rolled back after failure",
                actor=rolled_back_by,
            )

            # Record rollback outcome
            self._record_rollback_outcome(action_id, case_id, True, rollback_result)

            return True, None

        except Exception as e:
            # Record failed rollback
            self._record_rollback_outcome(action_id, case_id, False, {"error": str(e)})
            return False, f"Rollback failed: {e}"

    def _execute_rollback(
        self,
        action_type: str,
        args: dict,
    ) -> dict:
        """
        Execute rollback logic for action type.

        In production, this would call external systems.
        """
        if action_type == "SET_POSTURE":
            # Revert to previous posture (would need to track this)
            return {"rolled_back_posture": args.get("posture")}

        elif action_type == "PUBLISH_GATEWAY_ADVISORY":
            # Publish retraction advisory
            return {"advisory_retracted": True}

        elif action_type == "UPDATE_BOOKING_RULES":
            # Revert rules changes
            return {"rules_reverted": True}

        elif action_type == "HOLD_CARGO":
            # Release the held cargo
            return {"cargo_released": True}

        return {"rollback_executed": True}

    def _record_rollback_outcome(
        self,
        action_id: UUID,
        case_id: UUID,
        success: bool,
        result: dict,
    ):
        """Record rollback outcome to trace."""
        seq = get_next_trace_seq(case_id, self.session)
        self.session.execute(
            text("""
                INSERT INTO trace_event
                (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                VALUES (:id, :case_id, :seq, 'TOOL_RESULT', 'rollback', :action_id, CAST(:meta AS jsonb), :created_at)
            """),
            {
                "id": uuid4(),
                "case_id": case_id,
                "seq": seq,
                "action_id": str(action_id),
                "meta": json.dumps({"success": success, "result": result}),
                "created_at": datetime.now(timezone.utc),
            }
        )
        self.session.commit()


def can_rollback(action_id: UUID, session: Session) -> Tuple[bool, Optional[str]]:
    """Convenience function to check if action can be rolled back."""
    manager = RollbackManager(session)
    return manager.can_rollback(action_id)


def execute_rollback(
    action_id: UUID,
    rolled_back_by: str,
    session: Session,
    reason: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """Convenience function to execute rollback."""
    manager = RollbackManager(session)
    return manager.rollback(action_id, rolled_back_by, reason)
