# app/governance/state_machine.py
"""
Action state machine for governance.

States:
PROPOSED -> PENDING_APPROVAL (if requires_approval)
PROPOSED -> APPROVED (if not requires_approval)
PENDING_APPROVAL -> APPROVED
APPROVED -> EXECUTING
EXECUTING -> COMPLETED | FAILED
FAILED -> ROLLED_BACK
"""

import json
from typing import List, Optional, Set, Tuple
from uuid import UUID
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from .models import ActionState
from ..db.engine import get_next_trace_seq


# Valid state transitions
TRANSITIONS: dict[ActionState, Set[ActionState]] = {
    ActionState.PROPOSED: {ActionState.PENDING_APPROVAL, ActionState.APPROVED},
    ActionState.PENDING_APPROVAL: {ActionState.APPROVED, ActionState.PROPOSED},  # Can reject back to PROPOSED
    ActionState.APPROVED: {ActionState.EXECUTING},
    ActionState.EXECUTING: {ActionState.COMPLETED, ActionState.FAILED},
    ActionState.FAILED: {ActionState.ROLLED_BACK},
    ActionState.COMPLETED: set(),  # Terminal
    ActionState.ROLLED_BACK: set(),  # Terminal
}


def get_valid_transitions(current_state: ActionState) -> Set[ActionState]:
    """Get valid transitions from current state."""
    return TRANSITIONS.get(current_state, set())


class ActionStateMachine:
    """
    State machine for action governance.

    Enforces valid state transitions and tracks history.
    """

    def __init__(self, session: Session):
        self.session = session

    def transition(
        self,
        action_id: UUID,
        to_state: ActionState,
        reason: Optional[str] = None,
        actor: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Transition action to new state.

        Args:
            action_id: Action ID
            to_state: Target state
            reason: Optional reason for transition
            actor: Who triggered the transition

        Returns:
            (success, error_message)
        """
        # Get current state
        result = self.session.execute(
            text("SELECT state FROM action WHERE id = :id"),
            {"id": action_id}
        )
        row = result.fetchone()
        if not row:
            return False, f"Action not found: {action_id}"

        current_state = ActionState(row[0])

        # Check if transition is valid
        valid_transitions = get_valid_transitions(current_state)
        if to_state not in valid_transitions:
            return False, (
                f"Invalid transition: {current_state.value} -> {to_state.value}. "
                f"Valid transitions: {[s.value for s in valid_transitions]}"
            )

        # Execute transition
        from datetime import datetime, timezone

        if to_state == ActionState.APPROVED:
            self.session.execute(
                text("""
                    UPDATE action
                    SET state = :state, approved_by = :actor, approved_at = :now
                    WHERE id = :id
                """),
                {
                    "id": action_id,
                    "state": to_state.value,
                    "actor": actor or "SYSTEM",
                    "now": datetime.now(timezone.utc),
                }
            )
        else:
            self.session.execute(
                text("UPDATE action SET state = :state WHERE id = :id"),
                {"id": action_id, "state": to_state.value}
            )

        # Log transition to trace
        from uuid import uuid4

        case_id = self._get_case_id(action_id)
        seq = get_next_trace_seq(case_id, self.session)
        self.session.execute(
            text("""
                INSERT INTO trace_event
                (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                VALUES (:id, :case_id, :seq, 'STATE_ENTER', 'action', :action_id, CAST(:meta AS jsonb), :created_at)
            """),
            {
                "id": uuid4(),
                "case_id": case_id,
                "seq": seq,
                "action_id": str(action_id),
                "meta": json.dumps({
                    "from_state": current_state.value,
                    "to_state": to_state.value,
                    "reason": reason,
                    "actor": actor,
                }),
                "created_at": datetime.now(timezone.utc),
            }
        )

        self.session.commit()
        return True, None

    def _get_case_id(self, action_id: UUID) -> UUID:
        """Get case ID for action."""
        result = self.session.execute(
            text("SELECT case_id FROM action WHERE id = :id"),
            {"id": action_id}
        )
        row = result.fetchone()
        return row[0] if row else None

    def get_state(self, action_id: UUID) -> Optional[ActionState]:
        """Get current state of action."""
        result = self.session.execute(
            text("SELECT state FROM action WHERE id = :id"),
            {"id": action_id}
        )
        row = result.fetchone()
        return ActionState(row[0]) if row else None

    def get_pending_approvals(self, case_id: Optional[UUID] = None) -> List[dict]:
        """Get actions pending approval."""
        if case_id:
            result = self.session.execute(
                text("""
                    SELECT id, case_id, type, args, risk_level, created_at
                    FROM action
                    WHERE state = 'PENDING_APPROVAL'
                      AND case_id = :case_id
                    ORDER BY created_at
                """),
                {"case_id": case_id}
            )
        else:
            result = self.session.execute(
                text("""
                    SELECT id, case_id, type, args, risk_level, created_at
                    FROM action
                    WHERE state = 'PENDING_APPROVAL'
                    ORDER BY created_at
                """)
            )

        return [
            {
                "action_id": row[0],
                "case_id": row[1],
                "type": row[2],
                "args": row[3],
                "risk_level": row[4],
                "created_at": row[5],
            }
            for row in result
        ]
