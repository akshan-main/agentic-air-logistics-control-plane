# app/governance/approvals.py
"""
Approval management for governed actions.
"""

from typing import Optional, Tuple
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from .models import ActionState
from .state_machine import ActionStateMachine


class ApprovalManager:
    """
    Manages action approvals.

    Handles approval requests, decisions, and escalations.
    """

    def __init__(self, session: Session):
        self.session = session
        self.state_machine = ActionStateMachine(session)

    def request_approval(
        self,
        action_id: UUID,
        requested_by: str = "SYSTEM",
        reason: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Request approval for an action.

        Args:
            action_id: Action ID
            requested_by: Who is requesting
            reason: Reason for approval request

        Returns:
            (success, error_message)
        """
        # Transition to PENDING_APPROVAL
        return self.state_machine.transition(
            action_id,
            ActionState.PENDING_APPROVAL,
            reason=reason or "Approval required by policy",
            actor=requested_by,
        )

    def approve(
        self,
        action_id: UUID,
        approved_by: str,
        reason: Optional[str] = None,
        auto_execute: bool = True,
    ) -> Tuple[bool, Optional[str]]:
        """
        Approve an action and optionally execute it.

        Args:
            action_id: Action ID
            approved_by: Who is approving
            reason: Optional reason
            auto_execute: If True, execute the action after approval

        Returns:
            (success, error_message)
        """
        # Check current state
        current_state = self.state_machine.get_state(action_id)
        if current_state != ActionState.PENDING_APPROVAL:
            return False, f"Action is not pending approval (state: {current_state})"

        # Transition to APPROVED
        success, error = self.state_machine.transition(
            action_id,
            ActionState.APPROVED,
            reason=reason or "Approved",
            actor=approved_by,
        )

        if not success:
            return success, error

        # Execute the approved action if requested
        if auto_execute:
            try:
                from ..agents.roles.executor import ExecutorAgent

                # Get case_id for executor
                result = self.session.execute(
                    text("SELECT case_id FROM action WHERE id = :id"),
                    {"id": action_id}
                )
                row = result.fetchone()
                if row:
                    case_id = row[0]
                    executor = ExecutorAgent(case_id, self.session)
                    outcome = executor.execute_approved_action(action_id)

                    if not outcome.get("success"):
                        return False, f"Execution failed: {outcome.get('error')}"

                    # Check if all actions for this case are now terminal
                    # If so, update case status to RESOLVED
                    self._check_and_resolve_case(case_id)

            except Exception as e:
                return False, f"Execution failed: {str(e)}"

        return True, None

    def _check_and_resolve_case(self, case_id: UUID) -> bool:
        """
        Check if all actions for a case are terminal and mark case as RESOLVED.

        Called after an action is executed to complete the case if all actions are done.

        Args:
            case_id: Case ID to check

        Returns:
            True if case was resolved, False otherwise
        """
        # Check if all actions are in terminal state
        result = self.session.execute(
            text("""
                SELECT COUNT(*) FROM action
                WHERE case_id = :case_id
                  AND state NOT IN ('COMPLETED', 'FAILED', 'ROLLED_BACK')
            """),
            {"case_id": case_id}
        )
        non_terminal_count = result.scalar()

        if non_terminal_count == 0:
            # All actions are terminal - resolve the case
            self.session.execute(
                text('UPDATE "case" SET status = :status WHERE id = :id'),
                {"id": case_id, "status": "RESOLVED"}
            )
            self.session.commit()
            return True

        return False

    def reject(
        self,
        action_id: UUID,
        rejected_by: str,
        reason: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        Reject an action.

        Args:
            action_id: Action ID
            rejected_by: Who is rejecting
            reason: Reason for rejection

        Returns:
            (success, error_message)
        """
        # Check current state
        current_state = self.state_machine.get_state(action_id)
        if current_state != ActionState.PENDING_APPROVAL:
            return False, f"Action is not pending approval (state: {current_state})"

        # Transition back to PROPOSED (rejected)
        success, error = self.state_machine.transition(
            action_id,
            ActionState.PROPOSED,
            reason=f"REJECTED: {reason}",
            actor=rejected_by,
        )

        if success:
            # Mark as rejected in action record
            self.session.execute(
                text("""
                    UPDATE action
                    SET args = jsonb_set(args, '{_rejected}', 'true'::jsonb)
                    WHERE id = :id
                """),
                {"id": action_id}
            )
            self.session.commit()

        return success, error

    def get_approval_status(self, action_id: UUID) -> dict:
        """
        Get approval status for an action.

        Args:
            action_id: Action ID

        Returns:
            Status dict
        """
        result = self.session.execute(
            text("""
                SELECT state, requires_approval, approved_by, approved_at, risk_level
                FROM action
                WHERE id = :id
            """),
            {"id": action_id}
        )

        row = result.fetchone()
        if not row:
            return {"error": "Action not found"}

        return {
            "action_id": str(action_id),
            "state": row[0],
            "requires_approval": row[1],
            "approved_by": row[2],
            "approved_at": row[3].isoformat() if row[3] else None,
            "risk_level": row[4],
        }


def approve_action(
    action_id: UUID,
    approved_by: str,
    session: Session,
    reason: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Convenience function to approve an action.

    Args:
        action_id: Action ID
        approved_by: Approver identifier
        session: Database session
        reason: Optional reason

    Returns:
        (success, error_message)
    """
    manager = ApprovalManager(session)
    return manager.approve(action_id, approved_by, reason)


def reject_action(
    action_id: UUID,
    rejected_by: str,
    reason: str,
    session: Session,
) -> Tuple[bool, Optional[str]]:
    """
    Convenience function to reject an action.

    Args:
        action_id: Action ID
        rejected_by: Rejector identifier
        reason: Rejection reason
        session: Database session

    Returns:
        (success, error_message)
    """
    manager = ApprovalManager(session)
    return manager.reject(action_id, rejected_by, reason)
