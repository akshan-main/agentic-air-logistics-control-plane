# app/agents/guardrails/validators.py
"""
Validators for actions, claims, and edges.

These validate data before persistence.
"""

from typing import Tuple, Optional, Dict, Any
from uuid import UUID

from sqlalchemy.orm import Session

from ..planner.action_library import ACTION_TYPES, SHIPMENT_ACTIONS


def validate_action(
    action_type: str,
    args: Dict[str, Any],
    case_id: UUID,
    session: Session,
) -> Tuple[bool, Optional[str]]:
    """
    Validate action before creation.

    Args:
        action_type: Type of action
        args: Action arguments
        case_id: Case ID
        session: Database session

    Returns:
        (valid, error_message)
    """
    # Check action type is valid
    if action_type not in ACTION_TYPES:
        return False, f"Unknown action type: {action_type}"

    # Check shipment actions have booking evidence
    if action_type in SHIPMENT_ACTIONS:
        from .gates import NoShipmentActionWithoutBookingGate
        gate = NoShipmentActionWithoutBookingGate()
        return gate.check(action_type, case_id, session)

    # Validate specific action types
    if action_type == "SET_POSTURE":
        posture = args.get("posture")
        valid_postures = {"ACCEPT", "RESTRICT", "HOLD", "ESCALATE"}
        if posture not in valid_postures:
            return False, f"Invalid posture: {posture}"

    return True, None


def validate_claim(
    text: str,
    status: str,
    confidence: float,
    claim_id: Optional[UUID] = None,
    session: Optional[Session] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Validate claim data.

    Args:
        text: Claim text
        status: Claim status
        confidence: Confidence score
        claim_id: Optional claim ID (for updates)
        session: Optional database session

    Returns:
        (valid, error_message)
    """
    # Check text
    if not text or len(text.strip()) == 0:
        return False, "Claim text cannot be empty"

    # Check status
    valid_statuses = {"DRAFT", "HYPOTHESIS", "FACT", "RETRACTED"}
    if status not in valid_statuses:
        return False, f"Invalid claim status: {status}"

    # Check confidence
    if not 0 <= confidence <= 1:
        return False, f"Confidence must be between 0 and 1, got: {confidence}"

    # If promoting to FACT, check evidence binding
    if status == "FACT" and claim_id and session:
        from .gates import EvidenceBindingGate
        gate = EvidenceBindingGate()
        return gate.check_claim(claim_id, status, session)

    return True, None


def validate_edge(
    src: UUID,
    dst: UUID,
    edge_type: str,
    status: str,
    confidence: float,
    edge_id: Optional[UUID] = None,
    session: Optional[Session] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Validate edge data.

    Args:
        src: Source node ID
        dst: Destination node ID
        edge_type: Edge type
        status: Edge status
        confidence: Confidence score
        edge_id: Optional edge ID (for updates)
        session: Optional database session

    Returns:
        (valid, error_message)
    """
    # Check nodes are different (unless self-loop is intentional)
    # Self-loops are allowed for signal edges

    # Check status
    valid_statuses = {"DRAFT", "FACT", "RETRACTED"}
    if status not in valid_statuses:
        return False, f"Invalid edge status: {status}"

    # Check confidence
    if not 0 <= confidence <= 1:
        return False, f"Confidence must be between 0 and 1, got: {confidence}"

    # If promoting to FACT, check evidence binding
    if status == "FACT" and edge_id and session:
        from .gates import EvidenceBindingGate
        gate = EvidenceBindingGate()
        return gate.check_edge(edge_id, status, session)

    return True, None
