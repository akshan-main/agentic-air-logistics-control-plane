# app/governance/models.py
"""
Governance models for action approval workflows.
"""

from enum import Enum
from typing import Optional, Dict, Any
from dataclasses import dataclass
from uuid import UUID
from datetime import datetime


class ActionState(Enum):
    """
    Action state machine states.

    PROPOSED -> PENDING_APPROVAL -> APPROVED -> EXECUTING -> COMPLETED/FAILED
    FAILED -> ROLLED_BACK
    """
    PROPOSED = "PROPOSED"
    PENDING_APPROVAL = "PENDING_APPROVAL"
    APPROVED = "APPROVED"
    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"


@dataclass
class ApprovalRequest:
    """Request for action approval."""
    action_id: UUID
    case_id: UUID
    action_type: str
    args: Dict[str, Any]
    risk_level: str
    requested_at: datetime
    requested_by: str  # System or user who proposed
    reason: str
    expires_at: Optional[datetime] = None


@dataclass
class ApprovalDecision:
    """Decision on approval request."""
    request_id: UUID
    action_id: UUID
    approved: bool
    decided_by: str
    decided_at: datetime
    reason: Optional[str] = None
    conditions: Optional[Dict[str, Any]] = None  # Conditional approval
