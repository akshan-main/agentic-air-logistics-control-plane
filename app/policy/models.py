# app/policy/models.py
"""
Policy models.
"""

from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from uuid import UUID
from datetime import datetime


@dataclass
class PolicyCondition:
    """Condition that must be met for policy to apply."""
    field: str  # e.g., "risk_level", "action_type"
    operator: str  # e.g., "==", "in", ">", "<"
    value: Any  # e.g., "HIGH", ["HOLD_CARGO", "REBOOK_FLIGHT"]


@dataclass
class PolicyEffect:
    """Effect when policy conditions are met."""
    action: str  # e.g., "require_approval", "block", "warn"
    params: Optional[Dict[str, Any]] = None


@dataclass
class Policy:
    """Governance policy."""
    id: UUID
    type: str  # e.g., "approval_requirement", "evidence_requirement"
    text: str  # Human-readable description
    conditions: List[PolicyCondition]
    effects: List[PolicyEffect]
    effective_from: datetime
    effective_to: Optional[datetime] = None
    priority: int = 0  # Higher priority policies evaluated first

    def is_active(self, at_time: datetime) -> bool:
        """Check if policy is active at given time."""
        if at_time < self.effective_from:
            return False
        if self.effective_to and at_time >= self.effective_to:
            return False
        return True
