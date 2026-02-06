# app/packets/models.py
"""
Decision packet models.

The DecisionPacket is the primary output of the system -
a complete audit trail of the decision process.
"""

from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from uuid import UUID
from datetime import datetime


@dataclass
class PostureAction:
    """Posture directive action."""
    posture: str  # ACCEPT, RESTRICT, HOLD, ESCALATE
    airport: str
    effective_at: datetime
    reason: str


@dataclass
class ClaimSummary:
    """Summary of a claim in the packet."""
    claim_id: UUID
    text: str
    status: str
    confidence: float
    evidence_ids: List[UUID]


@dataclass
class EvidenceSummary:
    """Summary of evidence in the packet."""
    evidence_id: UUID
    source_system: str
    retrieved_at: datetime
    excerpt: Optional[str] = None


@dataclass
class ContradictionSummary:
    """Summary of a contradiction in the packet."""
    claim_a_id: UUID
    claim_b_id: UUID
    contradiction_type: str
    resolution_status: str


@dataclass
class PolicyReference:
    """Reference to an applied policy."""
    policy_id: Optional[str]  # Can be UUID or builtin policy ID
    policy_text: str
    effect: str


@dataclass
class ActionSummary:
    """Summary of an action in the packet."""
    action_id: UUID
    action_type: str
    args: Dict[str, Any]
    state: str
    risk_level: str


@dataclass
class OutcomeSummary:
    """Summary of an action outcome."""
    action_id: UUID
    success: bool
    payload: Dict[str, Any]


@dataclass
class BlockedInfo:
    """Information about blocked state."""
    reason: str
    missing_evidence_requests: List[Dict[str, Any]]


@dataclass
class PacketMetrics:
    """
    Operational metrics for the decision packet.

    PDL (Posture Decision Latency) is the key metric.
    """
    first_signal_at: datetime
    posture_emitted_at: datetime
    pdl_seconds: float  # Posture Decision Latency
    evidence_count: int
    uncertainty_resolved_count: int
    contradiction_count: int
    action_count: int


@dataclass
class DecisionPacket:
    """
    Complete decision packet.

    The primary output of the system - contains everything
    needed to audit the decision process.
    """
    case_id: UUID
    case_type: str
    scope: Dict[str, Any]  # airport, lane, service tier

    # Timestamps
    created_at: datetime
    completed_at: Optional[datetime]

    # Primary decision
    posture_decision: PostureAction

    # Evidence and claims
    top_claims: List[ClaimSummary] = field(default_factory=list)
    evidence_list: List[EvidenceSummary] = field(default_factory=list)

    # Issues
    contradictions: List[ContradictionSummary] = field(default_factory=list)

    # Governance
    policies_applied: List[PolicyReference] = field(default_factory=list)

    # Actions
    actions_proposed: List[ActionSummary] = field(default_factory=list)
    actions_executed: List[OutcomeSummary] = field(default_factory=list)

    # Blocked state (if applicable)
    blocked_section: Optional[BlockedInfo] = None

    # Metrics
    metrics: Optional[PacketMetrics] = None

    # Workflow trace (state transitions)
    workflow_trace: List[Dict[str, Any]] = field(default_factory=list)

    # Confidence explanation
    confidence_breakdown: Optional[Dict[str, Any]] = None

    # Cascade impact (operational data)
    cascade_impact: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "case_id": str(self.case_id),
            "case_type": self.case_type,
            "scope": self.scope,
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "posture_decision": {
                "posture": self.posture_decision.posture,
                "airport": self.posture_decision.airport,
                "effective_at": self.posture_decision.effective_at.isoformat(),
                "reason": self.posture_decision.reason,
            },
            "top_claims": [
                {
                    "claim_id": str(c.claim_id),
                    "text": c.text,
                    "status": c.status,
                    "confidence": c.confidence,
                    "evidence_ids": [str(e) for e in c.evidence_ids],
                }
                for c in self.top_claims
            ],
            "evidence_list": [
                {
                    "evidence_id": str(e.evidence_id),
                    "source_system": e.source_system,
                    "retrieved_at": e.retrieved_at.isoformat(),
                    "excerpt": e.excerpt,
                }
                for e in self.evidence_list
            ],
            "contradictions": [
                {
                    "claim_a_id": str(c.claim_a_id),
                    "claim_b_id": str(c.claim_b_id),
                    "contradiction_type": c.contradiction_type,
                    "resolution_status": c.resolution_status,
                }
                for c in self.contradictions
            ],
            "policies_applied": [
                {
                    "policy_id": str(p.policy_id),
                    "policy_text": p.policy_text,
                    "effect": p.effect,
                }
                for p in self.policies_applied
            ],
            "actions_proposed": [
                {
                    "action_id": str(a.action_id),
                    "action_type": a.action_type,
                    "args": a.args,
                    "state": a.state,
                    "risk_level": a.risk_level,
                }
                for a in self.actions_proposed
            ],
            "actions_executed": [
                {
                    "action_id": str(o.action_id),
                    "success": o.success,
                    "payload": o.payload,
                }
                for o in self.actions_executed
            ],
            "blocked_section": {
                "reason": self.blocked_section.reason,
                "missing_evidence_requests": self.blocked_section.missing_evidence_requests,
            } if self.blocked_section else None,
            "metrics": {
                "first_signal_at": self.metrics.first_signal_at.isoformat(),
                "posture_emitted_at": self.metrics.posture_emitted_at.isoformat(),
                "pdl_seconds": self.metrics.pdl_seconds,
                "evidence_count": self.metrics.evidence_count,
                "uncertainty_resolved_count": self.metrics.uncertainty_resolved_count,
                "contradiction_count": self.metrics.contradiction_count,
                "action_count": self.metrics.action_count,
            } if self.metrics else None,
            "workflow_trace": self.workflow_trace,
            "confidence_breakdown": self.confidence_breakdown,
            "cascade_impact": self.cascade_impact,
        }
