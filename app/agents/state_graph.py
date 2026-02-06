# app/agents/state_graph.py
"""
Explicit state machine for the agent.

NOT a ReAct loop - this is a deterministic state machine with 12 explicit states.
State transitions are based on explicit conditions, not LLM decisions.
"""

from enum import Enum
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from uuid import UUID


class AgentState(Enum):
    """
    Explicit agent states.

    State machine with 12 states, not a loop.
    """
    S0_INIT_CASE = "S0_INIT_CASE"
    S1_INGEST_SIGNALS = "S1_INGEST_SIGNALS"
    S2_BUILD_BELIEF_STATE = "S2_BUILD_BELIEF_STATE"
    S3_DETECT_CONTRADICTIONS = "S3_DETECT_CONTRADICTIONS"
    S4_PLAN_NEXT_EVIDENCE = "S4_PLAN_NEXT_EVIDENCE"
    S5_GATHER_EVIDENCE = "S5_GATHER_EVIDENCE"
    S6_EVALUATE_POSTCONDITIONS = "S6_EVALUATE_POSTCONDITIONS"
    S7_PROPOSE_ACTIONS = "S7_PROPOSE_ACTIONS"
    S8_GOVERNANCE_REVIEW = "S8_GOVERNANCE_REVIEW"
    S9_EXECUTE_OR_BLOCK = "S9_EXECUTE_OR_BLOCK"
    S10_PACKETIZE_AND_PERSIST = "S10_PACKETIZE_AND_PERSIST"
    S11_REPLAY_AND_LEARN = "S11_REPLAY_AND_LEARN"


class Posture(Enum):
    """
    Gateway posture directive.

    The primary output of the system.
    """
    ACCEPT = "ACCEPT"       # Accept new bookings
    RESTRICT = "RESTRICT"   # Restrict specific service tiers/SLAs
    HOLD = "HOLD"           # Hold tendering until evidence clears
    ESCALATE = "ESCALATE"   # Escalate to duty manager


class StopCondition(Enum):
    """
    Stop conditions for the agent.
    """
    MET = "MET"                     # Goal achieved
    BLOCKED = "BLOCKED"             # Blocked by missing evidence
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED"  # Budget/iteration limit hit


@dataclass
class Hypothesis:
    """
    A hypothesis in the belief state.
    """
    id: UUID
    text: str
    confidence: float
    supporting_claim_ids: List[UUID] = field(default_factory=list)


@dataclass
class Uncertainty:
    """
    An uncertainty that needs resolution.
    """
    id: str
    question: str
    uncertainty_type: str  # airport_status_unknown, weather_conditions_unknown, etc.
    missing_evidence_request_id: Optional[UUID] = None
    resolved: bool = False
    resolved_by_evidence_id: Optional[UUID] = None


@dataclass
class ContradictionRef:
    """
    Reference to a detected contradiction.
    """
    claim_a: UUID
    claim_b: UUID
    contradiction_type: str
    why_it_matters: str
    resolved: bool = False


@dataclass
class BeliefState:
    """
    The agent's current belief state.

    Updated as the agent progresses through states.
    """
    # Case context
    airport_icao: Optional[str] = None
    case_id: Optional[UUID] = None

    # Beliefs
    hypotheses: List[Hypothesis] = field(default_factory=list)
    uncertainties: List[Uncertainty] = field(default_factory=list)
    contradictions: List[ContradictionRef] = field(default_factory=list)
    current_posture: Posture = Posture.HOLD
    stop_condition: Optional[StopCondition] = None

    # Evidence tracking
    evidence_ids: List[UUID] = field(default_factory=list)
    # Track valid evidence separately (excludes API errors)
    # This is used by the critic to ensure quality, not just quantity
    valid_evidence_ids: List[UUID] = field(default_factory=list)
    error_evidence_ids: List[UUID] = field(default_factory=list)
    claim_ids: List[UUID] = field(default_factory=list)
    edge_ids: List[UUID] = field(default_factory=list)

    # Budget tracking
    iterations: int = 0
    max_iterations: int = 10
    tool_calls: int = 0
    max_tool_calls: int = 50

    def add_hypothesis(self, hypothesis: Hypothesis):
        """Add a hypothesis."""
        self.hypotheses.append(hypothesis)

    def add_uncertainty(self, uncertainty: Uncertainty):
        """Add an uncertainty."""
        self.uncertainties.append(uncertainty)

    def add_contradiction(self, contradiction: ContradictionRef):
        """Add a contradiction."""
        self.contradictions.append(contradiction)

    def resolve_uncertainty(self, uncertainty_id: str, evidence_id: UUID):
        """Mark an uncertainty as resolved."""
        for u in self.uncertainties:
            if u.id == uncertainty_id:
                u.resolved = True
                u.resolved_by_evidence_id = evidence_id
                break

    @property
    def open_uncertainties(self) -> List[Uncertainty]:
        """Get unresolved uncertainties."""
        return [u for u in self.uncertainties if not u.resolved]

    @property
    def open_contradictions(self) -> List[ContradictionRef]:
        """Get unresolved contradictions."""
        return [c for c in self.contradictions if not c.resolved]

    @property
    def uncertainty_count(self) -> int:
        """Count of open uncertainties."""
        return len(self.open_uncertainties)

    @property
    def contradiction_count(self) -> int:
        """Count of open contradictions."""
        return len(self.open_contradictions)

    @property
    def evidence_count(self) -> int:
        """Count of all gathered evidence (including errors)."""
        return len(self.evidence_ids)

    @property
    def valid_evidence_count(self) -> int:
        """Count of valid evidence (excludes API errors).

        This is what should be used by the critic for quality assessment.
        API errors are not evidence of conditions - they're evidence of failure to fetch.
        """
        return len(self.valid_evidence_ids)

    @property
    def error_evidence_count(self) -> int:
        """Count of error evidence (API failures, etc.)."""
        return len(self.error_evidence_ids)

    @property
    def budget_remaining(self) -> bool:
        """Check if within budget."""
        return (
            self.iterations < self.max_iterations and
            self.tool_calls < self.max_tool_calls
        )

    def increment_iteration(self):
        """Increment iteration count."""
        self.iterations += 1
        if not self.budget_remaining:
            self.stop_condition = StopCondition.BUDGET_EXCEEDED

    def increment_tool_calls(self, count: int = 1):
        """Increment tool call count."""
        self.tool_calls += count
        if not self.budget_remaining:
            self.stop_condition = StopCondition.BUDGET_EXCEEDED

    def to_summary(self) -> Dict[str, Any]:
        """
        Convert to summary dict.

        This is what gets persisted to trace_event.meta.
        NOT the full belief state - no chain-of-thought.
        """
        return {
            "airport_icao": self.airport_icao,
            "hypothesis_count": len(self.hypotheses),
            "uncertainty_count": self.uncertainty_count,
            "contradiction_count": self.contradiction_count,
            "evidence_count": self.evidence_count,
            "current_posture": self.current_posture.value,
            "stop_condition": self.stop_condition.value if self.stop_condition else None,
            "iterations": self.iterations,
            "tool_calls": self.tool_calls,
        }


# State transition rules (deterministic)
STATE_TRANSITIONS = {
    AgentState.S0_INIT_CASE: [AgentState.S1_INGEST_SIGNALS],
    AgentState.S1_INGEST_SIGNALS: [AgentState.S2_BUILD_BELIEF_STATE],
    AgentState.S2_BUILD_BELIEF_STATE: [AgentState.S3_DETECT_CONTRADICTIONS],
    AgentState.S3_DETECT_CONTRADICTIONS: [
        AgentState.S4_PLAN_NEXT_EVIDENCE,
        AgentState.S7_PROPOSE_ACTIONS,  # If no uncertainties
    ],
    AgentState.S4_PLAN_NEXT_EVIDENCE: [
        AgentState.S5_GATHER_EVIDENCE,
        AgentState.S7_PROPOSE_ACTIONS,  # If no more evidence needed
    ],
    AgentState.S5_GATHER_EVIDENCE: [AgentState.S6_EVALUATE_POSTCONDITIONS],
    AgentState.S6_EVALUATE_POSTCONDITIONS: [
        AgentState.S3_DETECT_CONTRADICTIONS,  # Loop back if more investigation needed
        AgentState.S7_PROPOSE_ACTIONS,  # Move forward if done
    ],
    AgentState.S7_PROPOSE_ACTIONS: [AgentState.S8_GOVERNANCE_REVIEW],
    AgentState.S8_GOVERNANCE_REVIEW: [AgentState.S9_EXECUTE_OR_BLOCK],
    AgentState.S9_EXECUTE_OR_BLOCK: [AgentState.S10_PACKETIZE_AND_PERSIST],
    AgentState.S10_PACKETIZE_AND_PERSIST: [AgentState.S11_REPLAY_AND_LEARN],
    AgentState.S11_REPLAY_AND_LEARN: [],  # Terminal state
}
