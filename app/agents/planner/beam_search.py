# app/agents/planner/beam_search.py
"""
Beam search planner with deterministic scoring.

DETERMINISTIC - no LLM calls for scoring.

Action candidates: INVESTIGATE or INTERVENTION
Beam width: 4
Max depth: 4

Scoring:
- INVESTIGATE: score = information_gain - cost - risk_penalty
- INTERVENTION: score = action_value - cost - risk_penalty

FIXED: Interventions now have base "action_value" so they get positive scores
and are included in the output (previously only SET_POSTURE was included).
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

from ..state_graph import BeliefState, Posture
from .action_library import (
    get_action_risk_level,
    requires_approval,
)


# ============================================================
# DETERMINISTIC SCORING CONSTANTS
# ============================================================
# These are the SINGLE SOURCE OF TRUTH for information gain scoring.
# Do not use LLM calls to compute these values.

# How much each uncertainty type is "worth" resolving
UNCERTAINTY_VALUES: Dict[str, float] = {
    "airport_status_unknown": 1.0,
    "weather_conditions_unknown": 0.8,
    "alert_status_unknown": 0.7,
    "movement_data_unknown": 0.5,
    "contradiction_unresolved": 0.9,
}

# Which uncertainties each tool resolves
TOOL_RESOLVES: Dict[str, List[str]] = {
    "fetch_faa_status": ["airport_status_unknown"],
    "fetch_weather": ["weather_conditions_unknown"],
    "fetch_alerts": ["alert_status_unknown"],
    "fetch_opensky": ["movement_data_unknown"],
}

# Relative cost of each tool (API latency, rate limits)
TOOL_COSTS: Dict[str, float] = {
    "fetch_faa_status": 0.1,
    "fetch_weather": 0.1,
    "fetch_alerts": 0.1,
    "fetch_opensky": 0.3,  # Higher due to rate limits
}

# Cost of each intervention type
INTERVENTION_COSTS: Dict[str, float] = {
    "SET_POSTURE": 0.0,
    "PUBLISH_GATEWAY_ADVISORY": 0.1,
    "UPDATE_BOOKING_RULES": 0.2,
    "TRIGGER_REEVALUATION": 0.1,
    "ESCALATE_OPS": 0.2,
    # Shipment actions have higher costs
    "HOLD_CARGO": 0.5,
    "RELEASE_CARGO": 0.3,
    "SWITCH_GATEWAY": 0.8,
    "REBOOK_FLIGHT": 0.9,
    "UPGRADE_SERVICE": 0.7,
    "NOTIFY_CUSTOMER": 0.6,
    "FILE_CLAIM": 0.8,
}

# Base action value for interventions (they don't resolve uncertainty but DO provide operational value)
# These values are applied when the action is contextually appropriate
ACTION_VALUES: Dict[str, float] = {
    "SET_POSTURE": 1.0,               # Core output - always valuable
    "PUBLISH_GATEWAY_ADVISORY": 0.6,  # Important for non-ACCEPT postures
    "UPDATE_BOOKING_RULES": 0.5,      # Important for RESTRICT/HOLD
    "TRIGGER_REEVALUATION": 0.4,      # Useful when contradictions exist
    "ESCALATE_OPS": 0.7,              # Important for ESCALATE posture
    # Shipment actions have value but require booking evidence
    "HOLD_CARGO": 0.6,
    "RELEASE_CARGO": 0.5,
    "SWITCH_GATEWAY": 0.7,
    "REBOOK_FLIGHT": 0.8,
    "UPGRADE_SERVICE": 0.5,
    "NOTIFY_CUSTOMER": 0.6,
    "FILE_CLAIM": 0.5,
}

# Risk penalties
RISK_PENALTIES: Dict[str, float] = {
    "LOW": 0.0,
    "MEDIUM": 0.1,
    "HIGH": 0.3,
}


@dataclass
class ActionCandidate:
    """Candidate action for planning."""
    action_type: str  # "INVESTIGATE" or intervention type
    tool: Optional[str] = None  # For INVESTIGATE actions
    args: Dict[str, Any] = field(default_factory=dict)
    score: float = 0.0
    requires_approval: bool = False
    requires_notification: bool = False
    risk_level: str = "LOW"


@dataclass
class ActionSequence:
    """Sequence of actions (for beam search)."""
    actions: List[ActionCandidate] = field(default_factory=list)
    total_score: float = 0.0


def score_action(
    action: ActionCandidate,
    belief_state: BeliefState,
) -> float:
    """
    Deterministic action scoring.

    score = information_gain - cost - risk_penalty

    No LLM calls. Pure computation from belief state.

    Args:
        action: Action candidate to score
        belief_state: Current belief state

    Returns:
        Score value (higher is better)
    """
    if action.action_type == "INVESTIGATE":
        return _score_investigation(action, belief_state)
    else:
        return _score_intervention(action, belief_state)


def _score_investigation(
    action: ActionCandidate,
    belief_state: BeliefState,
) -> float:
    """Score an investigation action."""
    tool = action.tool
    if not tool:
        return 0.0

    # Information gain = sum of uncertainty values this tool resolves
    resolvable = TOOL_RESOLVES.get(tool, [])
    open_uncertainties = [u.uncertainty_type for u in belief_state.open_uncertainties]

    info_gain = sum(
        UNCERTAINTY_VALUES.get(u, 0)
        for u in open_uncertainties
        if u in resolvable
    )

    # Cost
    cost = TOOL_COSTS.get(tool, 0.1)

    # Risk (investigations have no risk)
    risk = 0.0

    return info_gain - cost - risk


def _score_intervention(
    action: ActionCandidate,
    belief_state: BeliefState,
) -> float:
    """
    Score an intervention action.

    FIXED: Interventions now have base "action value" (not just info_gain).
    They don't resolve uncertainty, but they DO provide operational value.

    score = action_value - cost - risk_penalty
    """
    action_type = action.action_type

    # Action value: base value for taking this action
    # (Interventions don't resolve uncertainty but provide operational value)
    action_value = ACTION_VALUES.get(action_type, 0.3)

    # Cost
    cost = INTERVENTION_COSTS.get(action_type, 0.5)

    # Risk penalty
    risk_level = get_action_risk_level(action_type)
    risk = RISK_PENALTIES.get(risk_level, 0.1)

    # Additional penalty if requires approval
    if requires_approval(action_type):
        risk += 0.1

    return action_value - cost - risk


def plan_actions(
    belief_state: BeliefState,
    risk_assessment: Optional[Dict[str, Any]],
    beam_width: int = 4,
    max_depth: int = 4,
) -> List[Dict[str, Any]]:
    """
    Plan actions using beam search with deterministic scoring.

    Args:
        belief_state: Current belief state
        risk_assessment: Risk assessment from RiskQuantAgent
        beam_width: Number of candidates to keep at each level
        max_depth: Maximum depth of search

    Returns:
        List of proposed actions (as dicts)
    """
    # Generate intervention candidates based on risk assessment
    candidates = _generate_intervention_candidates(belief_state, risk_assessment)

    # Score all candidates
    for candidate in candidates:
        candidate.score = score_action(candidate, belief_state)

    # Sort by score (descending)
    candidates.sort(key=lambda c: -c.score)

    # Take top candidates up to beam_width
    top_candidates = candidates[:beam_width]

    # Convert to action dicts
    # FIXED: Now all generated candidates have positive scores (action_value - cost - risk)
    # so we include everything with score >= 0 (was incorrectly filtering out non-SET_POSTURE)
    proposed_actions = []
    for candidate in top_candidates:
        if candidate.score >= 0:
            proposed_actions.append({
                "type": candidate.action_type,
                "args": candidate.args,
                "score": candidate.score,
                "risk_level": candidate.risk_level,
                "requires_approval": candidate.requires_approval,
                "requires_notification": candidate.requires_notification,
            })

    return proposed_actions


def _generate_intervention_candidates(
    belief_state: BeliefState,
    risk_assessment: Optional[Dict[str, Any]],
) -> List[ActionCandidate]:
    """Generate intervention candidates based on current state."""
    candidates = []

    # Get recommended posture
    recommended_posture = Posture.HOLD
    if risk_assessment:
        posture_str = risk_assessment.get("recommended_posture", "HOLD")
        recommended_posture = Posture[posture_str]

    # Always propose SET_POSTURE (include airport from belief_state)
    candidates.append(ActionCandidate(
        action_type="SET_POSTURE",
        args={
            "posture": recommended_posture.value,
            "airport": belief_state.airport_icao,  # Required for webhook payload
        },
        requires_approval=False,
        requires_notification=False,
        risk_level="LOW",
    ))

    # Propose PUBLISH_GATEWAY_ADVISORY for non-ACCEPT postures
    if recommended_posture != Posture.ACCEPT:
        candidates.append(ActionCandidate(
            action_type="PUBLISH_GATEWAY_ADVISORY",
            args={
                "posture": recommended_posture.value,
                "airport": belief_state.airport_icao,
            },
            requires_approval=False,
            requires_notification=False,
            risk_level="LOW",
        ))

    # Propose UPDATE_BOOKING_RULES for RESTRICT/HOLD
    if recommended_posture in (Posture.RESTRICT, Posture.HOLD):
        candidates.append(ActionCandidate(
            action_type="UPDATE_BOOKING_RULES",
            args={"restriction_level": recommended_posture.value},
            requires_approval=False,
            requires_notification=False,
            risk_level="MEDIUM",
        ))

    # Propose ESCALATE_OPS for ESCALATE posture
    if recommended_posture == Posture.ESCALATE:
        candidates.append(ActionCandidate(
            action_type="ESCALATE_OPS",
            args={"reason": "Automated escalation required"},
            requires_approval=False,
            requires_notification=True,
            risk_level="LOW",
        ))

    # Propose TRIGGER_REEVALUATION if contradictions exist
    if belief_state.contradiction_count > 0:
        candidates.append(ActionCandidate(
            action_type="TRIGGER_REEVALUATION",
            args={"reason": "Unresolved contradictions"},
            requires_approval=False,
            requires_notification=False,
            risk_level="LOW",
        ))

    return candidates
