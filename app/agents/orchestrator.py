# app/agents/orchestrator.py
"""
Multi-agent orchestration with deterministic state machine.

This is NOT "six wrappers around one LLM" - it's a deterministic state machine
that invokes specialized agents at specific states, with explicit interrupt/veto paths.

Key properties:
1. State machine is deterministic - transitions based on explicit conditions
2. Critic can force return to INVESTIGATE state (evidence quality gate)
3. PolicyJudge can veto actions (policy compliance gate)
4. Only trace_events, actions, evidence_ids are persisted - no chain-of-thought
"""

import json
from enum import Enum
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import SessionLocal
from ..logging import get_logger
from .state_graph import BeliefState, Posture, StopCondition

logger = get_logger(__name__)


class OrchestratorState(Enum):
    """State machine for orchestrating multi-agent investigation."""
    INIT = "INIT"
    INVESTIGATE = "INVESTIGATE"           # InvestigatorAgent gathers evidence
    QUANTIFY_RISK = "QUANTIFY_RISK"       # RiskQuantAgent assesses exposure
    EVALUATE_POLICY = "EVALUATE_POLICY"   # PolicyJudgeAgent checks constraints
    CRITIQUE = "CRITIQUE"                 # CriticAgent challenges conclusions
    PLAN_ACTIONS = "PLAN_ACTIONS"         # Planner proposes interventions
    DRAFT_COMMS = "DRAFT_COMMS"           # CommsAgent drafts notifications
    EXECUTE = "EXECUTE"                   # ExecutorAgent runs approved actions
    COMPLETE = "COMPLETE"


@dataclass
class OrchestratorTransition:
    """Defines valid state transitions with conditions."""
    from_state: OrchestratorState
    to_state: OrchestratorState
    condition: str  # Human-readable condition
    handler: str    # Method to call


# State transition table - deterministic, not LLM-decided
TRANSITIONS: List[OrchestratorTransition] = [
    # INIT -> INVESTIGATE (always)
    OrchestratorTransition(
        OrchestratorState.INIT,
        OrchestratorState.INVESTIGATE,
        "always",
        "start_investigation"
    ),

    # INVESTIGATE -> INVESTIGATE (if still gathering)
    OrchestratorTransition(
        OrchestratorState.INVESTIGATE,
        OrchestratorState.INVESTIGATE,
        "uncertainty_count > 0 and budget_remaining",
        "continue_investigation"
    ),

    # INVESTIGATE -> COMPLETE (blocked by missing evidence)
    OrchestratorTransition(
        OrchestratorState.INVESTIGATE,
        OrchestratorState.COMPLETE,
        "has_blocking_missing_evidence",
        "complete_missing_evidence_blocked"
    ),

    # INVESTIGATE -> QUANTIFY_RISK (when evidence gathered)
    OrchestratorTransition(
        OrchestratorState.INVESTIGATE,
        OrchestratorState.QUANTIFY_RISK,
        "evidence_count > 0 and no_blocking_missing_evidence",
        "quantify_risk"
    ),

    # QUANTIFY_RISK -> CRITIQUE (always run critic before policy)
    OrchestratorTransition(
        OrchestratorState.QUANTIFY_RISK,
        OrchestratorState.CRITIQUE,
        "risk_assessment_complete",
        "run_critic"
    ),

    # CRITIQUE -> INVESTIGATE (if critic rejects evidence quality)
    OrchestratorTransition(
        OrchestratorState.CRITIQUE,
        OrchestratorState.INVESTIGATE,
        "critic_verdict == 'INSUFFICIENT_EVIDENCE'",
        "force_reinvestigation"
    ),

    # CRITIQUE -> EVALUATE_POLICY (if critic accepts)
    OrchestratorTransition(
        OrchestratorState.CRITIQUE,
        OrchestratorState.EVALUATE_POLICY,
        "critic_verdict == 'ACCEPTABLE'",
        "evaluate_policy"
    ),

    # EVALUATE_POLICY -> PLAN_ACTIONS (if no policy violations)
    OrchestratorTransition(
        OrchestratorState.EVALUATE_POLICY,
        OrchestratorState.PLAN_ACTIONS,
        "policy_verdict == 'COMPLIANT'",
        "plan_actions"
    ),

    # EVALUATE_POLICY -> INVESTIGATE (if policy requires more evidence)
    OrchestratorTransition(
        OrchestratorState.EVALUATE_POLICY,
        OrchestratorState.INVESTIGATE,
        "policy_verdict == 'NEEDS_EVIDENCE'",
        "force_reinvestigation"
    ),

    # EVALUATE_POLICY -> COMPLETE (if policy blocks action entirely)
    OrchestratorTransition(
        OrchestratorState.EVALUATE_POLICY,
        OrchestratorState.COMPLETE,
        "policy_verdict == 'BLOCKED'",
        "complete_blocked"
    ),

    # PLAN_ACTIONS -> DRAFT_COMMS (if actions proposed)
    OrchestratorTransition(
        OrchestratorState.PLAN_ACTIONS,
        OrchestratorState.DRAFT_COMMS,
        "proposed_actions_count > 0 and any_requires_notification",
        "draft_communications"
    ),

    # PLAN_ACTIONS -> EXECUTE (if actions proposed, no comms needed)
    OrchestratorTransition(
        OrchestratorState.PLAN_ACTIONS,
        OrchestratorState.EXECUTE,
        "proposed_actions_count > 0 and not any_requires_notification",
        "execute_actions"
    ),

    # PLAN_ACTIONS -> COMPLETE (no actions needed - case resolved at posture level)
    OrchestratorTransition(
        OrchestratorState.PLAN_ACTIONS,
        OrchestratorState.COMPLETE,
        "proposed_actions_count == 0",
        "complete_no_actions"
    ),

    # DRAFT_COMMS -> EXECUTE
    OrchestratorTransition(
        OrchestratorState.DRAFT_COMMS,
        OrchestratorState.EXECUTE,
        "communications_drafted",
        "execute_actions"
    ),

    # EXECUTE -> COMPLETE (all actions done)
    OrchestratorTransition(
        OrchestratorState.EXECUTE,
        OrchestratorState.COMPLETE,
        "all_actions_terminal",
        "complete_case"
    ),

    # EXECUTE -> COMPLETE (waiting for approval - can't proceed synchronously)
    OrchestratorTransition(
        OrchestratorState.EXECUTE,
        OrchestratorState.COMPLETE,
        "has_pending_approvals",
        "complete_waiting_approval"
    ),
]


class Orchestrator:
    """
    Multi-agent orchestration with explicit role contracts.

    Key properties:
    1. State machine is deterministic - transitions based on explicit conditions
    2. Critic can force return to INVESTIGATE state (evidence quality gate)
    3. PolicyJudge can veto actions (policy compliance gate)
    4. Only trace_events, actions, evidence_ids are persisted - no chain-of-thought
    """

    def __init__(
        self,
        case_id: UUID,
        session: Optional[Session] = None,
    ):
        self.case_id = case_id
        self.state = OrchestratorState.INIT
        self.belief_state = BeliefState(case_id=case_id)

        # Session management
        self._session = session
        self._owns_session = session is None

        # Load case info to set airport_icao
        self._load_case_info()

        # Role verdict storage
        self.critic_verdict: Optional[str] = None
        self.policy_verdict: Optional[str] = None
        self.risk_assessment: Optional[Dict[str, Any]] = None
        self.proposed_actions: List[Dict[str, Any]] = []
        self.communications: List[Dict[str, Any]] = []

        # Loop prevention
        self._investigation_count = 0
        self._max_investigations = 2  # Max 2 investigation rounds

        # Last packet for streaming access
        self.last_packet: Optional[Dict[str, Any]] = None

        # Role agents (lazy initialized)
        self._investigator = None
        self._risk_quant = None
        self._policy_judge = None
        self._critic = None
        self._comms = None
        self._executor = None

        # Playbook context (for guided resolution)
        self._playbook_id: Optional[str] = None
        self._playbook_pattern: Optional[Dict[str, Any]] = None
        self._playbook_action_template: Optional[Dict[str, Any]] = None
        self._playbook_used: bool = False

    def _load_case_info(self):
        """Load case info and set airport_icao on belief_state."""
        result = self.session.execute(
            text("""
                SELECT scope FROM "case" WHERE id = :case_id
            """),
            {"case_id": self.case_id}
        )
        row = result.fetchone()
        if row and row[0]:
            scope = row[0] if isinstance(row[0], dict) else json.loads(row[0])
            self.belief_state.airport_icao = scope.get("airport")

    @property
    def session(self) -> Session:
        if self._session is None:
            self._session = SessionLocal()
        return self._session

    def close(self):
        if self._owns_session and self._session is not None:
            self._session.close()
            self._session = None

    def set_playbook_context(self, playbook_id: str):
        """
        Set playbook context for guided resolution.

        Args:
            playbook_id: ID of playbook to use for guidance
        """
        from ..replay.playbooks import PlaybookManager

        # Store playbook ID for use during execution
        self._playbook_id = playbook_id

        # Load playbook patterns and action templates
        try:
            manager = PlaybookManager(self.session)
            playbook = manager.get_playbook(UUID(playbook_id))
            if playbook:
                self._playbook_pattern = playbook.get("pattern", {})
                self._playbook_action_template = playbook.get("action_template", {})
        except Exception:
            # Playbook loading is optional - continue without it
            pass

    def run(self) -> Dict[str, Any]:
        """
        Run the state machine until COMPLETE.

        Returns:
            Decision packet dict
        """
        logger.info(
            "orchestrator_started",
            case_id=str(self.case_id),
            initial_state=self.state.value,
        )
        try:
            while self.state != OrchestratorState.COMPLETE:
                transition = self._find_valid_transition()
                if transition is None:
                    # No valid transition - force complete
                    self._log_trace("GUARDRAIL_FAIL", "no_valid_transition", {
                        "reason": "No valid transition found from current state",
                        "from_state": self.state.value,
                    })
                    break

                # Log state exit with transition reason
                self._log_trace("STATE_EXIT", self.state.value, {
                    "transition_to": transition.to_state.value,
                    "condition": transition.condition,
                    "handler": transition.handler,
                })

                # Execute transition handler
                handler = getattr(self, transition.handler)
                handler()

                # Log state enter with findings from handler
                context = {
                    "transition_from": self.state.value,
                    "handler": transition.handler,
                    "condition_met": transition.condition,
                }
                # Add agent-specific findings
                if transition.to_state == OrchestratorState.QUANTIFY_RISK and self.risk_assessment:
                    context["risk_level"] = self.risk_assessment.get("risk_level")
                    context["recommended_posture"] = self.risk_assessment.get("recommended_posture")
                    context["confidence"] = self.risk_assessment.get("confidence")
                if transition.to_state == OrchestratorState.CRITIQUE:
                    context["critic_verdict"] = self.critic_verdict
                if transition.to_state == OrchestratorState.EVALUATE_POLICY:
                    context["policy_verdict"] = self.policy_verdict
                if transition.to_state == OrchestratorState.PLAN_ACTIONS and self.proposed_actions:
                    context["actions_planned"] = len(self.proposed_actions)
                    context["action_types"] = [a.get("type") for a in self.proposed_actions]

                self._log_trace("STATE_ENTER", transition.to_state.value, context)
                self.state = transition.to_state

                # Increment iteration
                self.belief_state.increment_iteration()

            packet = self._build_packet()
            self.last_packet = packet  # Store for streaming endpoint

            logger.info(
                "orchestrator_completed",
                case_id=str(self.case_id),
                final_state=self.state.value,
                posture=self.belief_state.current_posture.value if self.belief_state.current_posture else None,
                evidence_count=self.belief_state.evidence_count,
                iterations=self.belief_state.iterations,
            )
            return packet

        finally:
            self.close()

    def run_with_progress(self):
        """
        Run the state machine with progress yields for streaming.

        Yields:
            Progress event dicts with state transitions
        """
        try:
            while self.state != OrchestratorState.COMPLETE:
                transition = self._find_valid_transition()
                if transition is None:
                    # No valid transition - force complete
                    self._log_trace("GUARDRAIL_FAIL", "no_valid_transition", {
                        "reason": "No valid transition found from current state",
                        "from_state": self.state.value,
                    })
                    yield {
                        "event": "guardrail_fail",
                        "state": self.state.value,
                        "reason": "no_valid_transition"
                    }
                    break

                # Yield state transition event with description
                description = self._get_state_description(transition.to_state.value, "STATE_ENTER")
                yield {
                    "event": "state_transition",
                    "from_state": self.state.value,
                    "to_state": transition.to_state.value,
                    "handler": transition.handler,
                    "condition": transition.condition,
                    "description": description,
                    "evidence_count": self.belief_state.evidence_count,
                    "uncertainty_count": self.belief_state.uncertainty_count,
                    "iteration": self.belief_state.iterations,
                }

                # Log state exit with transition reason
                self._log_trace("STATE_EXIT", self.state.value, {
                    "transition_to": transition.to_state.value,
                    "condition": transition.condition,
                    "handler": transition.handler,
                })

                # Execute transition handler
                handler = getattr(self, transition.handler)
                handler()

                # Log state enter with findings from handler
                context = {
                    "transition_from": self.state.value,
                    "handler": transition.handler,
                    "condition_met": transition.condition,
                }
                # Add agent-specific findings
                if transition.to_state == OrchestratorState.QUANTIFY_RISK and self.risk_assessment:
                    context["risk_level"] = self.risk_assessment.get("risk_level")
                    context["recommended_posture"] = self.risk_assessment.get("recommended_posture")
                    context["confidence"] = self.risk_assessment.get("confidence")
                if transition.to_state == OrchestratorState.CRITIQUE:
                    context["critic_verdict"] = self.critic_verdict
                if transition.to_state == OrchestratorState.EVALUATE_POLICY:
                    context["policy_verdict"] = self.policy_verdict
                if transition.to_state == OrchestratorState.PLAN_ACTIONS and self.proposed_actions:
                    context["actions_planned"] = len(self.proposed_actions)
                    context["action_types"] = [a.get("type") for a in self.proposed_actions]

                self._log_trace("STATE_ENTER", transition.to_state.value, context)
                self.state = transition.to_state

                # Increment iteration
                self.belief_state.increment_iteration()

                # Yield progress after handler execution with description
                progress_description = self._get_state_description(self.state.value, "STATE_ENTER")
                progress = {
                    "event": "progress",
                    "state": self.state.value,
                    "description": progress_description,
                    "evidence_count": self.belief_state.evidence_count,
                    "claim_count": len(self.belief_state.claim_ids),
                    "uncertainty_count": self.belief_state.uncertainty_count,
                    "contradiction_count": self.belief_state.contradiction_count,
                    "current_posture": self.belief_state.current_posture.value if self.belief_state.current_posture else None,
                }

                # Add risk assessment info if available
                if self.risk_assessment:
                    progress["risk_level"] = self.risk_assessment.get("risk_level")
                    progress["recommended_posture"] = self.risk_assessment.get("recommended_posture")
                    progress["confidence"] = self.risk_assessment.get("confidence")

                yield progress

            # Build final packet
            packet = self._build_packet()
            self.last_packet = packet

        except Exception as e:
            yield {
                "event": "error",
                "state": self.state.value,
                "error": str(e)
            }
        finally:
            self.close()

    def _find_valid_transition(self) -> Optional[OrchestratorTransition]:
        """Find first valid transition from current state."""
        for t in TRANSITIONS:
            if t.from_state == self.state and self._eval_condition(t.condition):
                return t
        return None

    def _eval_condition(self, condition: str) -> bool:
        """
        Evaluate transition condition.

        Conditions are simple predicates on orchestrator state.
        """
        # Map condition strings to actual checks
        # NOTE: Re-investigation loops are limited to prevent infinite hangs
        can_reinvestigate = self._investigation_count < self._max_investigations

        conditions = {
            "always": True,
            "has_blocking_missing_evidence": self._has_blocking_missing_evidence(),
            "evidence_count > 0 and no_blocking_missing_evidence": (
                self.belief_state.evidence_count > 0 and
                not self._has_blocking_missing_evidence()
            ),
            "uncertainty_count > 0 and budget_remaining": (
                self.belief_state.uncertainty_count > 0 and
                self.belief_state.budget_remaining and
                can_reinvestigate  # Prevent infinite loops
            ),
            "risk_assessment_complete": self.risk_assessment is not None,
            "critic_verdict == 'INSUFFICIENT_EVIDENCE'": (
                self.critic_verdict == "INSUFFICIENT_EVIDENCE" and
                can_reinvestigate  # Prevent infinite loops
            ),
            "critic_verdict == 'ACCEPTABLE'": (
                self.critic_verdict == "ACCEPTABLE" or
                not can_reinvestigate  # Force acceptance if max loops reached
            ),
            "policy_verdict == 'COMPLIANT'": (
                self.policy_verdict == "COMPLIANT"
            ),
            "policy_verdict == 'NEEDS_EVIDENCE'": (
                self.policy_verdict == "NEEDS_EVIDENCE" and
                can_reinvestigate  # Prevent infinite loops
            ),
            "policy_verdict == 'BLOCKED'": (
                self.policy_verdict == "BLOCKED"
            ),
            "proposed_actions_count > 0 and any_requires_notification": (
                len(self.proposed_actions) > 0 and
                any(a.get("requires_notification") for a in self.proposed_actions)
            ),
            "proposed_actions_count > 0 and not any_requires_notification": (
                len(self.proposed_actions) > 0 and
                not any(a.get("requires_notification") for a in self.proposed_actions)
            ),
            "proposed_actions_count == 0": len(self.proposed_actions) == 0,
            "communications_drafted": len(self.communications) > 0,
            "all_actions_terminal": self._all_actions_terminal(),
            "has_pending_approvals": self._has_pending_approvals(),
        }
        return conditions.get(condition, False)

    def complete_missing_evidence_blocked(self):
        """Complete case as blocked due to missing evidence."""
        self.belief_state.stop_condition = StopCondition.BLOCKED

        # Set case status to BLOCKED so the packet builder exposes missing evidence requests
        self.session.execute(
            text('UPDATE "case" SET status = :status WHERE id = :id'),
            {"id": self.case_id, "status": "BLOCKED"}
        )
        self.session.commit()

        # Summarize missing evidence for trace/debug
        result = self.session.execute(
            text("""
                SELECT source_system, request_type, reason, criticality
                FROM missing_evidence_request
                WHERE case_id = :case_id AND resolved_at IS NULL
                ORDER BY criticality, created_at
            """),
            {"case_id": self.case_id}
        )
        missing = [
            {
                "source_system": r[0],
                "request_type": r[1],
                "reason": r[2],
                "criticality": r[3],
            }
            for r in result
        ]

        blocking = [m for m in missing if m.get("criticality") == "BLOCKING"]
        self._log_trace("BLOCKED", "missing_evidence", {
            "reason": "Case blocked due to missing evidence",
            "blocking_count": len(blocking),
            "missing_evidence_requests": missing,
            "description": "Case blocked - required evidence sources unavailable; re-run once evidence is available.",
        })

    def _has_blocking_missing_evidence(self) -> bool:
        """Check if there's blocking missing evidence."""
        result = self.session.execute(
            text("""
                SELECT COUNT(*) FROM missing_evidence_request
                WHERE case_id = :case_id
                  AND criticality = 'BLOCKING'
                  AND resolved_at IS NULL
            """),
            {"case_id": self.case_id}
        )
        return result.scalar() > 0

    def _all_actions_terminal(self) -> bool:
        """Check if all actions are in terminal state."""
        result = self.session.execute(
            text("""
                SELECT COUNT(*) FROM action
                WHERE case_id = :case_id
                  AND state NOT IN ('COMPLETED', 'FAILED', 'ROLLED_BACK')
            """),
            {"case_id": self.case_id}
        )
        return result.scalar() == 0

    def _has_pending_approvals(self) -> bool:
        """Check if there are actions awaiting approval."""
        result = self.session.execute(
            text("""
                SELECT COUNT(*) FROM action
                WHERE case_id = :case_id
                  AND state = 'PENDING_APPROVAL'
            """),
            {"case_id": self.case_id}
        )
        return result.scalar() > 0

    def _log_trace(self, event_type: str, state_name: str, context: Optional[Dict[str, Any]] = None):
        """
        Persist trace event with descriptive context.

        ONLY structured data, no chain-of-thought.
        The context parameter adds human-readable descriptions of WHY
        transitions happen and what was discovered.
        """
        from ..db.engine import get_next_trace_seq

        # Get next sequence number from centralized function
        seq = get_next_trace_seq(self.case_id, self.session)

        # Include state name in meta since ref_id is UUID type
        meta = self.belief_state.to_summary()
        meta["state"] = state_name

        # Add descriptive context if provided
        if context:
            meta.update(context)

        # Add state-specific descriptions based on current orchestrator state
        meta["description"] = self._get_state_description(state_name, event_type)

        self.session.execute(
            text("""
                INSERT INTO trace_event
                (id, case_id, seq, event_type, ref_type, meta, created_at)
                VALUES (:id, :case_id, :seq, :event_type, :ref_type, CAST(:meta AS jsonb), :created_at)
            """),
            {
                "id": uuid4(),
                "case_id": self.case_id,
                "seq": seq,
                "event_type": event_type,
                "ref_type": "state",
                "meta": json.dumps(meta),
                "created_at": datetime.now(timezone.utc),
            }
        )
        self.session.commit()

    def _get_state_description(self, state_name: str, event_type: str) -> str:
        """Generate human-readable description for state transitions."""
        if event_type == "STATE_EXIT":
            return f"Exiting {state_name}"

        descriptions = {
            "INIT": "Initializing case investigation",
            "INVESTIGATE": self._describe_investigate(),
            "QUANTIFY_RISK": self._describe_risk(),
            "CRITIQUE": self._describe_critique(),
            "EVALUATE_POLICY": self._describe_policy(),
            "PLAN_ACTIONS": self._describe_plan(),
            "DRAFT_COMMS": "Drafting communications for stakeholders",
            "EXECUTE": self._describe_execute(),
            "COMPLETE": self._describe_complete(),
        }

        return descriptions.get(state_name, f"Processing {state_name}")

    def _describe_investigate(self) -> str:
        """Describe investigation state."""
        sources = ["FAA_NAS", "METAR", "TAF", "NWS_ALERTS", "OPENSKY"]
        evidence_count = self.belief_state.evidence_count
        uncertainty_count = self.belief_state.uncertainty_count

        if evidence_count == 0:
            return f"Gathering evidence from {len(sources)} sources: {', '.join(sources)}"
        else:
            return f"Gathered {evidence_count} evidence records, {uncertainty_count} uncertainties remaining"

    def _describe_risk(self) -> str:
        """Describe risk quantification state."""
        if self.risk_assessment:
            risk_level = self.risk_assessment.get("risk_level", "UNKNOWN")
            posture = self.risk_assessment.get("recommended_posture", "UNKNOWN")
            confidence = self.risk_assessment.get("confidence", 0)
            return f"Risk assessed: {risk_level} → Recommending {posture} (confidence: {int(confidence * 100)}%)"
        return "Analyzing evidence to quantify operational risk"

    def _describe_critique(self) -> str:
        """Describe critique state."""
        if self.critic_verdict:
            if self.critic_verdict == "ACCEPTABLE":
                return "Critic validated evidence quality - proceeding to policy evaluation"
            elif self.critic_verdict == "INSUFFICIENT_EVIDENCE":
                return "Critic found evidence gaps - returning to investigation"
            return f"Critic verdict: {self.critic_verdict}"
        return "Critic validating evidence quality and completeness"

    def _describe_policy(self) -> str:
        """Describe policy evaluation state."""
        if self.policy_verdict:
            if self.policy_verdict == "COMPLIANT":
                return "Policy check passed - proceeding to action planning"
            elif self.policy_verdict == "BLOCKED":
                return "Policy blocked action - case will complete without execution"
            elif self.policy_verdict == "NEEDS_EVIDENCE":
                return "Policy requires additional evidence - returning to investigation"
            return f"Policy verdict: {self.policy_verdict}"
        return "Evaluating recommended actions against governance policies"

    def _describe_plan(self) -> str:
        """Describe action planning state."""
        if self.proposed_actions:
            action_types = [a.get("type", "UNKNOWN") for a in self.proposed_actions]
            return f"Planned {len(self.proposed_actions)} actions: {', '.join(action_types)}"
        return "Planning recommended actions based on risk assessment"

    def _describe_execute(self) -> str:
        """Describe execution state."""
        if self.proposed_actions:
            posture_actions = [a for a in self.proposed_actions if a.get("type") == "SET_POSTURE"]
            if posture_actions:
                posture = posture_actions[0].get("args", {}).get("posture", "UNKNOWN")
                return f"Executing posture directive: {posture}"
            return f"Executing {len(self.proposed_actions)} approved actions"
        return "Executing approved actions"

    def _describe_complete(self) -> str:
        """Describe completion state."""
        if self.belief_state.stop_condition:
            if self.belief_state.stop_condition.value == "BLOCKED":
                return "Case completed - BLOCKED by policy or missing evidence"
            elif self.belief_state.stop_condition.value == "MET":
                posture = self.belief_state.current_posture
                return f"Case resolved with posture: {posture.value if posture else 'UNKNOWN'}"
        return "Case processing complete"

    # ============================================================
    # TRANSITION HANDLERS
    # ============================================================

    def start_investigation(self):
        """Start investigation phase."""
        from .roles.investigator import InvestigatorAgent

        # Track investigation count to prevent infinite loops
        self._investigation_count += 1

        # Auto-match playbook if not already set
        if self._playbook_id is None:
            self._auto_match_playbook()

        if self._investigator is None:
            self._investigator = InvestigatorAgent(self.case_id, self.session)

        self._investigator.investigate(self.belief_state)

    def _auto_match_playbook(self):
        """Automatically match a playbook based on case type and scope."""
        from ..replay.playbooks import PlaybookManager

        try:
            # Get case type and scope
            result = self.session.execute(
                text("""
                    SELECT case_type, scope FROM "case" WHERE id = :case_id
                """),
                {"case_id": self.case_id}
            )
            row = result.fetchone()
            if not row:
                return

            case_type = row[0]
            scope = row[1] if isinstance(row[1], dict) else json.loads(row[1]) if row[1] else {}

            # Find matching playbooks
            manager = PlaybookManager(self.session)
            matches = manager.find_matching(case_type, scope, limit=1)

            if matches and matches[0].get("match_score", 0) > 0.5:
                best_match = matches[0]
                self._playbook_id = str(best_match["playbook_id"])
                self._playbook_pattern = best_match.get("pattern", {})
                self._playbook_action_template = best_match.get("action_template", {})

                # Log playbook match
                self._log_trace("TOOL_RESULT", "playbook_matched", {
                    "playbook_id": self._playbook_id,
                    "playbook_name": best_match.get("name"),
                    "match_score": best_match.get("match_score"),
                    "description": f"Matched playbook: {best_match.get('name')} (score: {best_match.get('match_score'):.2f})",
                })

        except Exception as e:
            # Playbook matching is optional - continue without it
            self._log_trace("TOOL_RESULT", "playbook_match_failed", {
                "error": str(e),
            })

    def continue_investigation(self):
        """Continue investigation with remaining uncertainties."""
        # Track investigation count to prevent infinite loops
        self._investigation_count += 1

        if self._investigator is not None:
            self._investigator.investigate_uncertainties(
                self.belief_state,
                self.belief_state.open_uncertainties
            )

    def quantify_risk(self):
        """Quantify risk using RiskQuantAgent."""
        from .roles.risk_quant import RiskQuantAgent

        if self._risk_quant is None:
            self._risk_quant = RiskQuantAgent(self.case_id, self.session)

        self.risk_assessment = self._risk_quant.assess_risk(self.belief_state)

        # Update belief_state posture from LLM recommendation
        posture_str = self.risk_assessment.get("recommended_posture", "HOLD")
        try:
            self.belief_state.current_posture = Posture[posture_str]
        except KeyError:
            self.belief_state.current_posture = Posture.HOLD

    def run_critic(self):
        """Run critic agent to validate evidence quality."""
        from .roles.critic import CriticAgent

        if self._critic is None:
            self._critic = CriticAgent(self.case_id, self.session)

        self.critic_verdict = self._critic.critique(
            self.belief_state,
            self.risk_assessment
        )

    def force_reinvestigation(self):
        """Forced return to investigation by critic or policy judge.

        This ACTUALLY re-investigates - not just logs. It calls the investigator
        to gather more evidence or resolve remaining uncertainties.
        """
        from .roles.investigator import InvestigatorAgent

        # Track investigation count to prevent infinite loops
        self._investigation_count += 1

        reason = "Evidence quality insufficient" if self.critic_verdict == "INSUFFICIENT_EVIDENCE" else "Policy requires more evidence"
        self._log_trace("HANDOFF", "reinvestigation_required", {
            "reason": reason,
            "critic_verdict": self.critic_verdict,
            "policy_verdict": self.policy_verdict,
            "investigation_count": self._investigation_count,
            "description": f"Returning to investigation: {reason}",
        })

        # Actually perform reinvestigation
        if self._investigator is None:
            self._investigator = InvestigatorAgent(self.case_id, self.session)

        # If there are open uncertainties, investigate them
        if self.belief_state.open_uncertainties:
            self._investigator.investigate_uncertainties(
                self.belief_state,
                self.belief_state.open_uncertainties
            )
        else:
            # No specific uncertainties - do a fresh full investigation
            # This happens when critic rejects quality but no explicit gaps
            self._investigator.investigate(self.belief_state)

        # Reset verdicts to force re-evaluation
        self.critic_verdict = None
        self.risk_assessment = None

    def evaluate_policy(self):
        """Evaluate policies using PolicyJudgeAgent."""
        from .roles.policy_judge import PolicyJudgeAgent

        if self._policy_judge is None:
            self._policy_judge = PolicyJudgeAgent(self.case_id, self.session)

        self.policy_verdict = self._policy_judge.evaluate(
            self.belief_state,
            self.risk_assessment,
            self.proposed_actions
        )

    def plan_actions(self):
        """Plan actions using beam search planner, guided by playbook if available."""
        from .planner.beam_search import plan_actions

        # Get base actions from planner
        base_actions = plan_actions(
            self.belief_state,
            self.risk_assessment
        )

        # Apply playbook guidance if available
        if self._playbook_action_template:
            self.proposed_actions = self._apply_playbook_guidance(base_actions)
            self._playbook_used = True

            # Log playbook influence
            self._log_trace("TOOL_RESULT", "playbook_applied", {
                "playbook_id": self._playbook_id,
                "original_actions": len(base_actions),
                "guided_actions": len(self.proposed_actions),
                "description": f"Playbook guidance applied: {len(base_actions)} → {len(self.proposed_actions)} actions",
            })
        else:
            self.proposed_actions = base_actions

    def _apply_playbook_guidance(
        self,
        base_actions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Apply playbook action template to guide action selection.

        The playbook can:
        1. Suggest additional actions from its template
        2. Modify action parameters based on learned patterns
        3. Reorder actions based on successful past sequences
        """
        if not self._playbook_action_template:
            return base_actions

        template_sequence = self._playbook_action_template.get("action_sequence", [])
        if not template_sequence:
            return base_actions

        guided_actions = []
        base_action_types = {a.get("type") for a in base_actions}

        # First, include base actions that align with template
        for template_action in template_sequence:
            template_type = template_action.get("type")

            # Find matching base action
            matching = [a for a in base_actions if a.get("type") == template_type]
            if matching:
                # Use base action but can enhance with template args
                action = matching[0].copy()
                # Merge template args (base takes precedence)
                template_args = template_action.get("args", {})
                action_args = action.get("args", {})
                merged_args = {**template_args, **action_args}
                action["args"] = merged_args
                action["playbook_guided"] = True
                guided_actions.append(action)

        # Include any base actions not in template (new situations)
        guided_types = {a.get("type") for a in guided_actions}
        for action in base_actions:
            if action.get("type") not in guided_types:
                action["playbook_guided"] = False
                guided_actions.append(action)

        return guided_actions

    def draft_communications(self):
        """Draft communications using CommsAgent."""
        from .roles.comms import CommsAgent

        if self._comms is None:
            self._comms = CommsAgent(self.case_id, self.session)

        self.communications = self._comms.draft_communications(
            self.belief_state,
            self.proposed_actions
        )

    def execute_actions(self):
        """Execute approved actions using ExecutorAgent."""
        from .roles.executor import ExecutorAgent

        if self._executor is None:
            self._executor = ExecutorAgent(self.case_id, self.session)

        # Pass belief state context to executor for webhook payloads
        # This allows webhooks to include confidence, evidence count, risk level
        # Calculate confidence from hypotheses average, or default to 0.5
        confidence = 0.5
        if self.belief_state.hypotheses:
            confidence = sum(h.confidence for h in self.belief_state.hypotheses) / len(self.belief_state.hypotheses)

        self._executor.set_context(
            confidence=confidence,
            evidence_count=self.belief_state.evidence_count,
            risk_level=self.risk_assessment.get("risk_level") if self.risk_assessment else None,
            previous_posture=None,  # Could track previous posture if needed
        )

        self._executor.execute(self.proposed_actions)

    def complete_blocked(self):
        """Complete case as blocked by policy."""
        self.belief_state.stop_condition = StopCondition.BLOCKED

        # Update case status to BLOCKED in database
        self.session.execute(
            text('UPDATE "case" SET status = :status WHERE id = :id'),
            {"id": self.case_id, "status": "BLOCKED"}
        )
        self.session.commit()

        self._log_trace("BLOCKED", "policy_blocked", {
            "reason": "Policy blocked execution",
            "policy_verdict": self.policy_verdict,
            "description": "Case blocked by policy - cannot proceed with proposed actions",
        })

    def complete_waiting_approval(self):
        """
        Complete orchestrator while actions await approval.

        Case status is set to BLOCKED (waiting for external approval).
        When approval is granted via API, the action is executed and
        case status is updated to RESOLVED if all actions complete.
        """
        self.belief_state.stop_condition = StopCondition.BLOCKED

        # Update case status to BLOCKED
        self.session.execute(
            text('UPDATE "case" SET status = :status WHERE id = :id'),
            {"id": self.case_id, "status": "BLOCKED"}
        )
        self.session.commit()

        # Count pending approvals
        result = self.session.execute(
            text("""
                SELECT COUNT(*) FROM action
                WHERE case_id = :case_id AND state = 'PENDING_APPROVAL'
            """),
            {"case_id": self.case_id}
        )
        pending_count = result.scalar()

        self._log_trace("BLOCKED", "waiting_approval", {
            "reason": "Actions require manual approval",
            "pending_approval_count": pending_count,
            "description": f"Waiting for approval of {pending_count} action(s). Case will resume after approval via API.",
        })

    def complete_case(self):
        """Complete the case successfully."""
        self.belief_state.stop_condition = StopCondition.MET

        # Update case status to RESOLVED in database
        self.session.execute(
            text('UPDATE "case" SET status = :status WHERE id = :id'),
            {"id": self.case_id, "status": "RESOLVED"}
        )
        self.session.commit()

        # Record playbook usage if one was used
        if self._playbook_used and self._playbook_id:
            self._record_playbook_success()

    def complete_no_actions(self):
        """
        Complete the case when no actions are needed.

        This happens when the planner determines that the current posture
        is already correct and no interventions are required.
        """
        self.belief_state.stop_condition = StopCondition.MET

        # Update case status to RESOLVED in database
        self.session.execute(
            text('UPDATE "case" SET status = :status WHERE id = :id'),
            {"id": self.case_id, "status": "RESOLVED"}
        )
        self.session.commit()

        self._log_trace("STATE_ENTER", "no_actions_needed", {
            "reason": "Planner determined no actions required",
            "current_posture": self.belief_state.current_posture.value if self.belief_state.current_posture else None,
            "description": "Case resolved - current posture is appropriate, no interventions needed",
        })

        # Record playbook usage if one was used
        if self._playbook_used and self._playbook_id:
            self._record_playbook_success()

    def _record_playbook_success(self):
        """Record successful playbook usage for learning."""
        from ..replay.playbooks import PlaybookManager

        try:
            manager = PlaybookManager(self.session)

            # Determine success based on case outcome
            # Success = case completed without being blocked
            success = self.belief_state.stop_condition == StopCondition.MET

            manager.record_usage(
                playbook_id=UUID(self._playbook_id),
                case_id=self.case_id,
                success=success
            )

            self._log_trace("TOOL_RESULT", "playbook_usage_recorded", {
                "playbook_id": self._playbook_id,
                "success": success,
                "description": f"Playbook usage recorded: {'success' if success else 'failed'}",
            })

        except Exception as e:
            # Non-critical - don't fail the case
            self._log_trace("TOOL_RESULT", "playbook_record_failed", {
                "error": str(e),
            })

    def _build_packet(self) -> Dict[str, Any]:
        """Build decision packet from orchestration results."""
        from ..packets.builder import build_decision_packet

        return build_decision_packet(
            case_id=self.case_id,
            belief_state=self.belief_state,
            risk_assessment=self.risk_assessment,
            proposed_actions=self.proposed_actions,
            session=self.session,
        )
