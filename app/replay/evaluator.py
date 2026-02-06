# app/replay/evaluator.py
"""
Playbook evaluation for replay learning.

Evaluates playbook applicability and quality.
"""

from typing import Dict, Any, Optional, List
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import SessionLocal
from .playbooks import PlaybookManager


class PlaybookEvaluator:
    """
    Evaluates playbooks for applicability.

    Used to:
    - Decide if a playbook should be applied
    - Compare playbook predictions to actual outcomes
    - Improve playbook selection
    """

    def __init__(self, session: Optional[Session] = None):
        self._session = session
        self._owns_session = session is None
        self.playbook_manager = PlaybookManager(session)

    @property
    def session(self) -> Session:
        if self._session is None:
            self._session = SessionLocal()
        return self._session

    def close(self):
        if self._owns_session and self._session is not None:
            self._session.close()
            self._session = None

    def evaluate_match(
        self,
        playbook_id: UUID,
        case_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Evaluate how well a playbook matches a case context.

        Args:
            playbook_id: Playbook ID
            case_context: Current case context

        Returns:
            Evaluation result dict
        """
        playbook = self.playbook_manager.get_playbook(playbook_id)
        if not playbook:
            return {"match": False, "reason": "Playbook not found"}

        pattern = playbook.get("pattern", {})

        # Check case type
        if pattern.get("case_type") != case_context.get("case_type"):
            return {
                "match": False,
                "reason": "Case type mismatch",
                "expected": pattern.get("case_type"),
                "actual": case_context.get("case_type"),
            }

        # Check scope overlap
        scope_match = self._evaluate_scope_match(
            pattern.get("scope_keys", []),
            case_context.get("scope", {}),
        )

        # Check evidence availability
        evidence_match = self._evaluate_evidence_match(
            pattern.get("evidence_sources", []),
            case_context.get("available_sources", []),
        )

        # Compute overall score
        overall_score = (scope_match["score"] + evidence_match["score"]) / 2

        return {
            "match": overall_score > 0.5,
            "overall_score": overall_score,
            "scope_match": scope_match,
            "evidence_match": evidence_match,
            "playbook_stats": playbook.get("stats", {}),
            "recommended_actions": playbook.get("action_template", {}).get("action_sequence", []),
        }

    def should_use_playbook(
        self,
        case_type: str,
        case_count: int,
    ) -> bool:
        """
        Determine if we should try to use a playbook.

        Replay gate: After 3 cases, 4th must retrieve playbook.

        Args:
            case_type: Type of case
            case_count: Number of cases processed

        Returns:
            True if should try to use playbook
        """
        # After 3 cases of same type, try playbook
        if case_count >= 3:
            # Check if playbooks exist for this type
            playbooks = self.playbook_manager.find_matching(case_type, {}, limit=1)
            return len(playbooks) > 0

        return False

    def evaluate_outcome(
        self,
        playbook_id: UUID,
        case_id: UUID,
        actual_actions: List[str],
        success: bool,
    ) -> Dict[str, Any]:
        """
        Evaluate playbook outcome after use.

        Args:
            playbook_id: Playbook that was used
            case_id: Case it was used for
            actual_actions: Actions that were actually taken
            success: Whether case was successful

        Returns:
            Evaluation result
        """
        playbook = self.playbook_manager.get_playbook(playbook_id)
        if not playbook:
            return {"error": "Playbook not found"}

        expected_actions = playbook.get("action_template", {}).get("action_sequence", [])

        # Compare actions
        action_match = self._compare_actions(expected_actions, actual_actions)

        # Record usage
        self.playbook_manager.record_usage(playbook_id, case_id, success)

        return {
            "playbook_id": str(playbook_id),
            "case_id": str(case_id),
            "success": success,
            "action_match": action_match,
            "expected_actions": expected_actions,
            "actual_actions": actual_actions,
        }

    def _evaluate_scope_match(
        self,
        pattern_keys: List[str],
        scope: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Evaluate scope match."""
        if not pattern_keys:
            return {"score": 1.0, "detail": "No scope constraints"}

        present = sum(1 for k in pattern_keys if k in scope)
        score = present / len(pattern_keys)

        return {
            "score": score,
            "required_keys": pattern_keys,
            "present_keys": [k for k in pattern_keys if k in scope],
            "missing_keys": [k for k in pattern_keys if k not in scope],
        }

    def _evaluate_evidence_match(
        self,
        required_sources: List[str],
        available_sources: List[str],
    ) -> Dict[str, Any]:
        """Evaluate evidence source match."""
        if not required_sources:
            return {"score": 1.0, "detail": "No evidence requirements"}

        available_set = set(available_sources)
        present = sum(1 for s in required_sources if s in available_set)
        score = present / len(required_sources)

        return {
            "score": score,
            "required_sources": required_sources,
            "available_sources": available_sources,
            "missing_sources": [s for s in required_sources if s not in available_set],
        }

    def _compare_actions(
        self,
        expected: List[str],
        actual: List[str],
    ) -> Dict[str, Any]:
        """Compare expected vs actual actions."""
        if not expected and not actual:
            return {"score": 1.0, "detail": "No actions expected or taken"}

        if not expected:
            return {"score": 0.5, "detail": "Unexpected actions taken"}

        if not actual:
            return {"score": 0.0, "detail": "Expected actions not taken"}

        # Check overlap
        expected_set = set(expected)
        actual_set = set(actual)

        overlap = len(expected_set & actual_set)
        union = len(expected_set | actual_set)

        score = overlap / union if union > 0 else 0

        return {
            "score": score,
            "expected": expected,
            "actual": actual,
            "overlap": list(expected_set & actual_set),
            "unexpected": list(actual_set - expected_set),
            "missing": list(expected_set - actual_set),
        }


def evaluate_playbook_match(
    playbook_id: UUID,
    case_context: Dict[str, Any],
    session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Convenience function to evaluate playbook match.

    Args:
        playbook_id: Playbook ID
        case_context: Case context
        session: Optional database session

    Returns:
        Evaluation result
    """
    evaluator = PlaybookEvaluator(session)
    try:
        return evaluator.evaluate_match(playbook_id, case_context)
    finally:
        if session is None:
            evaluator.close()
