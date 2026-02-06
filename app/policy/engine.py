# app/policy/engine.py
"""
Policy evaluation engine.
"""

from typing import List, Dict, Any, Optional, Tuple
from uuid import UUID
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from .models import Policy, PolicyCondition, PolicyEffect


class PolicyEngine:
    """
    Evaluates policies against context.
    """

    def __init__(self, session: Session):
        self.session = session

    def load_active_policies(self) -> List[Policy]:
        """Load all active policies."""
        now = datetime.now(timezone.utc)

        result = self.session.execute(
            text("""
                SELECT id, type, text, conditions, effects, effective_from, effective_to
                FROM policy
                WHERE effective_from <= :now
                  AND (effective_to IS NULL OR effective_to > :now)
                ORDER BY type
            """),
            {"now": now}
        )

        policies = []
        for row in result:
            conditions = self._parse_conditions(row[3])
            effects = self._parse_effects(row[4])

            policies.append(Policy(
                id=row[0],
                type=row[1],
                text=row[2],
                conditions=conditions,
                effects=effects,
                effective_from=row[5],
                effective_to=row[6],
            ))

        return policies

    def evaluate(
        self,
        context: Dict[str, Any],
        policies: Optional[List[Policy]] = None,
    ) -> List[Tuple[Policy, PolicyEffect]]:
        """
        Evaluate policies against context.

        Args:
            context: Current context (risk_level, action_type, etc.)
            policies: Optional list of policies (loads from DB if not provided)

        Returns:
            List of (policy, triggered_effect) tuples
        """
        if policies is None:
            policies = self.load_active_policies()

        triggered = []

        for policy in policies:
            if self._conditions_match(policy.conditions, context):
                for effect in policy.effects:
                    triggered.append((policy, effect))

        return triggered

    def _conditions_match(
        self,
        conditions: List[PolicyCondition],
        context: Dict[str, Any],
    ) -> bool:
        """Check if all conditions match context."""
        for condition in conditions:
            if not self._condition_matches(condition, context):
                return False
        return True

    def _condition_matches(
        self,
        condition: PolicyCondition,
        context: Dict[str, Any],
    ) -> bool:
        """Check if single condition matches context."""
        value = context.get(condition.field)

        if condition.operator == "==":
            return value == condition.value
        elif condition.operator == "!=":
            return value != condition.value
        elif condition.operator == "in":
            return value in condition.value
        elif condition.operator == "not_in":
            return value not in condition.value
        elif condition.operator == ">":
            return value is not None and value > condition.value
        elif condition.operator == ">=":
            return value is not None and value >= condition.value
        elif condition.operator == "<":
            return value is not None and value < condition.value
        elif condition.operator == "<=":
            return value is not None and value <= condition.value
        elif condition.operator == "exists":
            return value is not None
        elif condition.operator == "not_exists":
            return value is None

        return False

    def _parse_conditions(self, conditions_json: Dict) -> List[PolicyCondition]:
        """Parse conditions from JSON."""
        if not conditions_json:
            return []

        conditions = []
        for field, spec in conditions_json.items():
            if isinstance(spec, dict):
                operator = spec.get("op", "==")
                value = spec.get("value")
            else:
                operator = "=="
                value = spec

            conditions.append(PolicyCondition(
                field=field,
                operator=operator,
                value=value,
            ))

        return conditions

    def _parse_effects(self, effects_json: Dict) -> List[PolicyEffect]:
        """Parse effects from JSON."""
        if not effects_json:
            return []

        effects = []
        action = effects_json.get("action")
        if action:
            effects.append(PolicyEffect(
                action=action,
                params=effects_json.get("params"),
            ))

        return effects


def evaluate_policies(
    context: Dict[str, Any],
    session: Session,
) -> Dict[str, Any]:
    """
    Convenience function to evaluate policies.

    Args:
        context: Evaluation context
        session: Database session

    Returns:
        Evaluation result dict
    """
    engine = PolicyEngine(session)
    triggered = engine.evaluate(context)

    # Summarize results
    requires_approval = any(
        effect.action == "require_approval"
        for _, effect in triggered
    )

    blocked = any(
        effect.action == "block"
        for _, effect in triggered
    )

    warnings = [
        policy.text
        for policy, effect in triggered
        if effect.action == "warn"
    ]

    return {
        "requires_approval": requires_approval,
        "blocked": blocked,
        "warnings": warnings,
        "triggered_policies": [
            {"policy_id": str(p.id), "policy_text": p.text, "effect": e.action}
            for p, e in triggered
        ],
    }
