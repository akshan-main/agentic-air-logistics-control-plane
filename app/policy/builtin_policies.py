# app/policy/builtin_policies.py
"""
Built-in policies for governance.
"""

from typing import List, Dict, Any
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session


# ============================================================
# SINGLE SOURCE OF TRUTH for all governance policies.
#
# Both the SQL seed (005_seed_policies.sql) and the PolicyJudge
# runtime seeder reference this list. To add a policy, add it
# HERE and in the SQL seed. The `text` field is the unique key.
# ============================================================
BUILTIN_POLICIES: List[Dict[str, Any]] = [
    # --- Evidence & contradiction policies ---
    {
        "id": "builtin_contradiction_resolution",
        "type": "evidence_requirement",
        "text": "Open contradictions require evidence resolution before posture decision",
        "conditions": {"has_contradictions": True},
        "effects": {"action": "needs_evidence", "description": "Resolve contradicting signals before proceeding"},
    },
    {
        "id": "builtin_min_evidence",
        "type": "evidence_requirement",
        "text": "Posture changes require at least 2 evidence sources",
        "conditions": {"min_evidence": 2},
        "effects": {"action": "allow", "description": "Sufficient evidence for posture decision"},
    },
    {
        "id": "builtin_shipment_booking",
        "type": "evidence_requirement",
        "text": "Shipment-level actions require booking evidence",
        "conditions": {"action_type": "shipment"},
        "effects": {"action": "block_without_booking", "description": "Cannot modify shipments without booking data"},
    },
    {
        "id": "builtin_contradiction_restrict",
        "type": "posture_constraint",
        "text": "Open contradictions with stale evidence require RESTRICT posture",
        "conditions": {
            "has_contradictions": True,
            "has_stale_evidence": True,
            "proposed_posture": "ACCEPT",
        },
        "effects": {"action": "block", "params": {"reason": "Cannot ACCEPT with open contradictions"}},
    },

    # --- Approval thresholds ---
    {
        "id": "builtin_high_risk_approval",
        "type": "approval_requirement",
        "text": "HIGH or CRITICAL risk actions require human approval",
        "conditions": {"risk_level": ["HIGH", "CRITICAL"]},
        "effects": {"action": "requires_approval", "description": "Escalate to duty manager for approval"},
    },
    {
        "id": "builtin_premium_sla_approval",
        "type": "approval_requirement",
        "text": "Premium SLA posture changes within 48h require approval",
        "conditions": {
            "service_tier": "PREMIUM",
            "hours_until_deadline": {"op": "<", "value": 48},
            "action_type": "SET_POSTURE",
        },
        "effects": {"action": "require_approval"},
    },
    {
        "id": "builtin_cost_threshold_approval",
        "type": "approval_requirement",
        "text": "Actions with cost exposure above $10,000 require approval",
        "conditions": {"estimated_cost": {"op": ">", "value": 10000}},
        "effects": {"action": "require_approval"},
    },

    # --- Risk-posture thresholds ---
    {
        "id": "builtin_critical_no_accept",
        "type": "threshold",
        "text": "CRITICAL risk level prohibits ACCEPT posture",
        "conditions": {"risk_level": "CRITICAL", "posture": "ACCEPT"},
        "effects": {"action": "block", "description": "Cannot accept new bookings during critical disruptions"},
    },
    {
        "id": "builtin_high_risk_hold",
        "type": "threshold",
        "text": "HIGH risk recommends HOLD or ESCALATE posture",
        "conditions": {"risk_level": "HIGH"},
        "effects": {"action": "allow", "description": "Hold tendering until situation clarifies"},
    },

    # --- Operational policies ---
    {
        "id": "builtin_low_risk_accept",
        "type": "operational",
        "text": "LOW risk allows ACCEPT posture for normal operations",
        "conditions": {"risk_level": "LOW"},
        "effects": {"action": "allow", "description": "Normal operations, accept new bookings"},
    },
    {
        "id": "builtin_medium_risk_restrict",
        "type": "operational",
        "text": "MEDIUM risk allows RESTRICT posture",
        "conditions": {"risk_level": "MEDIUM"},
        "effects": {"action": "allow", "description": "Restrict premium SLAs, allow standard bookings"},
    },
    {
        "id": "builtin_weather_required",
        "type": "operational",
        "text": "Weather data must be available for disruption assessment",
        "conditions": {"has_weather": True},
        "effects": {"action": "allow", "description": "Weather conditions verified"},
    },
    {
        "id": "builtin_ifr_review",
        "type": "operational",
        "text": "IFR/LIFR weather conditions trigger posture review",
        "conditions": {"flight_category": ["IFR", "LIFR"]},
        "effects": {"action": "allow", "description": "Weather impacts assessed in posture decision"},
    },
]


def load_builtin_policies(session: Session) -> int:
    """
    Load built-in policies into database (idempotent).

    Uses INSERT ... ON CONFLICT(text) DO NOTHING to avoid duplicates
    and preserve any custom policies already in the table.

    Args:
        session: Database session

    Returns:
        Number of policies loaded
    """
    import json as _json
    now = datetime.now(timezone.utc)
    count = 0

    for policy_def in BUILTIN_POLICIES:
        result = session.execute(
            text("""
                INSERT INTO policy (id, type, text, conditions, effects, effective_from)
                VALUES (:id, :type, :text, CAST(:conditions AS jsonb),
                        CAST(:effects AS jsonb), :effective_from)
                ON CONFLICT (text) DO NOTHING
            """),
            {
                "id": uuid4(),
                "type": policy_def["type"],
                "text": policy_def["text"],
                "conditions": _json.dumps(policy_def["conditions"]),
                "effects": _json.dumps(policy_def["effects"]),
                "effective_from": now,
            }
        )
        if result.rowcount > 0:
            count += 1

    session.commit()
    return count
