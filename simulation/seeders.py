# simulation/seeders.py
"""
Seeders for simulation and demo data.

Creates playbooks, operational data, and demo scenarios in the database.
"""

import json
from typing import Dict, Any, List, Optional
from uuid import uuid4
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.engine import SessionLocal
from .scenarios import SCENARIOS, ExpectedPosture


def seed_playbooks(session: Optional[Session] = None) -> List[Dict[str, Any]]:
    """
    Seed playbooks based on simulation scenarios.

    Creates one playbook per scenario representing the "correct" resolution
    pattern for that type of disruption.

    Returns:
        List of created playbook summaries
    """
    owns_session = session is None
    if owns_session:
        session = SessionLocal()

    created = []

    try:
        for scenario_id, scenario in SCENARIOS.items():
            playbook = _create_playbook_from_scenario(session, scenario)
            if playbook:
                created.append(playbook)

        session.commit()

    except Exception as e:
        session.rollback()
        raise e

    finally:
        if owns_session:
            session.close()

    return created


def _create_playbook_from_scenario(
    session: Session,
    scenario,
) -> Optional[Dict[str, Any]]:
    """Create a playbook from a scenario definition."""
    # Check if playbook already exists for this scenario
    existing = session.execute(
        text("""
            SELECT id FROM playbook
            WHERE name = :name
        """),
        {"name": f"Scenario: {scenario.name}"}
    ).fetchone()

    if existing:
        return None  # Already seeded

    # Build pattern from scenario characteristics
    pattern = {
        "case_type": "AIRPORT_DISRUPTION",
        "scope_keys": ["airport"],
        "characteristics": {
            "expected_posture": scenario.expected_posture.value,
            "expected_risk_level": scenario.expected_risk_level,
            "has_contradiction": scenario.has_contradiction,
            "has_missing_source": scenario.has_missing_source,
        },
        "evidence_signals": _extract_evidence_signals(scenario),
    }

    # Build action template based on expected posture
    action_template = _build_action_template(scenario)

    playbook_id = uuid4()

    session.execute(
        text("""
            INSERT INTO playbook (id, name, pattern, action_template, stats, created_at)
            VALUES (:id, :name, CAST(:pattern AS jsonb), CAST(:action_template AS jsonb), CAST(:stats AS jsonb), :created_at)
        """),
        {
            "id": playbook_id,
            "name": f"Scenario: {scenario.name}",
            "pattern": json.dumps(pattern),
            "action_template": json.dumps(action_template),
            "stats": json.dumps({"use_count": 0, "success_count": 0, "success_rate": 0.0}),
            "created_at": datetime.now(timezone.utc),
        }
    )

    return {
        "playbook_id": str(playbook_id),
        "name": f"Scenario: {scenario.name}",
        "expected_posture": scenario.expected_posture.value,
        "expected_risk_level": scenario.expected_risk_level,
    }


def _extract_evidence_signals(scenario) -> Dict[str, Any]:
    """Extract evidence signals from scenario data."""
    signals = {}

    # FAA signals
    if scenario.faa_data:
        signals["faa"] = {
            "has_delay": scenario.faa_data.get("delay", False),
            "delay_type": scenario.faa_data.get("delay_type"),
            "ground_stop": scenario.faa_data.get("ground_stop", False),
            "closure": scenario.faa_data.get("closure", False),
        }
    else:
        signals["faa"] = {"has_delay": False}

    # Weather signals
    if scenario.metar_data:
        signals["weather"] = {
            "flight_category": scenario.metar_data.get("flight_category"),
            "visibility_miles": scenario.metar_data.get("visibility_miles"),
            "has_severe_conditions": any(
                cond in ["thunderstorms", "heavy snow", "heavy rain", "freezing fog"]
                for cond in scenario.metar_data.get("conditions", [])
            ),
        }

    # NWS alert signals
    if scenario.nws_alerts:
        signals["alerts"] = {
            "has_alerts": True,
            "max_severity": max(
                (a.get("severity", "Minor") for a in scenario.nws_alerts),
                key=lambda s: ["Minor", "Moderate", "Severe", "Extreme"].index(s)
                if s in ["Minor", "Moderate", "Severe", "Extreme"] else 0
            ),
            "alert_types": [a.get("event") for a in scenario.nws_alerts],
        }
    else:
        signals["alerts"] = {"has_alerts": False}

    # OpenSky signals
    if scenario.opensky_data:
        signals["movement"] = {
            "delta_percent": scenario.opensky_data.get("delta_percent", 0),
            "significant_reduction": scenario.opensky_data.get("delta_percent", 0) < -30,
        }
    else:
        signals["movement"] = {"missing": True}

    return signals


def _build_action_template(scenario) -> Dict[str, Any]:
    """Build action template based on expected posture."""
    posture = scenario.expected_posture

    # Base action is always SET_POSTURE
    actions = [
        {
            "type": "SET_POSTURE",
            "args": {
                "posture": posture.value,
                "airport": scenario.airport_icao,
            },
            "risk_level": "LOW",
        }
    ]

    # Add posture-specific actions
    if posture == ExpectedPosture.HOLD:
        actions.append({
            "type": "PUBLISH_GATEWAY_ADVISORY",
            "args": {"airport": scenario.airport_icao, "advisory_type": "HOLD"},
            "risk_level": "LOW",
        })

    elif posture == ExpectedPosture.RESTRICT:
        actions.append({
            "type": "PUBLISH_GATEWAY_ADVISORY",
            "args": {"airport": scenario.airport_icao, "advisory_type": "RESTRICT"},
            "risk_level": "LOW",
        })
        actions.append({
            "type": "UPDATE_BOOKING_RULES",
            "args": {"airport": scenario.airport_icao, "rule": "limit_premium_sla"},
            "risk_level": "LOW",
        })

    elif posture == ExpectedPosture.ESCALATE:
        actions.append({
            "type": "ESCALATE_OPS",
            "args": {"reason": "Critical disruption requires human decision"},
            "risk_level": "MEDIUM",
            "requires_notification": True,
        })

    return {"action_sequence": actions}


def seed_demo_case(
    airport_icao: str,
    session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Seed a demo case for an airport.

    Returns:
        Case info dict
    """
    owns_session = session is None
    if owns_session:
        session = SessionLocal()

    try:
        case_id = uuid4()

        session.execute(
            text("""
                INSERT INTO "case" (id, case_type, scope, status, created_at)
                VALUES (:id, :case_type, CAST(:scope AS jsonb), :status, :created_at)
            """),
            {
                "id": case_id,
                "case_type": "AIRPORT_DISRUPTION",
                "scope": json.dumps({"airport": airport_icao}),
                "status": "OPEN",
                "created_at": datetime.now(timezone.utc),
            }
        )

        session.commit()

        return {
            "case_id": str(case_id),
            "airport": airport_icao,
            "status": "OPEN",
        }

    finally:
        if owns_session:
            session.close()


def list_seeded_playbooks(session: Optional[Session] = None) -> List[Dict[str, Any]]:
    """List all seeded playbooks."""
    owns_session = session is None
    if owns_session:
        session = SessionLocal()

    try:
        result = session.execute(
            text("""
                SELECT id, name, pattern, stats
                FROM playbook
                ORDER BY created_at
            """)
        )

        return [
            {
                "playbook_id": str(row[0]),
                "name": row[1],
                "expected_posture": row[2].get("characteristics", {}).get("expected_posture")
                if isinstance(row[2], dict) else None,
                "use_count": row[3].get("use_count", 0) if isinstance(row[3], dict) else 0,
                "success_rate": row[3].get("success_rate", 0) if isinstance(row[3], dict) else 0,
            }
            for row in result
        ]

    finally:
        if owns_session:
            session.close()


def clear_playbooks(session: Optional[Session] = None) -> int:
    """Clear all playbooks (for re-seeding)."""
    owns_session = session is None
    if owns_session:
        session = SessionLocal()

    try:
        # First delete playbook_case links
        session.execute(text("DELETE FROM playbook_case"))
        # Then delete playbooks
        result = session.execute(text("DELETE FROM playbook"))
        session.commit()
        return result.rowcount

    finally:
        if owns_session:
            session.close()
