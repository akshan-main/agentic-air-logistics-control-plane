# app/signals/contradiction.py
"""
Contradiction detection between signals.

Detects when different data sources report conflicting information
about airport status.
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from uuid import UUID
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import SessionLocal


@dataclass
class ContradictionResult:
    """Detected contradiction between signals."""
    claim_a_id: UUID
    claim_b_id: UUID
    contradiction_type: str
    severity: str  # LOW, MEDIUM, HIGH
    explanation: str
    detected_at: datetime
    attrs: Dict[str, Any]


# Contradiction patterns
CONTRADICTION_PATTERNS = [
    {
        "name": "FAA_WEATHER_MISMATCH",
        "description": "FAA reports normal ops but weather indicates IFR/LIFR",
        "severity": "HIGH",
    },
    {
        "name": "FAA_MOVEMENT_MISMATCH",
        "description": "FAA reports normal ops but aircraft count collapsed",
        "severity": "HIGH",
    },
    {
        "name": "WEATHER_MOVEMENT_MISMATCH",
        "description": "Weather is VFR but aircraft count collapsed",
        "severity": "MEDIUM",
    },
    {
        "name": "STALE_FAA_DATA",
        "description": "FAA data is old but other sources show disruption",
        "severity": "MEDIUM",
    },
]


def detect_contradictions(
    airport_node_id: UUID,
    at_time: datetime,
    session: Optional[Session] = None,
) -> List[ContradictionResult]:
    """
    Detect contradictions between signals for an airport.

    Args:
        airport_node_id: Airport node ID
        at_time: Point in time to check
        session: Optional database session

    Returns:
        List of detected contradictions
    """
    if session is None:
        session = SessionLocal()
        owns_session = True
    else:
        owns_session = False

    try:
        contradictions = []

        # Get recent signals for this airport
        signals = _get_recent_signals(airport_node_id, at_time, session)

        # Check each contradiction pattern
        faa_signal = signals.get("FAA_NAS")
        weather_signal = signals.get("AVIATION_WEATHER")
        movement_signal = signals.get("OPENSKY")

        # FAA vs Weather contradiction
        if faa_signal and weather_signal:
            contradiction = _check_faa_weather_contradiction(
                faa_signal, weather_signal, at_time
            )
            if contradiction:
                contradictions.append(contradiction)

        # FAA vs Movement contradiction
        if faa_signal and movement_signal:
            contradiction = _check_faa_movement_contradiction(
                faa_signal, movement_signal, at_time
            )
            if contradiction:
                contradictions.append(contradiction)

        # Weather vs Movement contradiction
        if weather_signal and movement_signal:
            contradiction = _check_weather_movement_contradiction(
                weather_signal, movement_signal, at_time
            )
            if contradiction:
                contradictions.append(contradiction)

        return contradictions

    finally:
        if owns_session:
            session.close()


def _get_recent_signals(
    airport_node_id: UUID,
    at_time: datetime,
    session: Session,
) -> Dict[str, Dict[str, Any]]:
    """Get most recent signals for each source."""
    result = session.execute(
        text("""
            SELECT DISTINCT ON (source_system)
                id, source_system, attrs, confidence, ingested_at
            FROM edge
            WHERE src = :node_id
              AND type IN (
                  'AIRPORT_HAS_FAA_DISRUPTION',
                  'AIRPORT_WEATHER_RISK',
                  'AIRPORT_MOVEMENT_COLLAPSE'
              )
              AND ingested_at <= :at_time
            ORDER BY source_system, ingested_at DESC
        """),
        {"node_id": airport_node_id, "at_time": at_time}
    )

    signals = {}
    for row in result:
        signals[row[1]] = {
            "id": row[0],
            "source_system": row[1],
            "attrs": row[2],
            "confidence": row[3],
            "ingested_at": row[4],
        }

    return signals


def _check_faa_weather_contradiction(
    faa: Dict[str, Any],
    weather: Dict[str, Any],
    at_time: datetime,
) -> Optional[ContradictionResult]:
    """Check for FAA vs Weather contradiction."""
    faa_attrs = faa["attrs"]
    weather_attrs = weather["attrs"]

    # FAA says no disruption (use has_disruption field if available, fallback to delay/closure check)
    faa_normal = not faa_attrs.get("has_disruption", faa_attrs.get("delay") or faa_attrs.get("closure"))

    # Weather says poor conditions (use severity field if available)
    weather_severity = weather_attrs.get("severity", "")
    weather_severe = weather_severity in ("HIGH", "CRITICAL")
    flight_cat = weather_attrs.get("flight_category")
    weather_ifr = flight_cat in ("IFR", "LIFR")

    if faa_normal and (weather_severe or weather_ifr):
        return ContradictionResult(
            claim_a_id=faa["id"],
            claim_b_id=weather["id"],
            contradiction_type="FAA_WEATHER_MISMATCH",
            severity="HIGH",
            explanation=(
                f"FAA reports normal operations but weather shows "
                f"{flight_cat} conditions with {weather_attrs.get('severity')} risk"
            ),
            detected_at=at_time,
            attrs={
                "faa_delay": faa_attrs.get("delay"),
                "faa_closure": faa_attrs.get("closure"),
                "weather_severity": weather_attrs.get("severity"),
                "flight_category": flight_cat,
            },
        )

    return None


def _check_faa_movement_contradiction(
    faa: Dict[str, Any],
    movement: Dict[str, Any],
    at_time: datetime,
) -> Optional[ContradictionResult]:
    """Check for FAA vs Movement contradiction."""
    faa_attrs = faa["attrs"]
    movement_attrs = movement["attrs"]

    # FAA says no disruption (use has_disruption field if available)
    faa_normal = not faa_attrs.get("has_disruption", faa_attrs.get("delay") or faa_attrs.get("closure"))

    # Movement shows collapse (use derived severity if available)
    movement_severity = movement_attrs.get("severity", "")
    movement_severe = movement_severity in ("HIGH", "CRITICAL")
    delta = movement_attrs.get("delta_percent", 0)

    if faa_normal and movement_severe:
        return ContradictionResult(
            claim_a_id=faa["id"],
            claim_b_id=movement["id"],
            contradiction_type="FAA_MOVEMENT_MISMATCH",
            severity="HIGH",
            explanation=(
                f"FAA reports normal operations but aircraft count "
                f"is down {abs(delta):.0f}% from baseline"
            ),
            detected_at=at_time,
            attrs={
                "faa_delay": faa_attrs.get("delay"),
                "faa_closure": faa_attrs.get("closure"),
                "aircraft_count": movement_attrs.get("aircraft_count"),
                "delta_percent": delta,
            },
        )

    return None


def _check_weather_movement_contradiction(
    weather: Dict[str, Any],
    movement: Dict[str, Any],
    at_time: datetime,
) -> Optional[ContradictionResult]:
    """Check for Weather vs Movement contradiction."""
    weather_attrs = weather["attrs"]
    movement_attrs = movement["attrs"]

    # Weather is good (VFR)
    flight_cat = weather_attrs.get("flight_category")
    weather_good = flight_cat == "VFR" and weather_attrs.get("severity") == "LOW"

    # Movement shows collapse
    movement_severe = movement_attrs.get("severity") in ("HIGH", "CRITICAL")

    if weather_good and movement_severe:
        return ContradictionResult(
            claim_a_id=weather["id"],
            claim_b_id=movement["id"],
            contradiction_type="WEATHER_MOVEMENT_MISMATCH",
            severity="MEDIUM",
            explanation=(
                f"Weather is VFR but aircraft count shows "
                f"{movement_attrs.get('severity')} collapse"
            ),
            detected_at=at_time,
            attrs={
                "flight_category": flight_cat,
                "weather_severity": weather_attrs.get("severity"),
                "movement_severity": movement_attrs.get("severity"),
                "aircraft_count": movement_attrs.get("aircraft_count"),
            },
        )

    return None


def persist_contradiction(
    contradiction: ContradictionResult,
    session: Optional[Session] = None,
) -> UUID:
    """
    Persist contradiction to database.

    Args:
        contradiction: Detected contradiction
        session: Optional session

    Returns:
        Contradiction ID
    """
    from uuid import uuid4

    if session is None:
        session = SessionLocal()
        owns_session = True
    else:
        owns_session = False

    try:
        contradiction_id = uuid4()

        session.execute(
            text("""
                INSERT INTO contradiction
                (id, claim_a, claim_b, detected_at, resolution_status, resolution_notes)
                VALUES (:id, :claim_a, :claim_b, :detected_at, 'OPEN', :notes)
            """),
            {
                "id": contradiction_id,
                "claim_a": contradiction.claim_a_id,
                "claim_b": contradiction.claim_b_id,
                "detected_at": contradiction.detected_at,
                "notes": contradiction.explanation,
            }
        )

        session.commit()
        return contradiction_id

    finally:
        if owns_session:
            session.close()
