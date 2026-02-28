# app/signals/movement_collapse.py
"""
Movement data extraction from OpenSky ADS-B data.

NOTE: This module only EXTRACTS and STRUCTURES data.
It does NOT assign severity or make decisions - that's the LLM's job.
"""

from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime

from ..ingestion.opensky import OpenSkyResponse


@dataclass
class MovementSignal:
    """Extracted movement signal - raw facts only."""
    airport_icao: str
    aircraft_count: int
    airborne_count: int
    ground_count: int
    timestamp: int
    retrieved_at: datetime
    attrs: Dict[str, Any]


# Default baseline aircraft counts by airport size
# These are reference values for context, not for rule-based decisions
# The LLM uses these as context to understand if traffic is unusual
DEFAULT_BASELINES = {
    "KJFK": 150,
    "KLAX": 140,
    "KORD": 160,
    "KATL": 180,
    "KDFW": 130,
    "KDEN": 120,
    "KMIA": 100,
    "KSFO": 110,
    "KBOS": 90,
    "KSEA": 80,
    "KLAS": 70,
    "KMCO": 60,
    "KEWR": 80,
    "KPHX": 70,
}


def derive_movement_collapse_signal(
    airport_icao: str,
    opensky_response: OpenSkyResponse,
    baseline: Optional[int] = None,
) -> Optional[MovementSignal]:
    """
    Extract movement signal from OpenSky data.

    This is pure data extraction - no severity assignment.
    The LLM (RiskQuantAgent) reasons about what this means.

    Args:
        airport_icao: ICAO airport code
        opensky_response: OpenSky API response
        baseline: Optional historical baseline for context

    Returns:
        MovementSignal with raw facts
    """
    if not opensky_response:
        return None

    aircraft_count = opensky_response.aircraft_count
    airborne_count = sum(1 for s in opensky_response.states if not s.on_ground)
    ground_count = sum(1 for s in opensky_response.states if s.on_ground)

    # Get baseline for context (not for rule-based decisions)
    if baseline is None:
        baseline = DEFAULT_BASELINES.get(airport_icao.upper(), 100)

    # Calculate percent for context (LLM uses this to reason)
    if baseline > 0:
        percent_of_baseline = (aircraft_count / baseline) * 100
    else:
        percent_of_baseline = 100.0

    return MovementSignal(
        airport_icao=airport_icao,
        aircraft_count=aircraft_count,
        airborne_count=airborne_count,
        ground_count=ground_count,
        timestamp=opensky_response.time,
        retrieved_at=opensky_response.retrieved_at,
        attrs={
            "aircraft_count": aircraft_count,
            "airborne_count": airborne_count,
            "ground_count": ground_count,
            "baseline_reference": baseline,
            "percent_of_baseline": percent_of_baseline,
            "timestamp": opensky_response.time,
            "retrieved_at": opensky_response.retrieved_at.isoformat() if opensky_response.retrieved_at else None,
        },
    )


def movement_to_edge_attrs(signal: MovementSignal) -> Dict[str, Any]:
    """Convert movement signal to edge attributes."""
    return {
        "aircraft_count": signal.aircraft_count,
        "airborne_count": signal.airborne_count,
        "ground_count": signal.ground_count,
        **signal.attrs,
    }


def get_airport_baseline(airport_icao: str) -> int:
    """
    Get reference baseline aircraft count for airport.

    This is for context only - the LLM decides what counts as "unusual".

    Args:
        airport_icao: ICAO airport code

    Returns:
        Reference baseline aircraft count
    """
    return DEFAULT_BASELINES.get(airport_icao.upper(), 100)
