# app/signals/congestion.py
"""
Congestion signal extraction from FAA NAS status.

NOTE: This module only EXTRACTS and STRUCTURES data.
It does NOT assign severity or make decisions - that's the LLM's job.
"""

from typing import Optional, Dict, Any
from dataclasses import dataclass

from ..ingestion.faa_nasstatus import AirportStatus


@dataclass
class CongestionSignal:
    """Extracted congestion signal - raw facts only."""
    airport_icao: str
    has_delay: bool
    has_closure: bool
    delay_type: Optional[str]  # GROUND_STOP, GROUND_DELAY, GDP, etc.
    reason: Optional[str]
    avg_delay_minutes: Optional[int]
    attrs: Dict[str, Any]


def derive_congestion_signal(
    airport_status: AirportStatus,
) -> Optional[CongestionSignal]:
    """
    Extract congestion signal from FAA NAS status.

    This is pure data extraction - no severity assignment.
    The LLM (RiskQuantAgent) reasons about what this means.

    Args:
        airport_status: FAA NAS airport status

    Returns:
        CongestionSignal with raw facts, None if no data
    """
    if not airport_status:
        return None

    # No congestion if no delay and no closure
    if not airport_status.delay and not airport_status.closure:
        return None

    return CongestionSignal(
        airport_icao=airport_status.icao,
        has_delay=airport_status.delay,
        has_closure=airport_status.closure,
        delay_type=airport_status.delay_type,
        reason=airport_status.reason,
        avg_delay_minutes=airport_status.avg_delay_minutes,
        attrs={
            "delay": airport_status.delay,
            "delay_type": airport_status.delay_type,
            "reason": airport_status.reason,
            "avg_delay_minutes": airport_status.avg_delay_minutes,
            "closure": airport_status.closure,
            "retrieved_at": airport_status.retrieved_at.isoformat() if airport_status.retrieved_at else None,
        },
    )


def congestion_to_edge_attrs(signal: CongestionSignal) -> Dict[str, Any]:
    """Convert congestion signal to edge attributes."""
    return {
        "delay": signal.has_delay,
        "closure": signal.has_closure,
        "delay_type": signal.delay_type,
        "reason": signal.reason,
        "avg_delay_minutes": signal.avg_delay_minutes,
        **signal.attrs,
    }
