# app/signals/derive.py
"""
Signal extraction from raw evidence.

NOTE: This module only EXTRACTS and STRUCTURES data.
It does NOT assign severity or make decisions - that's the LLM's job.

Creates graph edges representing extracted signals:
- AIRPORT_HAS_FAA_DISRUPTION
- AIRPORT_WEATHER_RISK
- AIRPORT_HAS_NWS_ALERT
- AIRPORT_MOVEMENT_COLLAPSE
"""

from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from uuid import UUID

from ..graph.store import GraphStore
from ..ingestion.registry import AirportIngestionResult


@dataclass
class DerivedSignal:
    """Extracted signal to create as edge."""
    edge_type: str
    src_node_id: UUID
    dst_node_id: Optional[UUID]
    attrs: Dict[str, Any]
    confidence: float
    source_system: str
    evidence_ids: List[UUID]
    # Bi-temporal fields
    event_time_start: Optional[datetime] = None  # When this became true in reality
    event_time_end: Optional[datetime] = None    # When this stopped being true


class SignalDeriver:
    """
    Extracts signals from ingested evidence.

    Creates edges in the graph representing extracted signals.
    Each signal is linked to its source evidence.

    NOTE: No severity assignment - LLM does all reasoning.
    """

    def __init__(self, graph_store: GraphStore):
        self.graph = graph_store

    def derive_from_ingestion(
        self,
        airport_node_id: UUID,
        ingestion_result: AirportIngestionResult,
        evidence_ids: Dict[str, UUID],
    ) -> List[DerivedSignal]:
        """
        Extract all signals from airport ingestion result.

        Args:
            airport_node_id: Airport node ID
            ingestion_result: Result from ingestion registry
            evidence_ids: Map of source name to evidence ID

        Returns:
            List of derived signals (raw data, no severity)
        """
        signals = []

        # FAA disruption signal
        if ingestion_result.faa_status and ingestion_result.faa_status.success:
            faa_signal = self._extract_faa_signal(
                airport_node_id,
                ingestion_result.faa_status.data,
                evidence_ids.get("FAA_NAS"),
            )
            if faa_signal:
                signals.append(faa_signal)

        # Weather signal
        if ingestion_result.metar and ingestion_result.metar.success:
            weather_signal = self._extract_weather_signal(
                airport_node_id,
                ingestion_result.metar.data,
                ingestion_result.taf.data if ingestion_result.taf else None,
                evidence_ids.get("METAR"),
            )
            if weather_signal:
                signals.append(weather_signal)

        # NWS alert signals
        if ingestion_result.nws_alerts and ingestion_result.nws_alerts.success:
            for alert in ingestion_result.nws_alerts.data or []:
                alert_signal = self._extract_nws_signal(
                    airport_node_id,
                    alert,
                    evidence_ids.get("NWS_ALERTS"),
                )
                if alert_signal:
                    signals.append(alert_signal)

        # Movement signal
        if ingestion_result.opensky and ingestion_result.opensky.success:
            movement_signal = self._extract_movement_signal(
                airport_node_id,
                ingestion_result.opensky.data,
                evidence_ids.get("OPENSKY"),
            )
            if movement_signal:
                signals.append(movement_signal)

        return signals

    def _extract_faa_signal(
        self,
        airport_node_id: UUID,
        faa_status,
        evidence_id: Optional[UUID],
    ) -> Optional[DerivedSignal]:
        """Extract FAA status signal - raw data only.

        IMPORTANT: Always emit an edge, even for normal operations.
        This is required for contradiction detection to work - we need
        to know "FAA says normal" to detect conflicts with weather/movement.
        """
        # IngestionRegistry uses `data=None` to mean "no disruptions reported".
        # That should still create a NORMAL FAA edge so downstream components
        # (contradictions, packets, LLM context) can reason about "FAA says normal".
        inferred_from_absence = faa_status is None

        if inferred_from_absence:
            delay = False
            delay_type = None
            reason = None
            avg_delay_minutes = None
            closure = False
        elif isinstance(faa_status, dict):
            delay = bool(faa_status.get("delay", False))
            delay_type = faa_status.get("delay_type")
            reason = faa_status.get("reason")
            avg_delay_minutes = faa_status.get("avg_delay_minutes")
            closure = bool(faa_status.get("closure", False))
        else:
            delay = bool(getattr(faa_status, "delay", False))
            delay_type = getattr(faa_status, "delay_type", None)
            reason = getattr(faa_status, "reason", None)
            avg_delay_minutes = getattr(faa_status, "avg_delay_minutes", None)
            closure = bool(getattr(faa_status, "closure", False))

        # Determine status: disrupted or normal
        has_disruption = delay or closure

        # Set event time to now (when we observed this state)
        now = datetime.now(timezone.utc)
        observed_at = getattr(faa_status, "retrieved_at", None) or now

        confidence = 0.90 if inferred_from_absence else 0.95  # FAA is authoritative; absence is slightly weaker
        return DerivedSignal(
            edge_type="AIRPORT_HAS_FAA_DISRUPTION",
            src_node_id=airport_node_id,
            dst_node_id=None,
            attrs={
                # Raw facts - LLM interprets what these mean
                "delay": delay,
                "delay_type": delay_type,
                "reason": reason,
                "avg_delay_minutes": avg_delay_minutes,
                "closure": closure,
                # Status field for contradiction detection
                "status": "DISRUPTED" if has_disruption else "NORMAL",
                "has_disruption": has_disruption,
                "inferred_from_absence": inferred_from_absence,
            },
            confidence=confidence,
            source_system="FAA_NAS",
            evidence_ids=[evidence_id] if evidence_id else [],
            event_time_start=observed_at,  # When this state was observed
        )

    def _safe_value(self, value) -> Any:
        """Safely preserve a value."""
        if value is None:
            return None
        try:
            # Try to convert to native Python types
            if hasattr(value, 'item'):  # numpy types
                return value.item()
            return value
        except (ValueError, TypeError, AttributeError):
            return value

    def _extract_weather_signal(
        self,
        airport_node_id: UUID,
        metar,
        taf,
        evidence_id: Optional[UUID],
    ) -> Optional[DerivedSignal]:
        """Extract weather signal from METAR/TAF - raw data only."""
        if not metar:
            return None

        # Derive severity based on flight category and conditions
        severity = self._derive_weather_severity(metar)

        # Build conditions string from weather phenomena
        conditions = self._build_conditions_string(metar)

        # Set event time to METAR observation time if available, else now
        event_time = datetime.now(timezone.utc)
        if hasattr(metar, 'observation_time') and metar.observation_time:
            event_time = metar.observation_time

        return DerivedSignal(
            edge_type="AIRPORT_WEATHER_RISK",
            src_node_id=airport_node_id,
            dst_node_id=None,
            attrs={
                # Raw facts - LLM interprets what these mean
                "flight_category": metar.flight_category,
                "wind_direction": self._safe_value(metar.wind_direction),
                "wind_speed": self._safe_value(metar.wind_speed),
                "wind_gust": self._safe_value(metar.wind_gust),
                "visibility_miles": self._safe_value(metar.visibility_miles),
                "ceiling_feet": self._safe_value(metar.ceiling_feet),
                "ceiling_type": metar.ceiling_type,
                "weather": metar.weather,
                "temp_c": self._safe_value(metar.temp_c),
                "dewpoint_c": self._safe_value(metar.dewpoint_c),
                "raw_metar": metar.raw_text,
                # Derived fields for downstream consumers
                "conditions": conditions,
                "severity": severity,
            },
            confidence=0.90,
            source_system="AVIATION_WEATHER",
            evidence_ids=[evidence_id] if evidence_id else [],
            event_time_start=event_time,
        )

    def _derive_weather_severity(self, metar) -> str:
        """Derive severity from weather conditions."""
        # IFR/LIFR conditions are high severity
        if metar.flight_category in ("LIFR", "IFR"):
            return "HIGH"

        # Check for dangerous weather phenomena
        weather_str = metar.weather or ""
        if any(wx in weather_str for wx in ["TS", "GR", "FC", "SS", "DS"]):
            return "HIGH"  # Thunderstorm, hail, funnel cloud, sandstorm

        # Check wind conditions
        wind_speed = self._safe_value(metar.wind_speed) or 0
        wind_gust = self._safe_value(metar.wind_gust) or 0
        if wind_gust >= 35 or wind_speed >= 25:
            return "HIGH"
        if wind_gust >= 25 or wind_speed >= 15:
            return "MEDIUM"

        # MVFR is medium severity
        if metar.flight_category == "MVFR":
            return "MEDIUM"

        # Low visibility or ceiling
        visibility = self._safe_value(metar.visibility_miles) or 10
        ceiling = self._safe_value(metar.ceiling_feet) or 10000
        if visibility < 3 or ceiling < 1000:
            return "MEDIUM"

        return "LOW"

    def _build_conditions_string(self, metar) -> str:
        """Build human-readable conditions string."""
        parts = []

        # Flight category
        if metar.flight_category:
            parts.append(metar.flight_category)

        # Weather phenomena
        if metar.weather:
            parts.append(metar.weather)

        # Wind info if significant
        wind_speed = self._safe_value(metar.wind_speed) or 0
        wind_gust = self._safe_value(metar.wind_gust)
        if wind_speed >= 15:
            wind_str = f"Wind {wind_speed}kt"
            if wind_gust:
                wind_str += f" G{wind_gust}kt"
            parts.append(wind_str)

        return ", ".join(parts) if parts else "VFR"

    def _extract_nws_signal(
        self,
        airport_node_id: UUID,
        alert,
        evidence_id: Optional[UUID],
    ) -> Optional[DerivedSignal]:
        """Extract NWS alert signal - raw data only."""
        if not alert:
            return None

        # Best-effort event time from NWS; fall back to retrieval time
        now = datetime.now(timezone.utc)
        effective = getattr(alert, "effective", None) or now
        event_end = alert.expires if hasattr(alert, 'expires') and alert.expires else None

        return DerivedSignal(
            edge_type="AIRPORT_HAS_NWS_ALERT",
            src_node_id=airport_node_id,
            dst_node_id=None,
            attrs={
                # Raw facts - LLM interprets what these mean
                "event": alert.event,
                "severity": alert.severity,  # This is NWS's own categorization, not ours
                "certainty": alert.certainty,
                "urgency": alert.urgency,
                "headline": alert.headline,
                "expires": alert.expires.isoformat() if alert.expires else None,
            },
            confidence=0.85,
            source_system="NWS_ALERTS",
            evidence_ids=[evidence_id] if evidence_id else [],
            event_time_start=effective,
            event_time_end=event_end,
        )

    def _extract_movement_signal(
        self,
        airport_node_id: UUID,
        opensky_response,
        evidence_id: Optional[UUID],
    ) -> Optional[DerivedSignal]:
        """Extract movement signal from OpenSky - raw data only."""
        if not opensky_response:
            return None

        # Derive severity and delta from aircraft count
        # Typical major hub has 50-150 aircraft in bounding box
        # Secondary airport has 10-30
        aircraft_count = opensky_response.aircraft_count or 0
        severity, delta_percent = self._derive_movement_metrics(aircraft_count)

        # Use retrieved_at as event_time (when observation was made)
        event_time = opensky_response.retrieved_at if hasattr(opensky_response, 'retrieved_at') and opensky_response.retrieved_at else datetime.now(timezone.utc)

        return DerivedSignal(
            edge_type="AIRPORT_MOVEMENT_COLLAPSE",
            src_node_id=airport_node_id,
            dst_node_id=None,
            attrs={
                # Raw facts - LLM interprets what these mean
                "aircraft_count": aircraft_count,
                "timestamp": opensky_response.time,
                "retrieved_at": opensky_response.retrieved_at.isoformat() if opensky_response.retrieved_at else None,
                # Derived fields for downstream consumers
                "delta_percent": delta_percent,
                "severity": severity,
            },
            confidence=0.70,  # OpenSky less reliable
            source_system="OPENSKY",
            evidence_ids=[evidence_id] if evidence_id else [],
            event_time_start=event_time,
        )

    def _derive_movement_metrics(self, aircraft_count: int) -> tuple:
        """
        Derive movement severity and delta percentage.

        Without historical baseline, we estimate based on absolute counts.
        Typical ranges:
        - Major hub (JFK, ORD, LAX): 80-150 aircraft
        - Medium hub: 30-80 aircraft
        - Small airport: 5-30 aircraft

        Returns (severity, delta_percent)
        """
        # Estimate expected baseline (conservative estimate for hub)
        expected_baseline = 60  # Typical medium-large airport

        if aircraft_count == 0:
            return ("HIGH", -100)
        elif aircraft_count < 10:
            delta = ((aircraft_count - expected_baseline) / expected_baseline) * 100
            return ("HIGH", round(delta, 1))
        elif aircraft_count < 30:
            delta = ((aircraft_count - expected_baseline) / expected_baseline) * 100
            return ("MEDIUM", round(delta, 1))
        else:
            delta = ((aircraft_count - expected_baseline) / expected_baseline) * 100
            return ("LOW", round(delta, 1))

    def persist_signals(
        self,
        signals: List[DerivedSignal],
    ) -> List[UUID]:
        """
        Persist extracted signals as graph edges.

        Args:
            signals: Signals to persist

        Returns:
            List of created edge IDs
        """
        edge_ids = []

        for signal in signals:
            # Create edge as DRAFT with bi-temporal fields
            edge = self.graph.create_edge(
                src=signal.src_node_id,
                dst=signal.dst_node_id or signal.src_node_id,  # Self-loop if no dst
                type=signal.edge_type,
                source_system=signal.source_system,
                attrs=signal.attrs,
                status="DRAFT",
                confidence=signal.confidence,
                event_time_start=signal.event_time_start,
                event_time_end=signal.event_time_end,
            )

            # Add evidence bindings
            for evidence_id in signal.evidence_ids:
                self.graph.add_edge_evidence(edge.id, evidence_id)

            # Promote to FACT if we have evidence
            if signal.evidence_ids:
                self.graph.promote_edge_to_fact(edge.id)

            edge_ids.append(edge.id)

        return edge_ids


def derive_signals_for_airport(
    airport_icao: str,
    graph_store: GraphStore,
    ingestion_result: AirportIngestionResult,
    evidence_ids: Dict[str, UUID],
) -> List[UUID]:
    """
    Convenience function to extract and persist signals for an airport.

    Args:
        airport_icao: ICAO code
        graph_store: Graph store
        ingestion_result: Ingestion result
        evidence_ids: Map of source to evidence ID

    Returns:
        List of created edge IDs
    """
    # Get or create airport node
    airport_node = graph_store.get_node_by_identifier("AIRPORT", airport_icao)
    if not airport_node:
        airport_node = graph_store.create_node("AIRPORT", airport_icao)

    # Extract signals
    deriver = SignalDeriver(graph_store)
    signals = deriver.derive_from_ingestion(
        airport_node.id,
        ingestion_result,
        evidence_ids,
    )

    # Persist signals
    return deriver.persist_signals(signals)
