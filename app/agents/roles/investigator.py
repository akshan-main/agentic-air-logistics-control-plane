# app/agents/roles/investigator.py
"""
Investigator Agent - gathers evidence and queries graph.

Responsible for:
- Ingesting signals from external sources
- Querying the context graph
- Identifying uncertainties that need resolution
"""

import json
from pathlib import Path
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..state_graph import BeliefState, Uncertainty, Hypothesis
from ...ingestion.registry import IngestionRegistry
from ...evidence.store import store_evidence
from ...evidence.extract import extract_excerpt
from ...signals.derive import derive_signals_for_airport
from ...graph.store import GraphStore


INGESTION_CRITICALITY_BY_SOURCE = {
    # If these are missing, posture decisions are not defensible.
    "FAA_NAS": "BLOCKING",
    "METAR": "BLOCKING",
    # Helpful but not strictly required for a first posture.
    "TAF": "DEGRADED",
    "NWS_ALERTS": "DEGRADED",
    # Movement is validation, not authoritative ops status.
    "OPENSKY": "INFORMATIONAL",
}


class InvestigatorAgent:
    """
    Gathers evidence and queries the context graph.

    Does NOT make decisions - only gathers information.
    """

    def __init__(self, case_id: UUID, session: Session, skip_cache: bool = False):
        self.case_id = case_id
        self.session = session
        self.registry = IngestionRegistry()
        self.graph = GraphStore(session)
        self.skip_cache = skip_cache  # For simulation - bypass evidence cache

    def investigate(self, belief_state: BeliefState) -> None:
        """
        Perform initial investigation for case.

        Args:
            belief_state: Current belief state (modified in place)
        """
        # Get case scope
        case_scope = self._get_case_scope()
        if not case_scope:
            return

        airport_icao = case_scope.get("airport")
        if not airport_icao:
            return

        belief_state.airport_icao = airport_icao

        # Link any orphan missing_evidence_requests from pre-case ingestion
        self._link_orphan_missing_evidence(airport_icao)

        # Link any existing booking evidence for this airport to the case.
        # Booking evidence is created by graph_seeder or an internal booking system.
        # PolicyJudge._has_booking_evidence() checks evidence.source_system='BOOKING'
        # linked via trace_event — so we must create that link here.
        self._link_booking_evidence(airport_icao)

        # Ingest all signals
        self._ingest_airport_signals(airport_icao, belief_state)

        # Identify uncertainties
        self._identify_uncertainties(belief_state)

        # Detect contradictions between signals
        self._detect_contradictions(belief_state)

        # Build initial hypotheses
        self._build_hypotheses(belief_state)

    def investigate_uncertainties(
        self,
        belief_state: BeliefState,
        uncertainties: List[Uncertainty],
    ) -> None:
        """
        Investigate specific uncertainties.

        Args:
            belief_state: Current belief state
            uncertainties: Uncertainties to investigate
        """
        for uncertainty in uncertainties:
            self._investigate_uncertainty(uncertainty, belief_state)

    def _get_case_scope(self) -> Optional[Dict[str, Any]]:
        """Get case scope from database."""
        result = self.session.execute(
            text('SELECT scope FROM "case" WHERE id = :id'),
            {"id": self.case_id}
        )
        row = result.fetchone()
        return row[0] if row else None

    def _ingest_airport_signals(
        self,
        airport_icao: str,
        belief_state: BeliefState,
    ) -> None:
        """
        Ingest all signals for an airport.

        First checks for recent evidence (within 5 minutes). If found AND all 5 sources
        are present, uses that. Otherwise fetches fresh data.
        """
        # Expected sources - must have all 5
        EXPECTED_SOURCES = {"FAA_NAS", "METAR", "TAF", "NWS_ALERTS", "OPENSKY"}

        # Check for recent evidence first (within 5 minutes)
        # Skip cache if running in simulation mode
        if not self.skip_cache:
            recent_evidence = self._get_recent_evidence(airport_icao)

            if recent_evidence:
                # Check if we have all 5 sources cached
                cached_sources = {e[1] for e in recent_evidence}  # source_system is at index 1
                if cached_sources >= EXPECTED_SOURCES:
                    # All 5 sources present - use cached
                    self._use_existing_evidence(airport_icao, recent_evidence, belief_state)
                    return
                # Missing sources - fetch fresh

        # No recent evidence OR missing sources OR skip_cache - fetch fresh data
        result = self.registry.ingest_airport(
            airport_icao,
            case_id=str(self.case_id),
            include_opensky=True,
        )

        # Store evidence and track IDs
        evidence_ids: Dict[str, UUID] = {}

        # Store ALL 5 sources - even if None/empty/error
        all_sources = {
            "FAA_NAS": result.faa_status,
            "METAR": result.metar,
            "TAF": result.taf,
            "NWS_ALERTS": result.nws_alerts,
            "OPENSKY": result.opensky,
        }

        for source_name, ingestion_result in all_sources.items():
            # Store evidence for ALL sources - success, failure, no data, or None
            # This ensures we always have 5 evidence records
            import json

            # Handle case where ingestion_result is None (source not called)
            if ingestion_result is None:
                raw_bytes = json.dumps({
                    "status": "not_fetched",
                    "source": source_name,
                    "message": f"Source {source_name} was not fetched"
                }).encode('utf-8')
            elif ingestion_result.success and ingestion_result.data:
                # Has data - serialize normally
                if isinstance(ingestion_result.data, list):
                    if len(ingestion_result.data) > 0:
                        raw_bytes = json.dumps(
                            [item.__dict__ if hasattr(item, '__dict__') else item
                             for item in ingestion_result.data],
                            default=str
                        ).encode('utf-8')
                    else:
                        # Empty list (e.g., no NWS alerts) - still evidence of normal conditions
                        raw_bytes = json.dumps({
                            "status": "no_data",
                            "source": source_name,
                            "message": f"No active alerts/data from {source_name} (normal conditions)"
                        }).encode('utf-8')
                else:
                    raw_bytes = json.dumps(
                        ingestion_result.data.__dict__ if hasattr(ingestion_result.data, '__dict__')
                        else str(ingestion_result.data),
                        default=str
                    ).encode('utf-8')
            elif ingestion_result.success:
                # Success but no data (e.g., no FAA disruptions) - evidence of normal operations
                raw_bytes = json.dumps({
                    "status": "no_disruption",
                    "source": source_name,
                    "message": f"No disruptions reported by {source_name} (normal operations)"
                }).encode('utf-8')
            else:
                # API call failed - still record as evidence that we tried
                raw_bytes = json.dumps({
                    "status": "api_error",
                    "source": source_name,
                    "error": ingestion_result.error or "Unknown error",
                    "message": f"Failed to fetch from {source_name}"
                }).encode('utf-8')

            sha256 = store_evidence(raw_bytes)

            # Create evidence record - use source_name for consistency
            retrieved_at = ingestion_result.retrieved_at if ingestion_result else datetime.now(timezone.utc)
            excerpt = extract_excerpt(raw_bytes)
            evidence_id = self._create_evidence_record(
                source_system=source_name,
                payload_sha256=sha256,
                retrieved_at=retrieved_at,
                airport_icao=airport_icao,
                excerpt=excerpt,
            )

            evidence_ids[source_name] = evidence_id
            belief_state.evidence_ids.append(evidence_id)

            # Track valid vs error evidence separately
            # This is critical for the critic - API errors shouldn't count as valid evidence
            if ingestion_result is None or not ingestion_result.success:
                belief_state.error_evidence_ids.append(evidence_id)
            else:
                belief_state.valid_evidence_ids.append(evidence_id)

            # Record missing evidence (or resolve it) as first-class state.
            # This is what the orchestrator uses to decide whether the case can proceed.
            if ingestion_result is not None and ingestion_result.success:
                self._resolve_missing_evidence_for_source(source_name, evidence_id)
            else:
                if ingestion_result and ingestion_result.missing_evidence:
                    # OpenSky provides richer request_type/params/criticality.
                    self._upsert_missing_evidence_request(
                        source_system=ingestion_result.missing_evidence.source_system,
                        request_type=ingestion_result.missing_evidence.request_type,
                        request_params=ingestion_result.missing_evidence.request_params or {},
                        reason=ingestion_result.missing_evidence.reason,
                        criticality=ingestion_result.missing_evidence.criticality,
                    )
                else:
                    self._upsert_missing_evidence_request(
                        source_system=source_name,
                        request_type=f"airport_ingestion:{airport_icao}",
                        request_params={"airport_icao": airport_icao},
                        reason=(ingestion_result.error if ingestion_result else "Source was not fetched"),
                        criticality=INGESTION_CRITICALITY_BY_SOURCE.get(source_name, "DEGRADED"),
                    )

        # Commit evidence records
        self.session.commit()

        # Derive signals and create graph edges
        edge_ids = derive_signals_for_airport(
            airport_icao,
            self.graph,
            result,
            evidence_ids,
        )
        belief_state.edge_ids.extend(edge_ids)

        # FIXED: Log trace_events with ref_type='edge' for graph endpoints to work
        self._log_edge_trace_events(edge_ids)

        # Track tool calls - always 5 sources
        belief_state.increment_tool_calls(5)

    def _get_recent_evidence(self, airport_icao: str) -> list:
        """
        Get evidence for this airport that was retrieved within the last 5 minutes.

        Returns list of (evidence_id, source_system, retrieved_at) tuples.
        Only returns the MOST RECENT evidence per source_system (no duplicates).
        """
        result = self.session.execute(
            text("""
                SELECT DISTINCT ON (source_system) id, source_system, retrieved_at
                FROM evidence
                WHERE source_ref = :source_ref
                  AND retrieved_at > NOW() - INTERVAL '5 minutes'
                ORDER BY source_system, retrieved_at DESC
            """),
            {"source_ref": f"airport:{airport_icao}"}
        )
        return list(result.fetchall())

    def _use_existing_evidence(
        self,
        airport_icao: str,
        recent_evidence: list,
        belief_state: BeliefState,
    ) -> None:
        """Use existing recent evidence instead of re-fetching."""
        from uuid import uuid4
        from ...evidence.store import EVIDENCE_ROOT

        evidence_ids: Dict[str, UUID] = {}

        for evidence_id, source_system, retrieved_at in recent_evidence:
            # Link evidence to this case via trace_event
            from ...db.engine import get_next_trace_seq
            seq = get_next_trace_seq(self.case_id, self.session)
            self.session.execute(
                text("""
                    INSERT INTO trace_event
                    (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                    VALUES
                    (:id, :case_id, :seq, :event_type, :ref_type, :ref_id,
                     CAST(:meta AS jsonb), :created_at)
                """),
                {
                    "id": uuid4(),
                    "case_id": self.case_id,
                    "seq": seq,
                    "event_type": "TOOL_RESULT",
                    "ref_type": "evidence",
                    "ref_id": evidence_id,
                    "meta": json.dumps({"source_system": source_system, "reused": True}),
                    "created_at": datetime.now(timezone.utc),
                }
            )

            evidence_ids[source_system] = evidence_id
            belief_state.evidence_ids.append(evidence_id)

            # FIXED: Classify cached evidence into valid/error like fresh path
            # Read evidence payload to check if it's valid or an error
            is_error = False
            try:
                result = self.session.execute(
                    text("SELECT raw_path, payload_sha256 FROM evidence WHERE id = :id"),
                    {"id": evidence_id}
                )
                row = result.fetchone()
                if row:
                    raw_path, sha256 = row
                    evidence_file = Path(raw_path) if raw_path else None
                    if not evidence_file or not evidence_file.exists():
                        evidence_file = EVIDENCE_ROOT / f"{sha256}.bin"
                    if evidence_file.exists():
                        payload = json.loads(evidence_file.read_bytes())
                        # Check for error status markers from fresh ingestion path
                        status = payload.get("status") if isinstance(payload, dict) else None
                        if status in ("api_error", "not_fetched"):
                            is_error = True
            except Exception:
                # If we can't read the evidence, treat as error
                is_error = True

            # Classify like the fresh path does (lines 211-214)
            if is_error:
                belief_state.error_evidence_ids.append(evidence_id)
            else:
                belief_state.valid_evidence_ids.append(evidence_id)

        self.session.commit()

        # Now derive signals from the existing evidence
        edge_ids_before = len(belief_state.edge_ids)
        self._derive_signals_from_evidence(airport_icao, evidence_ids, belief_state)

        # FIXED: Log trace_events with ref_type='edge' for graph endpoints to work
        new_edge_ids = belief_state.edge_ids[edge_ids_before:]
        if new_edge_ids:
            self._log_edge_trace_events(new_edge_ids)

    def _derive_signals_from_evidence(
        self,
        airport_icao: str,
        evidence_ids: Dict[str, UUID],
        belief_state: BeliefState,
    ) -> None:
        """
        Derive signals from stored evidence.

        Reads evidence payloads and creates graph edges.
        """

        # Get or create airport node
        airport_node = self.graph.get_node_by_identifier("AIRPORT", airport_icao)
        if not airport_node:
            airport_node = self.graph.create_node("AIRPORT", airport_icao)

        # For each evidence, read and create appropriate edges
        for source_system, evidence_id in evidence_ids.items():
            # Read evidence payload
            result = self.session.execute(
                text("SELECT raw_path, payload_sha256 FROM evidence WHERE id = :id"),
                {"id": evidence_id}
            )
            row = result.fetchone()
            if not row:
                continue

            raw_path, sha256 = row

            # Read the stored evidence file
            try:
                from ...evidence.store import EVIDENCE_ROOT
                evidence_file = Path(raw_path)
                if not evidence_file.exists():
                    evidence_file = EVIDENCE_ROOT / f"{sha256}.bin"
                if not evidence_file.exists():
                    continue

                payload = json.loads(evidence_file.read_bytes())
            except Exception:
                continue

            # Create edge based on source type
            edge_type = None
            attrs = {}

            if source_system == "FAA_NAS":
                if not isinstance(payload, dict):
                    continue

                status = payload.get("status")
                # Mirror derive_from_ingestion(): no FAA edge if the fetch failed.
                if status in ("api_error", "not_fetched"):
                    continue

                delay = bool(payload.get("delay", False))
                closure = bool(payload.get("closure", False))
                has_disruption = delay or closure

                # IMPORTANT: Always emit an FAA edge (NORMAL or DISRUPTED).
                # "no_disruption"/"normal_operations" payloads won't have delay/closure keys.
                edge_type = "AIRPORT_HAS_FAA_DISRUPTION"
                attrs = {
                    "delay": delay,
                    "delay_type": payload.get("delay_type"),
                    "reason": payload.get("reason"),
                    "avg_delay_minutes": payload.get("avg_delay_minutes"),
                    "closure": closure,
                    "status": "DISRUPTED" if has_disruption else "NORMAL",
                    "has_disruption": has_disruption,
                    "inferred_from_absence": bool(payload.get("inferred_from_absence", False))
                    or status in ("no_disruption", "normal_operations"),
                }

            elif source_system == "METAR":
                if not isinstance(payload, dict):
                    continue
                status = payload.get("status")
                # No weather edge when METAR was unavailable or explicitly had no data.
                if status in ("api_error", "not_fetched", "no_data", "no_disruption", "normal_operations"):
                    continue
                edge_type = "AIRPORT_WEATHER_RISK"
                attrs = {
                    "flight_category": payload.get("flight_category"),
                    "wind_speed": payload.get("wind_speed"),
                    "wind_gust": payload.get("wind_gust"),
                    "wind_direction": payload.get("wind_direction"),
                    "visibility_miles": payload.get("visibility_miles"),
                    "ceiling_feet": payload.get("ceiling_feet"),
                    "ceiling_type": payload.get("ceiling_type"),
                    "weather": payload.get("weather"),
                    "raw_metar": payload.get("raw_text"),
                }

            elif source_system == "NWS_ALERTS":
                # NWS alerts is a list; a status wrapper dict means "no alerts" or error.
                if isinstance(payload, dict):
                    status = payload.get("status")
                    if status in ("api_error", "not_fetched"):
                        continue
                    if status in ("no_data", "no_disruption", "normal_operations"):
                        continue

                alerts = payload if isinstance(payload, list) else [payload]
                for alert in alerts:
                    if alert:
                        edge = self.graph.create_edge(
                            src=airport_node.id,
                            dst=airport_node.id,
                            type="AIRPORT_HAS_NWS_ALERT",
                            source_system=source_system,
                            attrs={
                                "event": alert.get("event"),
                                "severity": alert.get("severity"),
                                "certainty": alert.get("certainty"),
                                "urgency": alert.get("urgency"),
                                "headline": alert.get("headline"),
                            },
                            status="DRAFT",
                            confidence=0.85,
                        )
                        self.graph.add_edge_evidence(edge.id, evidence_id)
                        self.graph.promote_edge_to_fact(edge.id)
                        belief_state.edge_ids.append(edge.id)
                continue  # Already handled

            elif source_system == "OPENSKY":
                if not isinstance(payload, dict):
                    continue
                status = payload.get("status")
                # Mirror derive_from_ingestion(): no movement edge if the fetch failed.
                if status in ("api_error", "not_fetched"):
                    continue
                edge_type = "AIRPORT_MOVEMENT_COLLAPSE"
                attrs = {
                    "aircraft_count": payload.get("aircraft_count"),
                    "timestamp": payload.get("time"),
                    "retrieved_at": payload.get("retrieved_at"),
                }

            # Create edge if we have one
            if edge_type:
                edge = self.graph.create_edge(
                    src=airport_node.id,
                    dst=airport_node.id,
                    type=edge_type,
                    source_system=source_system,
                    attrs=attrs,
                    status="DRAFT",
                    confidence=0.9,
                )
                self.graph.add_edge_evidence(edge.id, evidence_id)
                self.graph.promote_edge_to_fact(edge.id)
                belief_state.edge_ids.append(edge.id)

    def _create_evidence_record(
        self,
        source_system: str,
        payload_sha256: str,
        retrieved_at: datetime,
        airport_icao: str = None,
        excerpt: str = None,
    ) -> UUID:
        """Create evidence record in database and link to case via trace_event."""
        from uuid import uuid4
        from ...evidence.store import EVIDENCE_ROOT

        evidence_id = uuid4()
        # Use airport as source_ref for caching, fall back to case_id
        source_ref = f"airport:{airport_icao}" if airport_icao else f"case:{self.case_id}"
        # Build consistent raw_path using EVIDENCE_ROOT
        raw_path = str(EVIDENCE_ROOT / f"{payload_sha256}.bin")

        result = self.session.execute(
            text("""
                INSERT INTO evidence
                (id, source_system, source_ref, retrieved_at, content_type,
                 payload_sha256, raw_path, excerpt, meta)
                VALUES
                (:id, :source_system, :source_ref, :retrieved_at, :content_type,
                 :payload_sha256, :raw_path, :excerpt, CAST(:meta AS jsonb))
                ON CONFLICT (source_system, source_ref, payload_sha256) DO UPDATE
                SET retrieved_at = EXCLUDED.retrieved_at
                RETURNING id
            """),
            {
                "id": evidence_id,
                "source_system": source_system,
                "source_ref": source_ref,
                "retrieved_at": retrieved_at,
                "content_type": "application/json",
                "payload_sha256": payload_sha256,
                "raw_path": raw_path,
                "excerpt": excerpt,
                "meta": json.dumps({}),
            }
        )
        # Use actual ID (may be existing row on conflict)
        row = result.fetchone()
        if row:
            evidence_id = row[0]

        # Link evidence to case via trace_event
        from ...db.engine import get_next_trace_seq
        seq = get_next_trace_seq(self.case_id, self.session)
        self.session.execute(
            text("""
                INSERT INTO trace_event
                (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                VALUES
                (:id, :case_id, :seq, :event_type, :ref_type, :ref_id,
                 CAST(:meta AS jsonb), :created_at)
            """),
            {
                "id": uuid4(),
                "case_id": self.case_id,
                "seq": seq,
                "event_type": "TOOL_RESULT",
                "ref_type": "evidence",
                "ref_id": evidence_id,
                "meta": json.dumps({"source_system": source_system}),
                "created_at": datetime.now(timezone.utc),
            }
        )

        self.session.commit()
        return evidence_id

    def _link_orphan_missing_evidence(self, airport_icao: str) -> None:
        """
        Link orphan missing_evidence_request records from pre-case ingestion.

        When /ingest/airport/{icao} fails, it persists missing_evidence_request
        with case_id=NULL. When a case starts investigating that airport, we
        link those orphan records to this case so the packet builder sees them.
        """
        self.session.execute(
            text("""
                UPDATE missing_evidence_request
                SET case_id = :case_id
                WHERE case_id IS NULL
                  AND request_type LIKE :pattern
                  AND resolved_at IS NULL
            """),
            {
                "case_id": self.case_id,
                "pattern": f"airport_ingestion:{airport_icao}",
            }
        )
        self.session.commit()

    def _link_booking_evidence(self, airport_icao: str) -> None:
        """
        Discover and link booking evidence to this case.

        Booking evidence (source_system='BOOKING') is created by the graph seeder
        or an internal booking system. PolicyJudge._has_booking_evidence() checks
        for these via trace_event, so we need to create the trace_event links.

        We find bookings for shipments that transit through this airport by
        joining through the graph: EVIDENCE(meta.shipment_id) -> SHIPMENT -> FLIGHT -> AIRPORT.
        """
        from ...db.engine import get_next_trace_seq
        from uuid import uuid4

        # Find booking evidence relevant to this airport and not yet linked to this case.
        # IMPORTANT: Do not blindly link "all booking evidence" — it creates confusing packets.
        result = self.session.execute(
            text("""
                SELECT DISTINCT e.id
                FROM evidence e
                JOIN node s
                  ON s.type = 'SHIPMENT'
                 AND s.identifier = (e.meta->>'shipment_id')
                JOIN edge es
                  ON es.src = s.id
                 AND es.type = 'SHIPMENT_ON_FLIGHT'
                JOIN node f
                  ON f.id = es.dst
                 AND f.type = 'FLIGHT'
                JOIN edge ef
                  ON ef.src = f.id
                 AND ef.type IN ('FLIGHT_DEPARTS_FROM', 'FLIGHT_ARRIVES_AT')
                JOIN node a
                  ON a.id = ef.dst
                 AND a.type = 'AIRPORT'
                 AND a.identifier = :icao
                WHERE e.source_system = 'BOOKING'
                  AND e.id NOT IN (
                      SELECT ref_id::uuid
                      FROM trace_event
                      WHERE case_id = :case_id
                        AND ref_type = 'evidence'
                  )
                LIMIT 100
            """),
            {"case_id": self.case_id, "icao": airport_icao.upper()}
        )
        booking_evidence_ids = [row[0] for row in result]

        for evidence_id in booking_evidence_ids:
            seq = get_next_trace_seq(self.case_id, self.session)
            self.session.execute(
                text("""
                    INSERT INTO trace_event
                    (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                    VALUES
                    (:id, :case_id, :seq, 'TOOL_RESULT', 'evidence', :ref_id,
                     CAST(:meta AS jsonb), :created_at)
                """),
                {
                    "id": uuid4(),
                    "case_id": self.case_id,
                    "seq": seq,
                    "event_type": "TOOL_RESULT",
                    "ref_type": "evidence",
                    "ref_id": evidence_id,
                    "meta": json.dumps({"source_system": "BOOKING"}),
                    "created_at": datetime.now(timezone.utc),
                }
            )
        if booking_evidence_ids:
            self.session.commit()
            # Resolve any booking-related missing evidence blockers once we have booking evidence.
            self._resolve_missing_evidence_for_source(
                source_system="INTERNAL_BOOKING",
                evidence_id=booking_evidence_ids[0],
            )
            self.session.commit()

    def _create_missing_evidence_request(self, missing: Any) -> UUID:
        """Create missing evidence request record."""
        from uuid import uuid4

        request_id = uuid4()

        return self._upsert_missing_evidence_request(
            source_system=missing.source_system,
            request_type=missing.request_type,
            request_params=missing.request_params or {},
            reason=missing.reason,
            criticality=missing.criticality,
            request_id=request_id,
        )

    def _upsert_missing_evidence_request(
        self,
        source_system: str,
        request_type: str,
        request_params: Dict[str, Any],
        reason: str,
        criticality: str,
        request_id: Optional[UUID] = None,
    ) -> UUID:
        """
        Insert (or update) an unresolved missing_evidence_request for this case.

        We prefer updating an existing unresolved request to avoid spamming the table
        during retry loops.
        """
        from uuid import uuid4

        if request_id is None:
            request_id = uuid4()

        params_json = json.dumps(request_params or {})

        # Update existing unresolved request if present
        update_result = self.session.execute(
            text("""
                UPDATE missing_evidence_request
                SET reason = :reason,
                    criticality = :criticality
                WHERE case_id = :case_id
                  AND source_system = :source_system
                  AND request_type = :request_type
                  AND request_params = CAST(:request_params AS jsonb)
                  AND resolved_at IS NULL
                RETURNING id
            """),
            {
                "case_id": self.case_id,
                "source_system": source_system,
                "request_type": request_type,
                "request_params": params_json,
                "reason": reason,
                "criticality": criticality,
            }
        )
        row = update_result.fetchone()
        if row and row[0]:
            return row[0]

        # No unresolved request found -> insert new
        self.session.execute(
            text("""
                INSERT INTO missing_evidence_request
                (id, case_id, source_system, request_type, request_params,
                 reason, criticality, created_at)
                VALUES
                (:id, :case_id, :source_system, :request_type, CAST(:request_params AS jsonb),
                 :reason, :criticality, :created_at)
            """),
            {
                "id": request_id,
                "case_id": self.case_id,
                "source_system": source_system,
                "request_type": request_type,
                "request_params": params_json,
                "reason": reason,
                "criticality": criticality,
                "created_at": datetime.now(timezone.utc),
            }
        )
        return request_id

    def _resolve_missing_evidence_for_source(self, source_system: str, evidence_id: UUID) -> None:
        """Resolve any unresolved missing_evidence_request rows for a source in this case."""
        self.session.execute(
            text("""
                UPDATE missing_evidence_request
                SET resolved_at = :resolved_at,
                    resolved_by_evidence_id = :evidence_id
                WHERE case_id = :case_id
                  AND source_system = :source_system
                  AND resolved_at IS NULL
            """),
            {
                "case_id": self.case_id,
                "source_system": source_system,
                "resolved_at": datetime.now(timezone.utc),
                "evidence_id": evidence_id,
            }
        )

    def _log_edge_trace_events(self, edge_ids: List[UUID]) -> None:
        """
        Log trace_events with ref_type='edge' for each edge.

        FIXED: This is required for /graph/case/{case_id} to return edges.
        The graph endpoints query trace_event WHERE ref_type='edge', but
        nothing was writing those events. Now we do.
        """
        from uuid import uuid4
        from ...db.engine import get_next_trace_seq

        for edge_id in edge_ids:
            seq = get_next_trace_seq(self.case_id, self.session)
            self.session.execute(
                text("""
                    INSERT INTO trace_event
                    (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                    VALUES
                    (:id, :case_id, :seq, 'TOOL_RESULT', 'edge', :ref_id,
                     CAST(:meta AS jsonb), :created_at)
                """),
                {
                    "id": uuid4(),
                    "case_id": self.case_id,
                    "seq": seq,
                    "ref_id": edge_id,
                    "meta": json.dumps({"edge_created": True}),
                    "created_at": datetime.now(timezone.utc),
                }
            )
        self.session.commit()

    def _identify_uncertainties(self, belief_state: BeliefState) -> None:
        """
        Identify uncertainties based on available evidence.

        NOTE: We check for EVIDENCE SOURCE presence, not edge types.
        This is because "no disruption" or "no alerts" is still valid evidence -
        it just doesn't create an edge. We only have uncertainty if we
        completely lack evidence from a source.
        """
        # Get evidence sources we have for this case
        evidence_sources = self._get_evidence_sources()
        failed_sources = self._get_failed_evidence_sources()

        # Check which sources we have evidence from
        has_faa = "FAA_NAS" in evidence_sources and "FAA_NAS" not in failed_sources
        # Current conditions are driven by METAR; TAF alone is not sufficient.
        has_metar = "METAR" in evidence_sources and "METAR" not in failed_sources
        has_nws = "NWS_ALERTS" in evidence_sources and "NWS_ALERTS" not in failed_sources
        has_opensky = "OPENSKY" in evidence_sources and "OPENSKY" not in failed_sources

        # Only add uncertainties for COMPLETELY missing data sources
        # "No disruption" or "no alerts" is NOT uncertainty - it's evidence of normal conditions
        if not has_faa and not any(u.id == "airport_status_unknown" for u in belief_state.uncertainties):
            belief_state.add_uncertainty(Uncertainty(
                id="airport_status_unknown",
                question="What is the current FAA status of the airport?",
                uncertainty_type="airport_status_unknown",
            ))

        if not has_metar and not any(u.id == "weather_conditions_unknown" for u in belief_state.uncertainties):
            belief_state.add_uncertainty(Uncertainty(
                id="weather_conditions_unknown",
                question="What are the current weather conditions?",
                uncertainty_type="weather_conditions_unknown",
            ))

        if not has_nws and not any(u.id == "alert_status_unknown" for u in belief_state.uncertainties):
            belief_state.add_uncertainty(Uncertainty(
                id="alert_status_unknown",
                question="Are there any active weather alerts?",
                uncertainty_type="alert_status_unknown",
            ))

        if not has_opensky and not any(u.id == "movement_data_unknown" for u in belief_state.uncertainties):
            belief_state.add_uncertainty(Uncertainty(
                id="movement_data_unknown",
                question="What is the current aircraft traffic level?",
                uncertainty_type="movement_data_unknown",
            ))

    def _get_evidence_sources(self) -> set:
        """
        Get the set of source systems we have evidence from for this case.

        Returns set of source_system names (e.g., {"FAA_NAS", "METAR", "OPENSKY"}).
        """
        result = self.session.execute(
            text("""
                SELECT DISTINCT e.source_system
                FROM evidence e
                JOIN trace_event t ON t.ref_id::text = e.id::text
                WHERE t.case_id = :case_id
                  AND t.ref_type = 'evidence'
            """),
            {"case_id": self.case_id}
        )
        return {row[0] for row in result}

    def _get_failed_evidence_sources(self) -> set:
        """
        Get sources whose LATEST evidence indicates a fetch failure.

        We treat status markers like "api_error" and "not_fetched" as unknown
        (i.e., we know we don't know) so the agent can attempt targeted re-fetch.
        """
        result = self.session.execute(
            text("""
                SELECT DISTINCT ON (e.source_system) e.source_system, e.excerpt
                FROM evidence e
                JOIN trace_event t ON t.ref_id::text = e.id::text
                WHERE t.case_id = :case_id
                  AND t.ref_type = 'evidence'
                ORDER BY e.source_system, e.retrieved_at DESC
            """),
            {"case_id": self.case_id}
        )

        failed = set()
        for source_system, excerpt in result:
            ex = excerpt or ""
            if '"status": "api_error"' in ex or '"status":"api_error"' in ex:
                failed.add(source_system)
            elif '"status": "not_fetched"' in ex or '"status":"not_fetched"' in ex:
                failed.add(source_system)
        return failed

    def _detect_contradictions(self, belief_state: BeliefState) -> None:
        """
        Detect contradictions between different signals.

        Contradiction types:
        - FAA says normal, but METAR shows IFR conditions
        - FAA says ground stop, but OpenSky shows high movement
        - NWS severe alert, but FAA shows normal
        """
        from uuid import uuid4
        from datetime import datetime, timezone
        from ..state_graph import ContradictionRef

        if not belief_state.edge_ids:
            return

        # Get all edge data
        result = self.session.execute(
            text("""
                SELECT id, type, attrs FROM edge
                WHERE id = ANY(:edge_ids)
            """),
            {"edge_ids": belief_state.edge_ids}
        )

        # Group edges by type - use lists to support multiple edges of same type (e.g., NWS alerts)
        edges_by_type: Dict[str, list] = {}
        for row in result:
            edge_type = row[1]
            edge_data = {"id": row[0], "attrs": row[2]}
            if edge_type not in edges_by_type:
                edges_by_type[edge_type] = []
            edges_by_type[edge_type].append(edge_data)

        # Check for contradictions - get first edge for single-value types
        faa_list = edges_by_type.get("AIRPORT_HAS_FAA_DISRUPTION", [])
        faa = faa_list[0].get("attrs", {}) if faa_list else {}
        weather_list = edges_by_type.get("AIRPORT_WEATHER_RISK", [])
        weather = weather_list[0].get("attrs", {}) if weather_list else {}
        movement_list = edges_by_type.get("AIRPORT_MOVEMENT_COLLAPSE", [])
        movement = movement_list[0].get("attrs", {}) if movement_list else {}
        # NWS alerts can have multiple entries
        alerts = edges_by_type.get("AIRPORT_HAS_NWS_ALERT", [])

        contradictions_found = []

        # Contradiction 1: FAA normal but weather is IFR/LIFR
        if not faa.get("delay") and not faa.get("closure"):
            flight_cat = weather.get("flight_category", "VFR")
            if flight_cat in ["IFR", "LIFR"]:
                contradictions_found.append({
                    "type": "FAA_WEATHER_MISMATCH",
                    "description": f"FAA reports normal operations but weather shows {flight_cat} conditions",
                    "severity": "MEDIUM",
                })

        # Contradiction 2: FAA ground stop but high aircraft movement
        if faa.get("closure") or faa.get("delay_type") == "Ground Stop":
            ac_raw = movement.get("aircraft_count", 0)
            try:
                aircraft_count = int(ac_raw) if ac_raw else 0
            except (ValueError, TypeError):
                aircraft_count = 0

            if aircraft_count > 50:
                contradictions_found.append({
                    "type": "FAA_MOVEMENT_MISMATCH",
                    "description": f"FAA reports ground stop but {aircraft_count} aircraft detected nearby",
                    "severity": "HIGH",
                })

        # Contradiction 3: Severe NWS alert but FAA normal
        for alert in alerts:
            alert_attrs = alert.get("attrs", {})
            if alert_attrs.get("severity") in ["Severe", "Extreme"]:
                if not faa.get("delay") and not faa.get("closure"):
                    contradictions_found.append({
                        "type": "NWS_FAA_MISMATCH",
                        "description": f"NWS {alert_attrs.get('severity')} alert but FAA reports normal",
                        "severity": "HIGH",
                    })

        # Contradiction 4: Low visibility but high aircraft count
        visibility_raw = weather.get("visibility_miles", 10)
        try:
            visibility = float(visibility_raw) if visibility_raw is not None else 10
        except (ValueError, TypeError):
            visibility = 10

        if visibility < 1:
            aircraft_count_raw = movement.get("aircraft_count", 0)
            try:
                aircraft_count = int(aircraft_count_raw) if aircraft_count_raw else 0
            except (ValueError, TypeError):
                aircraft_count = 0

            if aircraft_count > 30:
                contradictions_found.append({
                    "type": "VISIBILITY_MOVEMENT_MISMATCH",
                    "description": f"Visibility {visibility}mi but {aircraft_count} aircraft active",
                    "severity": "MEDIUM",
                })

        # Create contradiction records in database
        for c in contradictions_found:
            contradiction_id = uuid4()

            # Create two opposing claims
            claim_a_id = uuid4()
            claim_b_id = uuid4()

            # Insert claims
            self.session.execute(
                text("""
                    INSERT INTO claim (id, text, confidence, status, ingested_at)
                    VALUES (:id, :text, :confidence, 'HYPOTHESIS', :created_at)
                """),
                {
                    "id": claim_a_id,
                    "text": f"Signal A: {c['description'].split(' but ')[0]}",
                    "confidence": 0.8,
                    "created_at": datetime.now(timezone.utc),
                }
            )
            self.session.execute(
                text("""
                    INSERT INTO claim (id, text, confidence, status, ingested_at)
                    VALUES (:id, :text, :confidence, 'HYPOTHESIS', :created_at)
                """),
                {
                    "id": claim_b_id,
                    "text": f"Signal B: {c['description'].split(' but ')[1] if ' but ' in c['description'] else c['description']}",
                    "confidence": 0.8,
                    "created_at": datetime.now(timezone.utc),
                }
            )

            # Insert contradiction record
            self.session.execute(
                text("""
                    INSERT INTO contradiction
                    (id, claim_a, claim_b, detected_at, resolution_status)
                    VALUES (:id, :claim_a, :claim_b, :detected_at, 'OPEN')
                """),
                {
                    "id": contradiction_id,
                    "claim_a": claim_a_id,
                    "claim_b": claim_b_id,
                    "detected_at": datetime.now(timezone.utc),
                }
            )

            # Add to belief state
            belief_state.add_contradiction(ContradictionRef(
                claim_a=claim_a_id,
                claim_b=claim_b_id,
                contradiction_type=c["type"],
                why_it_matters=c["description"],
                resolved=False,
            ))

            # Create trace event
            from ...db.engine import get_next_trace_seq
            seq = get_next_trace_seq(self.case_id, self.session)
            self.session.execute(
                text("""
                    INSERT INTO trace_event
                    (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                    VALUES
                    (:id, :case_id, :seq, 'TOOL_RESULT', 'contradiction', :ref_id,
                     CAST(:meta AS jsonb), :created_at)
                """),
                {
                    "id": uuid4(),
                    "case_id": self.case_id,
                    "seq": seq,
                    "event_type": "TOOL_RESULT",
                    "ref_id": contradiction_id,
                    "meta": json.dumps(c),
                    "created_at": datetime.now(timezone.utc),
                }
            )

        if contradictions_found:
            self.session.commit()

    def _build_hypotheses(self, belief_state: BeliefState) -> None:
        """Build hypotheses from available evidence."""
        from uuid import uuid4

        # Get disruption signals
        result = self.session.execute(
            text("""
                SELECT e.type, e.attrs, e.confidence
                FROM edge e
                WHERE e.id = ANY(:edge_ids)
                  AND e.type LIKE 'AIRPORT%'
            """),
            {"edge_ids": belief_state.edge_ids}
        )

        for row in result:
            edge_type, attrs, confidence = row
            severity = attrs.get("severity", "UNKNOWN")

            hypothesis = Hypothesis(
                id=uuid4(),
                text=f"Airport has {edge_type} with severity {severity}",
                confidence=confidence,
                supporting_claim_ids=[],
            )
            belief_state.add_hypothesis(hypothesis)

    def _investigate_uncertainty(
        self,
        uncertainty: Uncertainty,
        belief_state: BeliefState,
    ) -> None:
        """
        Attempt to resolve a specific uncertainty by fetching targeted data.

        FIXED: Now actually fetches data from the source instead of just incrementing tool calls.
        """
        from uuid import uuid4

        # Map uncertainty types to registry method names
        source_map = {
            "airport_status_unknown": ("_ingest_faa", "FAA_NAS"),
            "weather_conditions_unknown": ("_ingest_metar", "METAR"),
            "alert_status_unknown": ("_ingest_nws", "NWS_ALERTS"),
            "movement_data_unknown": ("_ingest_opensky", "OPENSKY"),
        }

        source_info = source_map.get(uncertainty.uncertainty_type)
        if not source_info:
            return

        method_name, source_system = source_info

        # Get case scope
        case_scope = self._get_case_scope()
        if not case_scope:
            return

        airport_icao = case_scope.get("airport")
        if not airport_icao:
            return

        # Call the specific ingestion method
        ingestion_method = getattr(self.registry, method_name)
        if method_name == "_ingest_opensky":
            ingestion_result = ingestion_method(airport_icao, str(self.case_id))
        else:
            ingestion_result = ingestion_method(airport_icao)

        # Store evidence
        if ingestion_result is None:
            belief_state.increment_tool_calls(1)
            return

        # Create evidence record
        if ingestion_result.success and ingestion_result.data:
            if isinstance(ingestion_result.data, list):
                if len(ingestion_result.data) > 0:
                    raw_bytes = json.dumps(
                        [item.__dict__ if hasattr(item, '__dict__') else item
                         for item in ingestion_result.data],
                        default=str
                    ).encode('utf-8')
                else:
                    raw_bytes = json.dumps({
                        "status": "no_data",
                        "source": source_system,
                        "message": f"No active data from {source_system}"
                    }).encode('utf-8')
            else:
                raw_bytes = json.dumps(
                    ingestion_result.data.__dict__ if hasattr(ingestion_result.data, '__dict__')
                    else str(ingestion_result.data),
                    default=str
                ).encode('utf-8')
        elif ingestion_result.success:
            raw_bytes = json.dumps({
                "status": "no_disruption",
                "source": source_system,
                "message": f"No disruptions from {source_system}"
            }).encode('utf-8')
        else:
            raw_bytes = json.dumps({
                "status": "api_error",
                "source": source_system,
                "error": ingestion_result.error or "Unknown error"
            }).encode('utf-8')

        sha256 = store_evidence(raw_bytes)
        excerpt = extract_excerpt(raw_bytes)
        evidence_id = self._create_evidence_record(
            source_system=source_system,
            payload_sha256=sha256,
            retrieved_at=ingestion_result.retrieved_at,
            airport_icao=airport_icao,
            excerpt=excerpt,
        )

        belief_state.evidence_ids.append(evidence_id)
        if ingestion_result.success:
            belief_state.valid_evidence_ids.append(evidence_id)
            self._resolve_missing_evidence_for_source(source_system, evidence_id)
        else:
            belief_state.error_evidence_ids.append(evidence_id)
            self._upsert_missing_evidence_request(
                source_system=source_system,
                request_type=f"airport_ingestion:{airport_icao}",
                request_params={"airport_icao": airport_icao},
                reason=ingestion_result.error or "Unknown error",
                criticality=INGESTION_CRITICALITY_BY_SOURCE.get(source_system, "DEGRADED"),
            )

        self.session.commit()

        # Mark uncertainty as resolved
        # Only mark resolved when we successfully fetched the source.
        # A failed attempt is still "unknown" (we know we don't know).
        uncertainty.resolved = bool(ingestion_result.success)
        if uncertainty.resolved:
            uncertainty.resolved_by_evidence_id = evidence_id

        # Log resolution in trace_event
        from ...db.engine import get_next_trace_seq
        seq = get_next_trace_seq(self.case_id, self.session)
        self.session.execute(
            text("""
                INSERT INTO trace_event
                (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                VALUES
                (:id, :case_id, :seq, 'TOOL_RESULT', 'evidence', :ref_id,
                 CAST(:meta AS jsonb), :created_at)
            """),
            {
                "id": uuid4(),
                "case_id": self.case_id,
                "seq": seq,
                "ref_id": evidence_id,
                "meta": json.dumps({
                    "source_system": source_system,
                    "uncertainty_type": uncertainty.uncertainty_type,
                    "uncertainty_resolved": bool(ingestion_result.success),
                }),
                "created_at": datetime.now(timezone.utc),
            }
        )
        self.session.commit()

        belief_state.increment_tool_calls(1)
