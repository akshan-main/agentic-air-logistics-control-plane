# app/packets/builder.py
"""
Decision packet builder.
"""

from typing import Optional, Dict, Any, List
from uuid import UUID
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import SessionLocal
from ..agents.state_graph import BeliefState, Posture
from ..graph.visibility import edge_visible_at, get_visibility_params
from .models import (
    DecisionPacket,
    PacketMetrics,
    PostureAction,
    ClaimSummary,
    EvidenceSummary,
    ContradictionSummary,
    PolicyReference,
    ActionSummary,
    OutcomeSummary,
    BlockedInfo,
)


class DecisionPacketBuilder:
    """
    Builds decision packets from case data.
    """

    def __init__(self, case_id: UUID, session: Session):
        self.case_id = case_id
        self.session = session

    def build(
        self,
        belief_state: Optional[BeliefState] = None,
        risk_assessment: Optional[Dict[str, Any]] = None,
    ) -> DecisionPacket:
        """
        Build complete decision packet.

        Args:
            belief_state: Optional belief state
            risk_assessment: Optional risk assessment

        Returns:
            DecisionPacket
        """
        # Get case info
        case_info = self._get_case_info()

        # Get posture decision
        posture_action = self._get_posture_action(belief_state, risk_assessment)

        # Get claims
        claims = self._get_claims()

        # Get evidence
        evidence = self._get_evidence()

        # Get contradictions
        contradictions = self._get_contradictions()

        # Get policies
        policies = self._get_policies()

        # Get actions
        actions_proposed, actions_executed = self._get_actions()

        # Get blocked info
        blocked_info = self._get_blocked_info()

        # Build metrics
        metrics = self._build_metrics(evidence, belief_state)

        # Get workflow trace
        workflow_trace = self._get_workflow_trace()

        # Get confidence breakdown from risk assessment or trace_event
        confidence_breakdown = None
        if risk_assessment:
            confidence_breakdown = risk_assessment.get("confidence_breakdown")
        if not confidence_breakdown:
            # Try to retrieve from stored trace_event
            confidence_breakdown = self._get_confidence_breakdown_from_trace()

        # Get cascade impact (operational data)
        cascade_impact = self._get_cascade_impact()

        now = datetime.now(timezone.utc)

        return DecisionPacket(
            case_id=self.case_id,
            case_type=case_info.get("case_type", "AIRPORT_DISRUPTION"),
            scope=case_info.get("scope", {}),
            created_at=case_info.get("created_at", now),
            completed_at=now,
            posture_decision=posture_action,
            top_claims=claims,
            evidence_list=evidence,
            contradictions=contradictions,
            policies_applied=policies,
            actions_proposed=actions_proposed,
            actions_executed=actions_executed,
            blocked_section=blocked_info,
            metrics=metrics,
            workflow_trace=workflow_trace,
            confidence_breakdown=confidence_breakdown,
            cascade_impact=cascade_impact,
        )

    def _get_case_info(self) -> Dict[str, Any]:
        """Get case information."""
        result = self.session.execute(
            text('SELECT case_type, scope, created_at, status FROM "case" WHERE id = :id'),
            {"id": self.case_id}
        )
        row = result.fetchone()
        if row:
            return {
                "case_type": row[0],
                "scope": row[1],
                "created_at": row[2],
                "status": row[3],
            }
        return {}

    def _get_posture_action(
        self,
        belief_state: Optional[BeliefState],
        risk_assessment: Optional[Dict[str, Any]],
    ) -> PostureAction:
        """Get posture decision."""
        # Try to get from actions
        result = self.session.execute(
            text("""
                SELECT args, created_at FROM action
                WHERE case_id = :case_id AND type = 'SET_POSTURE'
                ORDER BY created_at DESC LIMIT 1
            """),
            {"case_id": self.case_id}
        )
        row = result.fetchone()

        if row:
            args = row[0]
            return PostureAction(
                posture=args.get("posture", "HOLD"),
                airport=self._get_airport(),
                effective_at=row[1],
                reason=args.get("reason", ""),
            )

        # Fall back to belief state or risk assessment
        posture = Posture.HOLD
        reason = "Default fallback"
        if belief_state:
            posture = belief_state.current_posture
            reason = "From belief state"
        elif risk_assessment:
            posture_str = risk_assessment.get("recommended_posture", "HOLD")
            posture = Posture[posture_str]
            reason = "From risk assessment"
        else:
            # No live state available — reconstruct from stored trace_events
            # Look for RiskQuant's assessment in trace_events
            risk_trace = self.session.execute(
                text("""
                    SELECT meta FROM trace_event
                    WHERE case_id = :case_id
                      AND ref_type = 'risk_assessment'
                    ORDER BY created_at DESC LIMIT 1
                """),
                {"case_id": self.case_id}
            )
            risk_row = risk_trace.fetchone()
            if risk_row and risk_row[0]:
                meta = risk_row[0] if isinstance(risk_row[0], dict) else {}
                rec = meta.get("recommended_posture")
                if rec and rec in Posture.__members__:
                    posture = Posture[rec]
                    reason = f"From stored risk assessment (case was BLOCKED)"

        return PostureAction(
            posture=posture.value,
            airport=self._get_airport(),
            effective_at=datetime.now(timezone.utc),
            reason=reason,
        )

    def _get_airport(self) -> str:
        """Get airport from case scope."""
        result = self.session.execute(
            text('SELECT scope FROM "case" WHERE id = :id'),
            {"id": self.case_id}
        )
        row = result.fetchone()
        if row and row[0]:
            return row[0].get("airport", "UNKNOWN")
        return "UNKNOWN"

    def _get_claims(self) -> List[ClaimSummary]:
        """Get top claims for packet."""
        result = self.session.execute(
            text("""
                SELECT c.id, c.text, c.status, c.confidence,
                       array_agg(ce.evidence_id) as evidence_ids
                FROM claim c
                LEFT JOIN claim_evidence ce ON ce.claim_id = c.id
                WHERE c.id IN (
                    SELECT ref_id::uuid FROM trace_event
                    WHERE case_id = :case_id AND ref_type = 'claim'
                )
                GROUP BY c.id, c.text, c.status, c.confidence
                ORDER BY c.confidence DESC
            """),
            {"case_id": self.case_id}
        )

        claims = []
        for row in result:
            evidence_ids = [e for e in (row[4] or []) if e is not None]
            claims.append(ClaimSummary(
                claim_id=row[0],
                text=row[1],
                status=row[2],
                confidence=row[3],
                evidence_ids=evidence_ids,
            ))

        return claims

    def _get_evidence(self) -> List[EvidenceSummary]:
        """Get evidence list for packet."""
        result = self.session.execute(
            text("""
                SELECT id, source_system, retrieved_at, excerpt
                FROM evidence
                WHERE id IN (
                    SELECT ref_id::uuid FROM trace_event
                    WHERE case_id = :case_id AND ref_type = 'evidence'
                )
                ORDER BY retrieved_at DESC
            """),
            {"case_id": self.case_id}
        )

        evidence = []
        for row in result:
            evidence.append(EvidenceSummary(
                evidence_id=row[0],
                source_system=row[1],
                retrieved_at=row[2],
                excerpt=row[3],
            ))

        return evidence

    def _get_contradictions(self) -> List[ContradictionSummary]:
        """Get contradictions for packet."""
        # Query contradictions via trace_event with ref_type='contradiction'
        result = self.session.execute(
            text("""
                SELECT c.claim_a, c.claim_b, c.resolution_status, t.meta
                FROM contradiction c
                JOIN trace_event t ON t.ref_id = c.id
                WHERE t.case_id = :case_id AND t.ref_type = 'contradiction'
            """),
            {"case_id": self.case_id}
        )

        contradictions = []
        for row in result:
            meta = row[3] or {}
            contradictions.append(ContradictionSummary(
                claim_a_id=row[0],
                claim_b_id=row[1],
                contradiction_type=meta.get("type", "SIGNAL_MISMATCH"),
                resolution_status=row[2],
            ))

        return contradictions

    def _get_policies(self) -> List[PolicyReference]:
        """Get all applied policies for packet (both successful and failed)."""
        # Get failed policies (GUARDRAIL_FAIL)
        result = self.session.execute(
            text("""
                SELECT DISTINCT t.meta->>'policy_id', t.meta->>'policy_text', t.meta->>'effect'
                FROM trace_event t
                WHERE t.case_id = :case_id
                  AND t.event_type = 'GUARDRAIL_FAIL'
                  AND t.ref_type = 'policy'
            """),
            {"case_id": self.case_id}
        )

        policies = []
        for row in result:
            if row[0]:
                policies.append(PolicyReference(
                    policy_id=row[0],
                    policy_text=row[1] or "",
                    effect=f"BLOCKED: {row[2]}" if row[2] else "BLOCKED",
                ))

        # Get successfully applied policies (TOOL_RESULT with policy_applied)
        result2 = self.session.execute(
            text("""
                SELECT DISTINCT t.meta->>'policy_id', t.meta->>'policy_text', t.meta->>'effect'
                FROM trace_event t
                WHERE t.case_id = :case_id
                  AND t.event_type = 'TOOL_RESULT'
                  AND t.ref_type = 'policy_applied'
            """),
            {"case_id": self.case_id}
        )

        for row in result2:
            if row[0]:
                policies.append(PolicyReference(
                    policy_id=row[0],
                    policy_text=row[1] or "",
                    effect=row[2] or "APPLIED",
                ))

        return policies

    def _get_actions(self) -> tuple:
        """Get actions for packet."""
        result = self.session.execute(
            text("""
                SELECT a.id, a.type, a.args, a.state, a.risk_level,
                       o.success, o.payload
                FROM action a
                LEFT JOIN outcome o ON o.action_id = a.id
                WHERE a.case_id = :case_id
                ORDER BY a.created_at
            """),
            {"case_id": self.case_id}
        )

        proposed = []
        executed = []

        for row in result:
            proposed.append(ActionSummary(
                action_id=row[0],
                action_type=row[1],
                args=row[2],
                state=row[3],
                risk_level=row[4],
            ))

            if row[5] is not None:  # Has outcome
                executed.append(OutcomeSummary(
                    action_id=row[0],
                    success=row[5],
                    payload=row[6] or {},
                ))

        return proposed, executed

    def _get_blocked_info(self) -> Optional[BlockedInfo]:
        """Get blocked info if case is blocked."""
        result = self.session.execute(
            text('SELECT status FROM "case" WHERE id = :id'),
            {"id": self.case_id}
        )
        row = result.fetchone()

        if not row or row[0] != "BLOCKED":
            return None

        # Get missing evidence requests
        mer_result = self.session.execute(
            text("""
                SELECT source_system, request_type, reason, criticality
                FROM missing_evidence_request
                WHERE case_id = :case_id AND resolved_at IS NULL
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
            for r in mer_result
        ]

        return BlockedInfo(
            reason="Case blocked due to missing evidence",
            missing_evidence_requests=missing,
        )

    def _get_workflow_trace(self) -> List[Dict[str, Any]]:
        """Get workflow state transitions for this case."""
        result = self.session.execute(
            text("""
                SELECT event_type, ref_type, meta, created_at
                FROM trace_event
                WHERE case_id = :case_id
                  AND event_type IN ('STATE_ENTER', 'STATE_EXIT', 'TOOL_CALL', 'HANDOFF')
                ORDER BY seq, created_at
            """),
            {"case_id": self.case_id}
        )

        trace = []
        for row in result:
            event_type, ref_type, meta, created_at = row
            meta_dict = meta or {}
            # State name is stored in meta["state"] for state transitions
            state_name = meta_dict.get("state", ref_type)
            trace.append({
                "event_type": event_type,
                "state": state_name,
                "meta": meta_dict,
                "timestamp": created_at.isoformat() if created_at else None,
            })

        return trace

    def _get_confidence_breakdown_from_trace(self) -> Optional[Dict[str, Any]]:
        """Get confidence breakdown from stored risk assessment trace event."""
        result = self.session.execute(
            text("""
                SELECT meta->'confidence_breakdown' as breakdown
                FROM trace_event
                WHERE case_id = :case_id
                  AND ref_type = 'risk_assessment'
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"case_id": self.case_id}
        )
        row = result.fetchone()
        return row[0] if row and row[0] else None

    def _get_cascade_impact(self) -> Optional[Dict[str, Any]]:
        """
        Get cascade impact analysis from operational data.

        Shows context graph traversal: Airport -> Flights -> Shipments -> Bookings -> Carriers
        Financial exposure is forwarder revenue (booking charges), NOT item value.

        USES CANONICAL VISIBILITY PREDICATES for bi-temporal correctness.
        """
        from datetime import timedelta

        # Get airport from case scope
        airport_icao = self._get_airport()
        if airport_icao == "UNKNOWN":
            return None

        now = datetime.now(timezone.utc)
        deadline_24h = now + timedelta(hours=24)

        # Use canonical visibility predicates for bi-temporal queries
        # Query current state: at_event_time=now, at_ingest_time=now
        edge_visibility = edge_visible_at(now, now, table_alias="e")
        visibility_params = get_visibility_params(now, now)

        try:
            # Get flights departing from/arriving at this airport
            # Context graph traversal: AIRPORT <- FLIGHT (via FLIGHT_DEPARTS_FROM/ARRIVES_AT)
            # FIXED: Using canonical edge visibility predicate
            flights_result = self.session.execute(
                text(f"""
                    SELECT
                        fv.attrs->>'flight_number' as flight_number,
                        fv.attrs->>'status' as status,
                        fv.attrs->>'origin' as origin,
                        fv.attrs->>'destination' as destination,
                        fv.attrs->>'carrier_id' as carrier_id
                    FROM node f
                    JOIN node_version fv ON fv.node_id = f.id
                        AND fv.valid_from <= :at_event_time
                        AND (fv.valid_to IS NULL OR fv.valid_to > :at_event_time)
                    JOIN edge e ON e.src = f.id
                    JOIN node a ON e.dst = a.id
                    WHERE f.type = 'FLIGHT'
                      AND a.type = 'AIRPORT'
                      AND a.identifier = :icao
                      AND e.type IN ('FLIGHT_DEPARTS_FROM', 'FLIGHT_ARRIVES_AT')
                      AND {edge_visibility}
                    ORDER BY f.created_at DESC
                    LIMIT 20
                """),
                {"icao": airport_icao, **visibility_params}
            )
            flights = []
            carrier_ids = set()
            for r in flights_result:
                flights.append({
                    "flight_number": r[0],
                    "status": r[1] or "SCHEDULED",
                    "origin": r[2],
                    "destination": r[3],
                })
                if r[4]:
                    carrier_ids.add(r[4])

            # Get carriers involved (context graph: CARRIER -> FLIGHT via CARRIER_OPERATES_FLIGHT)
            # FIXED: Using canonical node_version visibility
            carriers = []
            if carrier_ids:
                carriers_result = self.session.execute(
                    text("""
                        SELECT
                            cv.attrs->>'name' as name,
                            cv.attrs->>'iata_code' as iata_code,
                            c.identifier as carrier_id
                        FROM node c
                        JOIN node_version cv ON cv.node_id = c.id
                            AND cv.valid_from <= :at_event_time
                            AND (cv.valid_to IS NULL OR cv.valid_to > :at_event_time)
                        WHERE c.type = 'CARRIER'
                          AND c.identifier = ANY(:carrier_ids)
                    """),
                    {"carrier_ids": list(carrier_ids), **visibility_params}
                )
                carriers = [
                    {"name": r[0] or r[2], "iata_code": r[1] or ""}
                    for r in carriers_result
                ]

            # Get shipments with booking data (forwarder revenue, NOT item value)
            # Context graph traversal:
            #   SHIPMENT -> FLIGHT (via SHIPMENT_ON_FLIGHT)
            #   BOOKING -> SHIPMENT (via BOOKING_FOR_SHIPMENT)
            # FIXED: Using canonical visibility predicates for all edges
            es_visibility = edge_visible_at(now, now, table_alias="es")
            ef_visibility = edge_visible_at(now, now, table_alias="ef")
            shipments_result = self.session.execute(
                text(f"""
                    SELECT
                        sv.attrs->>'tracking_number' as tracking_number,
                        sv.attrs->>'commodity' as commodity,
                        sv.attrs->>'weight_kg' as weight_kg,
                        sv.attrs->>'service_level' as service_level,
                        bv.attrs->>'total_charge_usd' as total_charge_usd,
                        bv.attrs->>'sla_deadline' as sla_deadline,
                        bv.attrs->>'customer_id' as customer_id
                    FROM node s
                    JOIN node_version sv ON sv.node_id = s.id
                        AND sv.valid_from <= :at_event_time
                        AND (sv.valid_to IS NULL OR sv.valid_to > :at_event_time)
                    JOIN edge es ON es.src = s.id
                    JOIN node f ON es.dst = f.id
                    JOIN edge ef ON ef.src = f.id
                    JOIN node a ON ef.dst = a.id
                    LEFT JOIN edge eb ON eb.dst = s.id AND eb.type = 'BOOKING_FOR_SHIPMENT'
                    LEFT JOIN node b ON eb.src = b.id
                    LEFT JOIN node_version bv ON bv.node_id = b.id
                        AND bv.valid_from <= :at_event_time
                        AND (bv.valid_to IS NULL OR bv.valid_to > :at_event_time)
                    WHERE s.type = 'SHIPMENT'
                      AND f.type = 'FLIGHT'
                      AND a.type = 'AIRPORT'
                      AND a.identifier = :icao
                      AND es.type = 'SHIPMENT_ON_FLIGHT'
                      AND {es_visibility}
                      AND {ef_visibility}
                      AND ef.type IN ('FLIGHT_DEPARTS_FROM', 'FLIGHT_ARRIVES_AT')
                    ORDER BY bv.attrs->>'sla_deadline' ASC NULLS LAST
                    LIMIT 30
                """),
                {"icao": airport_icao, **visibility_params}
            )

            shipments = []
            sla_exposure = []
            total_weight = 0.0
            total_revenue = 0.0  # Forwarder revenue (booking charges), NOT item value
            bookings_count = 0
            sla_breaches_imminent = 0

            for r in shipments_result:
                weight = float(r[2]) if r[2] else 0
                booking_charge = float(r[4]) if r[4] else 0
                sla_deadline = r[5]

                total_weight += weight
                total_revenue += booking_charge
                if r[4]:  # Has booking
                    bookings_count += 1

                # Check if SLA deadline is within 24 hours (imminent breach)
                is_imminent = False
                hours_remaining = None
                if sla_deadline:
                    try:
                        deadline_dt = datetime.fromisoformat(sla_deadline.replace('Z', '+00:00'))
                        hours_remaining = (deadline_dt - now).total_seconds() / 3600
                        if hours_remaining <= 24:
                            is_imminent = True
                            sla_breaches_imminent += 1
                            sla_exposure.append({
                                "tracking_number": r[0],
                                "service_level": r[3] or "STANDARD",
                                "hours_remaining": round(hours_remaining, 1),
                                "booking_charge": booking_charge,
                                "customer_id": r[6] or "Unknown",
                            })
                    except (ValueError, TypeError):
                        pass

                shipments.append({
                    "tracking_number": r[0],
                    "commodity": r[1] or "General",
                    "weight_kg": weight,
                    "service_level": r[3] or "STANDARD",
                    "booking_charge": booking_charge,  # Forwarder's revenue, NOT item value
                    "sla_deadline": sla_deadline,
                    "hours_remaining": hours_remaining,
                    "imminent_breach": is_imminent,
                })

            # Determine where operational entities came from (SIMULATION vs integration)
            operational_sources: List[str] = []
            try:
                sources_result = self.session.execute(
                    text(f"""
                        SELECT DISTINCT e.source_system
                        FROM edge e
                        JOIN node a ON e.dst = a.id
                        WHERE a.type = 'AIRPORT'
                          AND a.identifier = :icao
                          AND e.type IN ('FLIGHT_DEPARTS_FROM', 'FLIGHT_ARRIVES_AT')
                          AND {edge_visibility}
                    """),
                    {"icao": airport_icao, **visibility_params}
                )
                operational_sources = sorted({r[0] for r in sources_result if r[0]})
            except Exception:
                operational_sources = []

            if not flights and not shipments:
                return None

            # Sort SLA exposure by urgency
            sla_exposure.sort(key=lambda x: x.get("hours_remaining", 999))

            # ============================================================
            # ADDITIONAL CONTEXT GRAPH ASPECTS (beyond cascade)
            # ============================================================

            # 1. Get claims related to this airport (graph-derived assertions)
            claims_result = self.session.execute(
                text("""
                    SELECT c.id, c.text, c.status, c.confidence, c.ingested_at,
                           e.source_system, e.excerpt
                    FROM claim c
                    JOIN node n ON c.subject_node_id = n.id
                    LEFT JOIN claim_evidence ce ON ce.claim_id = c.id
                    LEFT JOIN evidence e ON ce.evidence_id = e.id
                    WHERE n.type = 'AIRPORT' AND n.identifier = :icao
                    ORDER BY c.ingested_at DESC
                    LIMIT 5
                """),
                {"icao": airport_icao}
            )
            claims = [
                {
                    "text": r[1],
                    "status": r[2],
                    "confidence": round(r[3] * 100) if r[3] else 0,
                    "source": r[5] or "DERIVED",
                    "excerpt": r[6][:100] if r[6] else None,
                }
                for r in claims_result
            ]

            # 2. Get edge type statistics (how we traversed the graph)
            # FIXED: Using canonical edge visibility predicate
            edge_stats_result = self.session.execute(
                text(f"""
                    SELECT e.type, COUNT(*) as cnt
                    FROM edge e
                    JOIN node dst ON e.dst = dst.id
                    WHERE dst.type = 'AIRPORT' AND dst.identifier = :icao
                      AND {edge_visibility}
                    GROUP BY e.type
                    ORDER BY cnt DESC
                """),
                {"icao": airport_icao, **visibility_params}
            )
            edge_types = {r[0]: r[1] for r in edge_stats_result}

            # 3. Get connected airports (network position - upstream/downstream)
            # FIXED: Using canonical node_version visibility
            connected_result = self.session.execute(
                text("""
                    SELECT DISTINCT
                        CASE
                            WHEN fv.attrs->>'origin' = :icao THEN fv.attrs->>'destination'
                            ELSE fv.attrs->>'origin'
                        END as connected_airport,
                        COUNT(*) as flight_count
                    FROM node f
                    JOIN node_version fv ON fv.node_id = f.id
                        AND fv.valid_from <= :at_event_time
                        AND (fv.valid_to IS NULL OR fv.valid_to > :at_event_time)
                    WHERE f.type = 'FLIGHT'
                      AND (fv.attrs->>'origin' = :icao OR fv.attrs->>'destination' = :icao)
                    GROUP BY connected_airport
                    ORDER BY flight_count DESC
                    LIMIT 10
                """),
                {"icao": airport_icao, **visibility_params}
            )
            connected_airports = [
                {"airport": r[0], "flights": r[1]}
                for r in connected_result if r[0] and r[0] != airport_icao
            ]

            # 4. Get evidence sources used (provenance chain)
            evidence_sources_result = self.session.execute(
                text("""
                    SELECT e.source_system, COUNT(*) as cnt, MAX(e.retrieved_at) as latest
                    FROM evidence e
                    JOIN claim_evidence ce ON ce.evidence_id = e.id
                    JOIN claim c ON ce.claim_id = c.id
                    JOIN node n ON c.subject_node_id = n.id
                    WHERE n.type = 'AIRPORT' AND n.identifier = :icao
                    GROUP BY e.source_system
                    ORDER BY latest DESC
                """),
                {"icao": airport_icao}
            )
            evidence_sources = [
                {
                    "source": r[0],
                    "evidence_count": r[1],
                    "latest": r[2].isoformat() if r[2] else None,
                }
                for r in evidence_sources_result
            ]

            # ============================================================
            # BI-TEMPORAL ASPECTS (what we knew when)
            # ============================================================

            # 5. Get bi-temporal edge timeline (event time vs ingest time)
            bitemporal_edges = self.session.execute(
                text(f"""
                    SELECT
                        e.type,
                        e.event_time_start,
                        e.event_time_end,
                        e.ingested_at,
                        e.valid_from,
                        e.valid_to,
                        e.status,
                        e.source_system
                    FROM edge e
                    JOIN node dst ON e.dst = dst.id
                    WHERE dst.type = 'AIRPORT' AND dst.identifier = :icao
                      AND (e.event_time_start IS NOT NULL OR e.valid_from IS NOT NULL)
                      AND e.source_system <> 'SIMULATION'
                      AND {edge_visibility}
                    ORDER BY e.ingested_at DESC
                    LIMIT 10
                """),
                {"icao": airport_icao, **visibility_params}
            )
            temporal_edges = [
                {
                    "edge_type": r[0],
                    "event_time_start": r[1].isoformat() if r[1] else None,
                    "event_time_end": r[2].isoformat() if r[2] else None,
                    "ingested_at": r[3].isoformat() if r[3] else None,
                    "valid_from": r[4].isoformat() if r[4] else None,
                    "valid_to": r[5].isoformat() if r[5] else None,
                    "status": r[6],
                    "source": r[7],
                }
                for r in bitemporal_edges
            ]

            # 6. Get claim supersession chain (audit trail)
            supersession_result = self.session.execute(
                text("""
                    SELECT
                        c.text,
                        c.status,
                        c.confidence,
                        c.ingested_at,
                        sc.text as supersedes_text,
                        sc.status as supersedes_status
                    FROM claim c
                    LEFT JOIN claim sc ON c.supersedes_claim_id = sc.id
                    WHERE c.supersedes_claim_id IS NOT NULL
                    ORDER BY c.ingested_at DESC
                    LIMIT 5
                """),
                {}
            )
            supersession_chain = [
                {
                    "current_claim": r[0],
                    "current_status": r[1],
                    "confidence": round(r[2] * 100) if r[2] else 0,
                    "ingested_at": r[3].isoformat() if r[3] else None,
                    "supersedes_claim": r[4],
                    "supersedes_status": r[5],
                }
                for r in supersession_result
            ]

            # 7. Get node version history (attribute changes over time)
            version_history_result = self.session.execute(
                text("""
                    SELECT
                        n.identifier,
                        nv.valid_from,
                        nv.valid_to,
                        nv.created_at,
                        nv.attrs->>'status' as status
                    FROM node n
                    JOIN node_version nv ON nv.node_id = n.id
                    WHERE n.type = 'AIRPORT' AND n.identifier = :icao
                    ORDER BY nv.created_at DESC
                    LIMIT 5
                """),
                {"icao": airport_icao}
            )
            version_history = [
                {
                    "identifier": r[0],
                    "valid_from": r[1].isoformat() if r[1] else None,
                    "valid_to": r[2].isoformat() if r[2] else "CURRENT",
                    "created_at": r[3].isoformat() if r[3] else None,
                    "status": r[4] or "ACTIVE",
                }
                for r in version_history_result
            ]

            # 8. Get contradictions (temporal conflicts in claims)
            # FIXED: Scope to THIS case via trace_event, not the entire DB
            contradictions_result = self.session.execute(
                text("""
                    SELECT
                        co.resolution_status,
                        co.detected_at,
                        ca.text as claim_a,
                        cb.text as claim_b
                    FROM contradiction co
                    JOIN claim ca ON co.claim_a = ca.id
                    JOIN claim cb ON co.claim_b = cb.id
                    JOIN trace_event t ON t.ref_id = co.id
                    WHERE t.case_id = :case_id AND t.ref_type = 'contradiction'
                    ORDER BY co.detected_at DESC
                    LIMIT 5
                """),
                {"case_id": self.case_id}
            )
            contradictions = [
                {
                    "status": r[0],
                    "detected_at": r[1].isoformat() if r[1] else None,
                    "claim_a": r[2][:80] if r[2] else None,
                    "claim_b": r[3][:80] if r[3] else None,
                }
                for r in contradictions_result
            ]

            return {
                "airport": airport_icao,

                # ============================================================
                # CONTEXT GRAPH STRUCTURE (not just cascade)
                # ============================================================
                "graph_traversal": {
                    "path": "AIRPORT <- FLIGHT <- SHIPMENT <- BOOKING -> CARRIER",
                    "nodes_visited": {
                        "airports": 1,
                        "flights": len(flights),
                        "shipments": len(shipments),
                        "bookings": bookings_count,
                        "carriers": len(carriers),
                    },
                    # Edge types used in traversal
                    "edge_types": edge_types,
                },

                # Network position (connected airports - upstream/downstream)
                "network_position": {
                    "connected_airports": connected_airports,
                    "is_hub": len(connected_airports) >= 5,  # Simple hub detection
                },

                # ============================================================
                # CLAIMS & EVIDENCE (provenance chain)
                # ============================================================
                "claims": claims,  # Graph-derived assertions about this airport
                "evidence_sources": evidence_sources,  # Where claims come from

                # ============================================================
                # BI-TEMPORAL ASPECTS (what we knew when)
                # ============================================================
                "bitemporal": {
                    "description": "Event time (when it happened) vs Ingest time (when we learned it)",
                    "temporal_edges": temporal_edges,  # Edges with explicit time windows
                    "supersession_chain": supersession_chain,  # Claim audit trail
                    "version_history": version_history,  # Node attribute changes
                    "contradictions": contradictions,  # Temporal conflicts
                },

                # ============================================================
                # CASCADE IMPACT (operational)
                # ============================================================
                "operational_data": {
                    "sources": operational_sources,
                    "is_simulated": "SIMULATION" in operational_sources,
                },
                "flights_affected": len(flights),
                "flights": flights[:5],
                "carriers": carriers,
                "shipments_affected": len(shipments),
                "shipments": shipments[:5],
                "sla_exposure": sla_exposure[:10],
                "sla_breaches_imminent": sla_breaches_imminent,

                # Financial summary
                "summary": {
                    "total_flights": len(flights),
                    "total_shipments": len(shipments),
                    "total_bookings": bookings_count,
                    "total_weight_kg": round(total_weight, 2),
                    "total_revenue_usd": round(total_revenue, 2),
                    "sla_breaches_imminent": sla_breaches_imminent,
                },
            }

        except Exception as e:
            # Don't fail the packet if cascade query fails
            return {"error": str(e)}

    def _build_metrics(
        self,
        evidence: List[EvidenceSummary],
        belief_state: Optional[BeliefState],
    ) -> PacketMetrics:
        """
        Build packet metrics including PDL.

        FIXED: PDL now uses the actual posture emission time from the action table,
        not the current time. This prevents PDL from growing as you wait to read the packet.
        """
        now = datetime.now(timezone.utc)

        # Find first signal time
        first_signal_at = now
        if evidence:
            first_signal_at = min(e.retrieved_at for e in evidence)

        # FIXED: Get actual posture emission time from SET_POSTURE action
        # (was incorrectly using 'now' which grew the longer you waited to read)
        posture_result = self.session.execute(
            text("""
                SELECT o.created_at, o.payload
                FROM action a
                JOIN outcome o ON a.id = o.action_id
                WHERE a.case_id = :case_id
                  AND a.type = 'SET_POSTURE'
                  AND a.state = 'COMPLETED'
                ORDER BY a.created_at DESC
                LIMIT 1
            """),
            {"case_id": self.case_id}
        )
        posture_row = posture_result.fetchone()

        if posture_row:
            # Use the actual outcome creation time as posture_emitted_at
            posture_emitted_at = posture_row[0]
            # Or try to get from payload.effective_at if available
            payload = posture_row[1] if isinstance(posture_row[1], dict) else {}
            if payload.get("effective_at"):
                try:
                    from datetime import datetime as dt
                    posture_emitted_at = dt.fromisoformat(payload["effective_at"].replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass
        else:
            # No posture set yet - use now (case is in progress)
            posture_emitted_at = now

        # Calculate PDL using actual emission time
        pdl_seconds = (posture_emitted_at - first_signal_at).total_seconds()

        # Get counts
        uncertainty_resolved = 0
        if belief_state:
            uncertainty_resolved = len([
                u for u in belief_state.uncertainties if u.resolved
            ])

        contradiction_count = 0
        if belief_state:
            contradiction_count = belief_state.contradiction_count

        # Get action count
        result = self.session.execute(
            text("SELECT COUNT(*) FROM action WHERE case_id = :id"),
            {"id": self.case_id}
        )
        action_count = result.scalar() or 0

        return PacketMetrics(
            first_signal_at=first_signal_at,
            posture_emitted_at=posture_emitted_at,
            pdl_seconds=pdl_seconds,
            evidence_count=len(evidence),
            uncertainty_resolved_count=uncertainty_resolved,
            contradiction_count=contradiction_count,
            action_count=action_count,
        )


def build_decision_packet(
    case_id: UUID,
    belief_state: Optional[BeliefState] = None,
    risk_assessment: Optional[Dict[str, Any]] = None,
    proposed_actions: Optional[List[Dict[str, Any]]] = None,
    session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Build decision packet for a case.

    Args:
        case_id: Case ID
        belief_state: Optional belief state
        risk_assessment: Optional risk assessment
        proposed_actions: Optional proposed actions
        session: Optional database session

    Returns:
        Decision packet as dict
    """
    if session is None:
        session = SessionLocal()
        owns_session = True
    else:
        owns_session = False

    try:
        builder = DecisionPacketBuilder(case_id, session)
        packet = builder.build(belief_state, risk_assessment)
        return packet.to_dict()

    finally:
        if owns_session:
            session.close()
