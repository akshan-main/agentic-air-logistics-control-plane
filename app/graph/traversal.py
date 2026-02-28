# app/graph/traversal.py
"""
Multi-hop graph traversal using recursive CTEs.

Uses CANONICAL visibility predicates from visibility.py.
Returns subgraph (nodes + edges), not flat rows.
"""

from datetime import datetime, timedelta
from typing import List, Optional, Set
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import SessionLocal
from .models import Node, Edge, GraphSubset
from .visibility import edge_visible_at, get_visibility_params


@dataclass
class TraversalResult:
    """Result of graph traversal."""
    start_nodes: List[UUID]
    edge_types: List[str]
    max_hops: int
    at_event_time: datetime
    at_ingest_time: datetime
    subgraph: GraphSubset
    traversal_depth: int = 0

    @property
    def node_ids_reached(self) -> Set[UUID]:
        """All node IDs reached by traversal."""
        ids = set()
        for edge in self.subgraph.edges:
            ids.add(edge.src)
            ids.add(edge.dst)
        return ids


def traverse(
    start_node_ids: List[UUID],
    edge_types: List[str],
    at_event_time: datetime,
    at_ingest_time: datetime,
    max_hops: int = 3,
    session: Optional[Session] = None,
) -> TraversalResult:
    """
    Traverse graph from start nodes following edge types.

    Uses recursive CTE with CANONICAL visibility predicate.

    Args:
        start_node_ids: Starting node IDs
        edge_types: Edge types to follow
        at_event_time: Event time point
        at_ingest_time: Ingest time point
        max_hops: Maximum traversal depth

    Returns:
        TraversalResult with subgraph
    """
    if session is None:
        session = SessionLocal()
        owns_session = True
    else:
        owns_session = False

    try:
        # Use canonical visibility predicate for both base case and recursive case
        base_visibility = edge_visible_at(at_event_time, at_ingest_time, table_alias="edge")
        recursive_visibility = edge_visible_at(at_event_time, at_ingest_time, table_alias="e")
        params = get_visibility_params(at_event_time, at_ingest_time)
        params["start_ids"] = start_node_ids
        params["edge_types"] = edge_types
        params["max_hops"] = max_hops

        # Recursive CTE for multi-hop traversal
        query = f"""
        WITH RECURSIVE reachable AS (
            -- Base case: edges from start nodes
            SELECT id, src, dst, type, attrs, status, supersedes_edge_id,
                   event_time_start, event_time_end, ingested_at,
                   valid_from, valid_to, source_system, confidence,
                   0 as depth
            FROM edge
            WHERE src = ANY(:start_ids)
              AND type = ANY(:edge_types)
              AND {base_visibility}

            UNION ALL

            -- Recursive case: edges from reached nodes
            SELECT e.id, e.src, e.dst, e.type, e.attrs, e.status, e.supersedes_edge_id,
                   e.event_time_start, e.event_time_end, e.ingested_at,
                   e.valid_from, e.valid_to, e.source_system, e.confidence,
                   r.depth + 1
            FROM edge e
            JOIN reachable r ON e.src = r.dst
            WHERE r.depth < :max_hops
              AND e.type = ANY(:edge_types)
              AND {recursive_visibility}
        )
        SELECT DISTINCT id, src, dst, type, attrs, status, supersedes_edge_id,
               event_time_start, event_time_end, ingested_at,
               valid_from, valid_to, source_system, confidence,
               depth
        FROM reachable
        ORDER BY depth, id
        """

        result = session.execute(text(query), params)

        edges = []
        max_depth = 0
        for row in result:
            edges.append(Edge(
                id=row[0], src=row[1], dst=row[2], type=row[3],
                attrs=row[4], status=row[5], supersedes_edge_id=row[6],
                event_time_start=row[7], event_time_end=row[8],
                ingested_at=row[9], valid_from=row[10], valid_to=row[11],
                source_system=row[12], confidence=row[13],
            ))
            max_depth = max(max_depth, row[14])

        # Collect unique node IDs
        node_ids = set(start_node_ids)
        for edge in edges:
            node_ids.add(edge.src)
            node_ids.add(edge.dst)

        # Fetch nodes
        nodes = []
        if node_ids:
            node_result = session.execute(
                text("""
                    SELECT id, type, identifier, created_at
                    FROM node WHERE id = ANY(:ids)
                """),
                {"ids": list(node_ids)}
            )
            for row in node_result:
                nodes.append(Node(
                    id=row[0], type=row[1], identifier=row[2], created_at=row[3]
                ))

        subgraph = GraphSubset(nodes=nodes, edges=edges)

        return TraversalResult(
            start_nodes=start_node_ids,
            edge_types=edge_types,
            max_hops=max_hops,
            at_event_time=at_event_time,
            at_ingest_time=at_ingest_time,
            subgraph=subgraph,
            traversal_depth=max_depth,
        )

    finally:
        if owns_session:
            session.close()


def get_subgraph(
    center_node_id: UUID,
    at_event_time: datetime,
    at_ingest_time: datetime,
    hops: int = 2,
    session: Optional[Session] = None,
) -> GraphSubset:
    """
    Get subgraph centered on a node.

    Traverses all edge types up to specified hops.

    Args:
        center_node_id: Center node ID
        at_event_time: Event time point
        at_ingest_time: Ingest time point
        hops: Number of hops from center

    Returns:
        GraphSubset containing reached nodes and edges
    """
    if session is None:
        session = SessionLocal()
        owns_session = True
    else:
        owns_session = False

    try:
        # Use canonical visibility predicate for both base case and recursive case
        base_visibility = edge_visible_at(at_event_time, at_ingest_time, table_alias="edge")
        recursive_visibility = edge_visible_at(at_event_time, at_ingest_time, table_alias="e")
        params = get_visibility_params(at_event_time, at_ingest_time)
        params["center_id"] = center_node_id
        params["max_hops"] = hops

        # Bidirectional traversal (both src and dst)
        query = f"""
        WITH RECURSIVE reachable AS (
            -- Base case: edges from/to center node
            SELECT id, src, dst, type, attrs, status, supersedes_edge_id,
                   event_time_start, event_time_end, ingested_at,
                   valid_from, valid_to, source_system, confidence,
                   0 as depth
            FROM edge
            WHERE (src = :center_id OR dst = :center_id)
              AND {base_visibility}

            UNION ALL

            -- Recursive case: edges from/to reached nodes
            SELECT e.id, e.src, e.dst, e.type, e.attrs, e.status, e.supersedes_edge_id,
                   e.event_time_start, e.event_time_end, e.ingested_at,
                   e.valid_from, e.valid_to, e.source_system, e.confidence,
                   r.depth + 1
            FROM edge e
            JOIN reachable r ON (e.src = r.dst OR e.dst = r.src)
            WHERE r.depth < :max_hops
              AND e.id != r.id
              AND {recursive_visibility}
        )
        SELECT DISTINCT id, src, dst, type, attrs, status, supersedes_edge_id,
               event_time_start, event_time_end, ingested_at,
               valid_from, valid_to, source_system, confidence
        FROM reachable
        """

        result = session.execute(text(query), params)

        edges = []
        node_ids = {center_node_id}

        for row in result:
            edges.append(Edge(
                id=row[0], src=row[1], dst=row[2], type=row[3],
                attrs=row[4], status=row[5], supersedes_edge_id=row[6],
                event_time_start=row[7], event_time_end=row[8],
                ingested_at=row[9], valid_from=row[10], valid_to=row[11],
                source_system=row[12], confidence=row[13],
            ))
            node_ids.add(row[1])  # src
            node_ids.add(row[2])  # dst

        # Fetch nodes
        nodes = []
        if node_ids:
            node_result = session.execute(
                text("""
                    SELECT id, type, identifier, created_at
                    FROM node WHERE id = ANY(:ids)
                """),
                {"ids": list(node_ids)}
            )
            for row in node_result:
                nodes.append(Node(
                    id=row[0], type=row[1], identifier=row[2], created_at=row[3]
                ))

        return GraphSubset(nodes=nodes, edges=edges)

    finally:
        if owns_session:
            session.close()


# ============================================================
# CASCADE ANALYSIS FUNCTIONS
# ============================================================
# These functions enable cascade analysis from airport disruption
# to affected operational entities (flights, shipments, bookings).


@dataclass
class CascadeResult:
    """
    Result of cascade analysis from airport disruption.

    Shows affected flights, shipments, and bookings
    along with forwarder exposure metrics (revenue at risk, not shipment value).
    """
    airport_icao: str
    affected_flights: List[dict]
    affected_shipments: List[dict]
    affected_bookings: List[dict]
    total_shipments: int
    total_bookings: int
    # Forwarder revenue metrics (what forwarder actually knows and cares about)
    total_revenue_at_risk: float  # Sum of total_charge_usd from bookings
    total_weight_kg: float
    sla_at_risk_count: int
    premium_sla_at_risk: int  # PREMIUM service level bookings at risk
    express_sla_at_risk: int  # EXPRESS service level bookings at risk


def cascade_from_airport(
    airport_icao: str,
    at_event_time: Optional[datetime] = None,
    at_ingest_time: Optional[datetime] = None,
    session: Optional[Session] = None,
) -> CascadeResult:
    """
    Find all operational entities affected by airport disruption.

    Cascade path:
    AIRPORT → FLIGHT (via FLIGHT_DEPARTS_FROM/ARRIVES_AT)
            → SHIPMENT (via SHIPMENT_ON_FLIGHT)
            → BOOKING (via BOOKING_FOR_SHIPMENT)

    This is the key function that makes the context graph useful
    for supply chain cascade analysis.

    Args:
        airport_icao: Airport ICAO code (e.g., "KJFK")
        at_event_time: Event time point (default: now)
        at_ingest_time: Ingest time point (default: now)

    Returns:
        CascadeResult with affected entities and exposure metrics
    """
    from datetime import timezone

    if at_event_time is None:
        at_event_time = datetime.now(timezone.utc)
    if at_ingest_time is None:
        at_ingest_time = datetime.now(timezone.utc)

    if session is None:
        session = SessionLocal()
        owns_session = True
    else:
        owns_session = False

    try:
        # Step 1: Find airport node
        airport_result = session.execute(
            text("""
                SELECT id FROM node
                WHERE type = 'AIRPORT' AND identifier = :icao
            """),
            {"icao": airport_icao.upper()}
        )
        airport_row = airport_result.fetchone()
        if not airport_row:
            return CascadeResult(
                airport_icao=airport_icao,
                affected_flights=[],
                affected_shipments=[],
                affected_bookings=[],
                total_shipments=0,
                total_bookings=0,
                total_revenue_at_risk=0.0,
                total_weight_kg=0.0,
                sla_at_risk_count=0,
                premium_sla_at_risk=0,
                express_sla_at_risk=0,
            )

        airport_id = airport_row[0]

        # Step 2: Find affected flights (departing from or arriving at this airport)
        flights_result = session.execute(
            text("""
                SELECT DISTINCT
                    f.id,
                    f.identifier,
                    fv.attrs
                FROM node f
                JOIN edge e ON f.id = e.src
                LEFT JOIN node_version fv ON f.id = fv.node_id AND fv.valid_to IS NULL
                WHERE f.type = 'FLIGHT'
                  AND e.dst = :airport_id
                  AND e.type IN ('FLIGHT_DEPARTS_FROM', 'FLIGHT_ARRIVES_AT')
            """),
            {"airport_id": airport_id}
        )

        affected_flights = []
        flight_ids = []
        for row in flights_result:
            flight_ids.append(row[0])
            attrs = row[2] or {}
            affected_flights.append({
                "id": str(row[0]),
                "identifier": row[1],
                "flight_number": attrs.get("flight_number", ""),
                "origin": attrs.get("origin", ""),
                "destination": attrs.get("destination", ""),
                "status": attrs.get("status", ""),
            })

        if not flight_ids:
            return CascadeResult(
                airport_icao=airport_icao,
                affected_flights=affected_flights,
                affected_shipments=[],
                affected_bookings=[],
                total_shipments=0,
                total_bookings=0,
                total_revenue_at_risk=0.0,
                total_weight_kg=0.0,
                sla_at_risk_count=0,
                premium_sla_at_risk=0,
                express_sla_at_risk=0,
            )

        # Step 3: Find affected shipments (on affected flights)
        shipments_result = session.execute(
            text("""
                SELECT DISTINCT
                    s.id,
                    s.identifier,
                    sv.attrs
                FROM node s
                JOIN edge e ON s.id = e.src
                LEFT JOIN node_version sv ON s.id = sv.node_id AND sv.valid_to IS NULL
                WHERE s.type = 'SHIPMENT'
                  AND e.dst = ANY(:flight_ids)
                  AND e.type = 'SHIPMENT_ON_FLIGHT'
            """),
            {"flight_ids": flight_ids}
        )

        affected_shipments = []
        shipment_ids = []
        total_weight = 0.0
        for row in shipments_result:
            shipment_ids.append(row[0])
            attrs = row[2] or {}
            weight = attrs.get("weight_kg", 0)
            total_weight += weight
            affected_shipments.append({
                "id": str(row[0]),
                "identifier": row[1],
                "tracking_number": attrs.get("tracking_number", ""),
                "commodity": attrs.get("commodity", ""),
                "weight_kg": weight,
                "service_level": attrs.get("service_level", ""),
                "status": attrs.get("status", ""),
            })

        # Step 4: Find affected bookings (for affected shipments)
        total_revenue = 0.0
        sla_at_risk = 0
        premium_at_risk = 0
        express_at_risk = 0
        affected_bookings = []

        # Build shipment -> service_level lookup
        shipment_service_levels = {
            s["id"]: s.get("service_level", "STANDARD")
            for s in affected_shipments
        }

        if shipment_ids:
            bookings_result = session.execute(
                text("""
                    SELECT DISTINCT
                        b.id,
                        b.identifier,
                        bv.attrs,
                        e.dst as shipment_node_id
                    FROM node b
                    JOIN edge e ON b.id = e.src
                    LEFT JOIN node_version bv ON b.id = bv.node_id AND bv.valid_to IS NULL
                    WHERE b.type = 'BOOKING'
                      AND e.dst = ANY(:shipment_ids)
                      AND e.type = 'BOOKING_FOR_SHIPMENT'
                """),
                {"shipment_ids": shipment_ids}
            )

            for row in bookings_result:
                attrs = row[2] or {}
                # Use total_charge_usd (forwarder revenue) instead of fake value_usd
                charge = attrs.get("total_charge_usd", 0)
                total_revenue += charge

                # Check SLA deadline and service level
                sla_deadline_str = attrs.get("sla_deadline")

                if sla_deadline_str:
                    try:
                        sla_deadline = datetime.fromisoformat(sla_deadline_str.replace("Z", "+00:00"))
                        if sla_deadline < at_event_time + timedelta(hours=48):
                            sla_at_risk += 1
                            # Track by service level
                            service_level = shipment_service_levels.get(str(row[3]), "STANDARD")
                            if service_level == "PREMIUM":
                                premium_at_risk += 1
                            elif service_level == "EXPRESS":
                                express_at_risk += 1
                    except (ValueError, TypeError):
                        pass

                affected_bookings.append({
                    "id": str(row[0]),
                    "identifier": row[1],
                    "booking_reference": attrs.get("booking_reference", ""),
                    "total_charge_usd": charge,
                    "rate_per_kg": attrs.get("rate_per_kg", 0),
                    "sla_deadline": sla_deadline_str,
                    "rate_type": attrs.get("rate_type", ""),
                })

        return CascadeResult(
            airport_icao=airport_icao,
            affected_flights=affected_flights,
            affected_shipments=affected_shipments,
            affected_bookings=affected_bookings,
            total_shipments=len(affected_shipments),
            total_bookings=len(affected_bookings),
            total_revenue_at_risk=total_revenue,
            total_weight_kg=total_weight,
            sla_at_risk_count=sla_at_risk,
            premium_sla_at_risk=premium_at_risk,
            express_sla_at_risk=express_at_risk,
        )

    finally:
        if owns_session:
            session.close()


def get_shipments_with_booking_evidence(
    airport_icao: str,
    session: Optional[Session] = None,
) -> List[dict]:
    """
    Get shipments that have booking evidence (required for shipment-level actions).

    Per plan.md: "Shipment-level actions require booking evidence.
    Without it, system stays at posture level."

    Args:
        airport_icao: Airport ICAO code

    Returns:
        List of shipments with their booking evidence
    """
    if session is None:
        session = SessionLocal()
        owns_session = True
    else:
        owns_session = False

    try:
        result = session.execute(
            text("""
                SELECT
                    s.id as shipment_id,
                    s.identifier as shipment_identifier,
                    sv.attrs as shipment_attrs,
                    b.id as booking_id,
                    b.identifier as booking_identifier,
                    bv.attrs as booking_attrs
                FROM node s
                JOIN node_version sv ON s.id = sv.node_id AND sv.valid_to IS NULL
                JOIN edge se ON s.id = se.src AND se.type = 'SHIPMENT_ORIGIN'
                JOIN node a ON se.dst = a.id AND a.type = 'AIRPORT' AND a.identifier = :icao
                JOIN edge be ON be.dst = s.id AND be.type = 'BOOKING_FOR_SHIPMENT'
                JOIN node b ON be.src = b.id AND b.type = 'BOOKING'
                JOIN node_version bv ON b.id = bv.node_id AND bv.valid_to IS NULL
                WHERE s.type = 'SHIPMENT'
            """),
            {"icao": airport_icao.upper()}
        )

        shipments = []
        for row in result:
            shipment_attrs = row[2] or {}
            booking_attrs = row[5] or {}
            shipments.append({
                "shipment_id": str(row[0]),
                "shipment_identifier": row[1],
                "tracking_number": shipment_attrs.get("tracking_number"),
                "commodity": shipment_attrs.get("commodity"),
                "service_level": shipment_attrs.get("service_level"),
                "has_booking_evidence": True,
                "booking_id": str(row[3]),
                "booking_reference": booking_attrs.get("booking_reference"),
                "booking_value_usd": booking_attrs.get("total_charge_usd", 0),
            })

        return shipments

    finally:
        if owns_session:
            session.close()
