# app/api/routes_graph.py
"""
Graph API routes.

Endpoints for querying the context graph.
"""

from typing import Dict, Any, Optional, List
from uuid import UUID
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter(prefix="/graph", tags=["graph"])


class GraphQueryRequest(BaseModel):
    """Request for graph query."""
    start_node_ids: List[str]
    edge_types: Optional[List[str]] = None
    max_hops: int = 3
    at_event_time: Optional[datetime] = None
    at_ingest_time: Optional[datetime] = None


class SubgraphResponse(BaseModel):
    """Subgraph response."""
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]
    node_count: int
    edge_count: int


class SearchRequest(BaseModel):
    """Hybrid search request."""
    query: str
    case_context: Optional[Dict[str, Any]] = None
    limit: int = 20


class SearchResponse(BaseModel):
    """Hybrid search response."""
    results: List[Dict[str, Any]]
    weights_used: Dict[str, float]


@router.get("/case/{case_id}", response_model=SubgraphResponse)
async def get_case_subgraph(
    case_id: str,
    max_hops: int = Query(default=3, le=5),
    include_evidence: bool = True,
) -> SubgraphResponse:
    """
    Get the decision subgraph for a case.

    Returns all nodes and edges relevant to a case's decision.
    Requires database connection.

    Args:
        case_id: Case ID
        max_hops: Maximum traversal depth
        include_evidence: Include evidence nodes

    Returns:
        Subgraph with nodes and edges
    """
    from sqlalchemy import text
    from ..db.engine import SessionLocal
    from datetime import timezone

    try:
        with SessionLocal() as session:
            # Get all nodes and edges connected to this case via trace_event
            # First, get node IDs referenced in trace events
            node_ids_result = session.execute(
                text("""
                    SELECT DISTINCT ref_id FROM trace_event
                    WHERE case_id = :case_id AND ref_type = 'node'
                """),
                {"case_id": case_id}
            )
            node_ids = [str(row[0]) for row in node_ids_result if row[0]]

            # Get edge IDs referenced in trace events
            edge_ids_result = session.execute(
                text("""
                    SELECT DISTINCT ref_id FROM trace_event
                    WHERE case_id = :case_id AND ref_type = 'edge'
                """),
                {"case_id": case_id}
            )
            edge_ids = [str(row[0]) for row in edge_ids_result if row[0]]

            # Fetch actual edges
            edges = []
            if edge_ids:
                edges_result = session.execute(
                    text("""
                        SELECT id, src, dst, type, attrs, status, confidence,
                               event_time_start, event_time_end, ingested_at, source_system
                        FROM edge
                        WHERE id::text = ANY(:ids)
                    """),
                    {"ids": edge_ids}
                )
                for row in edges_result:
                    edges.append({
                        "id": str(row[0]),
                        "src": str(row[1]),
                        "dst": str(row[2]),
                        "type": row[3],
                        "attrs": row[4] or {},
                        "status": row[5],
                        "confidence": row[6],
                        "event_time_start": row[7].isoformat() if row[7] else None,
                        "event_time_end": row[8].isoformat() if row[8] else None,
                        "ingested_at": row[9].isoformat() if row[9] else None,
                        "source_system": row[10],
                    })

            # Fetch nodes referenced by edges (and any explicit node trace events).
            # Edge trace events are sufficient to reconstruct the subgraph; relying on
            # ref_type='node' alone yields empty nodes for most cases.
            combined_node_ids = set(node_ids)
            for e in edges:
                if e.get("src"):
                    combined_node_ids.add(e["src"])
                if e.get("dst"):
                    combined_node_ids.add(e["dst"])

            nodes = []
            if combined_node_ids:
                nodes_result = session.execute(
                    text("""
                        SELECT n.id, n.type, n.identifier, n.created_at,
                               nv.attrs
                        FROM node n
                        LEFT JOIN node_version nv ON n.id = nv.node_id AND nv.valid_to IS NULL
                        WHERE n.id::text = ANY(:ids)
                    """),
                    {"ids": list(combined_node_ids)}
                )
                for row in nodes_result:
                    nodes.append({
                        "id": str(row[0]),
                        "type": row[1],
                        "identifier": row[2],
                        "created_at": row[3].isoformat() if row[3] else None,
                        "attrs": row[4] or {},
                    })

            # If include_evidence, also get evidence nodes
            if include_evidence:
                evidence_result = session.execute(
                    text("""
                        SELECT e.id, e.source_system, e.retrieved_at, e.excerpt
                        FROM evidence e
                        JOIN trace_event t ON t.ref_id::text = e.id::text
                        WHERE t.case_id = :case_id AND t.ref_type = 'evidence'
                    """),
                    {"case_id": case_id}
                )
                for row in evidence_result:
                    nodes.append({
                        "id": str(row[0]),
                        "type": "EVIDENCE",
                        "identifier": row[1],
                        "created_at": row[2].isoformat() if row[2] else None,
                        "attrs": {"excerpt": row[3][:200] if row[3] else None},
                    })

            return SubgraphResponse(
                nodes=nodes,
                edges=edges,
                node_count=len(nodes),
                edge_count=len(edges),
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post("/traverse", response_model=SubgraphResponse)
async def traverse_graph(
    request: GraphQueryRequest,
) -> SubgraphResponse:
    """
    Traverse the graph from specified starting nodes.
    Requires database connection.

    Args:
        request: Traversal request

    Returns:
        Traversed subgraph
    """
    from ..graph.traversal import traverse
    from uuid import UUID as UUIDType
    from datetime import timezone

    # Parse UUIDs
    try:
        start_ids = [UUIDType(sid) for sid in request.start_node_ids]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid UUID in start_node_ids: {e}")

    # Default to all edge types if not specified
    edge_types = request.edge_types or [
        "AIRPORT_HAS_FAA_DISRUPTION",
        "AIRPORT_WEATHER_RISK",
        "AIRPORT_HAS_NWS_ALERT",
        "AIRPORT_MOVEMENT_COLLAPSE",
        "FLIGHT_DEPARTS_FROM",
        "FLIGHT_ARRIVES_AT",
        "SHIPMENT_ON_FLIGHT",
        "BOOKING_FOR_SHIPMENT",
    ]

    # Default times to now
    at_event_time = request.at_event_time or datetime.now(timezone.utc)
    at_ingest_time = request.at_ingest_time or datetime.now(timezone.utc)

    try:
        result = traverse(
            start_node_ids=start_ids,
            edge_types=edge_types,
            at_event_time=at_event_time,
            at_ingest_time=at_ingest_time,
            max_hops=request.max_hops,
        )

        # Convert to response format
        nodes = [
            {
                "id": str(n.id),
                "type": n.type,
                "identifier": n.identifier,
                "created_at": n.created_at.isoformat() if n.created_at else None,
            }
            for n in result.subgraph.nodes
        ]

        edges = [
            {
                "id": str(e.id),
                "src": str(e.src),
                "dst": str(e.dst),
                "type": e.type,
                "attrs": e.attrs or {},
                "status": e.status,
                "confidence": e.confidence,
                "event_time_start": e.event_time_start.isoformat() if e.event_time_start else None,
                "event_time_end": e.event_time_end.isoformat() if e.event_time_end else None,
                "ingested_at": e.ingested_at.isoformat() if e.ingested_at else None,
                "source_system": e.source_system,
            }
            for e in result.subgraph.edges
        ]

        return SubgraphResponse(
            nodes=nodes,
            edges=edges,
            node_count=len(nodes),
            edge_count=len(edges),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Traversal error: {str(e)}")


@router.post("/search", response_model=SearchResponse)
async def hybrid_search_endpoint(
    request: SearchRequest,
) -> SearchResponse:
    """
    Hybrid search over the graph.

    Combines semantic, keyword, and graph-based retrieval.
    Uses deterministic ranking: 0.5*semantic + 0.3*keyword + 0.2*graph

    Args:
        request: Search request

    Returns:
        Search results with scores
    """
    from ..graph.retrieval import hybrid_search, WEIGHTS

    try:
        results = hybrid_search(
            query_text=request.query,
            case_context=request.case_context,
            limit=request.limit,
        )

        return SearchResponse(
            results=[
                {
                    "case_id": str(r.case_id),
                    "text": r.text,
                    "semantic_score": r.semantic_score,
                    "keyword_score": r.keyword_score,
                    "graph_score": r.graph_score,
                    "final_score": r.final_score,
                }
                for r in results
            ],
            weights_used=WEIGHTS,
        )
    except Exception as e:
        # Don't silently swallow errors - expose them for debugging
        # This helps identify DB connection issues, missing tables, etc.
        raise HTTPException(
            status_code=500,
            detail=f"Hybrid search failed: {str(e)}. Check database connection and schema."
        )


@router.get("/node/{node_id}")
async def get_node(
    node_id: str,
    include_versions: bool = False,
) -> Dict[str, Any]:
    """
    Get a node by ID.
    Requires database connection.

    Args:
        node_id: Node ID
        include_versions: Include version history

    Returns:
        Node data
    """
    raise HTTPException(
        status_code=503,
        detail="Database connection required. Configure DATABASE_URL in .env"
    )


@router.get("/edges")
async def get_edges(
    src: Optional[str] = None,
    dst: Optional[str] = None,
    edge_type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
) -> Dict[str, Any]:
    """
    Query edges with filters.
    Requires database connection.

    Args:
        src: Source node ID filter
        dst: Destination node ID filter
        edge_type: Edge type filter
        status: Status filter (DRAFT, FACT, RETRACTED)
        limit: Maximum results

    Returns:
        Matching edges
    """
    return {"edges": [], "count": 0, "note": "Database connection required"}


# ============================================================
# CASCADE ANALYSIS ENDPOINTS
# ============================================================
# These endpoints analyze the impact of airport disruptions
# on operational entities (flights, shipments, bookings).


@router.get("/cascade/{airport_icao}")
async def cascade_from_airport(
    airport_icao: str,
) -> Dict[str, Any]:
    """
    Find all operational entities affected by airport disruption.

    Cascade path:
    AIRPORT → FLIGHT → SHIPMENT → BOOKING

    This is the key endpoint for supply chain impact analysis.
    Requires operational data to be seeded via /simulation/seed/airport/{icao}

    Args:
        airport_icao: Airport ICAO code (e.g., KJFK)

    Returns:
        Affected flights, shipments, bookings with exposure metrics
    """
    from ..graph.traversal import cascade_from_airport as do_cascade

    try:
        result = do_cascade(airport_icao.upper())
        return {
            "airport": result.airport_icao,
            "cascade_summary": {
                "affected_flights": len(result.affected_flights),
                "affected_shipments": result.total_shipments,
                "affected_bookings": result.total_bookings,
                "total_revenue_at_risk_usd": result.total_revenue_at_risk,
                "total_weight_kg": result.total_weight_kg,
                "sla_at_risk_count": result.sla_at_risk_count,
                "premium_sla_at_risk": result.premium_sla_at_risk,
                "express_sla_at_risk": result.express_sla_at_risk,
            },
            "affected_flights": result.affected_flights,
            "affected_shipments": result.affected_shipments,
            "affected_bookings": result.affected_bookings,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shipments-with-booking/{airport_icao}")
async def get_shipments_with_booking(
    airport_icao: str,
) -> Dict[str, Any]:
    """
    Get shipments at an airport that have booking evidence.

    Per plan.md: "Shipment-level actions require booking evidence.
    Without it, system stays at posture level."

    Shipments with booking evidence can receive actions like:
    - HOLD_CARGO
    - RELEASE_CARGO
    - SWITCH_GATEWAY
    - REBOOK_FLIGHT

    Args:
        airport_icao: Airport ICAO code

    Returns:
        Shipments with their booking evidence
    """
    from ..graph.traversal import get_shipments_with_booking_evidence

    try:
        shipments = get_shipments_with_booking_evidence(airport_icao.upper())
        return {
            "airport": airport_icao.upper(),
            "shipments_with_booking_evidence": shipments,
            "count": len(shipments),
            "can_receive_shipment_actions": len(shipments) > 0,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# BI-TEMPORAL QUERY ENDPOINTS
# ============================================================
# These endpoints expose point-in-time queries for debugging
# and auditing: "What did we believe at time T?"


class BiTemporalQueryRequest(BaseModel):
    """Request for bi-temporal query."""
    case_id: str
    at_event_time: Optional[datetime] = None  # What was happening at this time?
    at_ingest_time: Optional[datetime] = None  # What did we know at this time?


@router.post("/bitemporal/beliefs")
async def query_beliefs_at_time(
    request: BiTemporalQueryRequest,
) -> Dict[str, Any]:
    """
    Query what the system believed at a specific point in time.

    This is the key bi-temporal debugging endpoint:
    - at_event_time: What was happening in the real world?
    - at_ingest_time: What had the system ingested by then?

    Example: "What did we know about JFK at 3pm, based on data we had by 2pm?"
      at_event_time=15:00, at_ingest_time=14:00

    Args:
        request: Bi-temporal query parameters

    Returns:
        State of beliefs at the specified time
    """
    from sqlalchemy import text
    from ..db.engine import SessionLocal
    from ..graph.visibility import edge_visible_at, claim_visible_at, get_visibility_params

    at_event = request.at_event_time or datetime.now(timezone.utc)
    at_ingest = request.at_ingest_time or datetime.now(timezone.utc)

    try:
        with SessionLocal() as session:
            # Use CANONICAL visibility predicate from visibility.py
            edge_visibility = edge_visible_at(at_event, at_ingest)
            params = get_visibility_params(at_event, at_ingest)

            # Get edges visible at that time
            edges_result = session.execute(
                text(f"""
                    SELECT id, src, dst, type, attrs, status, confidence,
                           event_time_start, event_time_end, ingested_at
                    FROM edge
                    WHERE {edge_visibility}
                    ORDER BY ingested_at DESC
                    LIMIT 100
                """),
                params
            )

            edges = [
                {
                    "id": str(row[0]),
                    "src": str(row[1]),
                    "dst": str(row[2]),
                    "type": row[3],
                    "attrs": row[4],
                    "status": row[5],
                    "confidence": row[6],
                    "event_time_start": row[7].isoformat() if row[7] else None,
                    "event_time_end": row[8].isoformat() if row[8] else None,
                    "ingested_at": row[9].isoformat() if row[9] else None,
                }
                for row in edges_result
            ]

            # Get claims visible at that time using CANONICAL visibility predicate
            claim_visibility = claim_visible_at(at_event, at_ingest)
            claims_result = session.execute(
                text(f"""
                    SELECT id, text, confidence, status, ingested_at
                    FROM claim
                    WHERE {claim_visibility}
                    ORDER BY ingested_at DESC
                    LIMIT 100
                """),
                params
            )

            claims = [
                {
                    "id": str(row[0]),
                    "text": row[1],
                    "confidence": row[2],
                    "status": row[3],
                    "ingested_at": row[4].isoformat() if row[4] else None,
                }
                for row in claims_result
            ]

            # Get evidence visible at that time
            evidence_result = session.execute(
                text("""
                    SELECT id, source_system, retrieved_at, excerpt
                    FROM evidence
                    WHERE retrieved_at <= :at_ingest_time
                    ORDER BY retrieved_at DESC
                    LIMIT 100
                """),
                {"at_ingest_time": at_ingest}
            )

            evidence = [
                {
                    "id": str(row[0]),
                    "source_system": row[1],
                    "retrieved_at": row[2].isoformat() if row[2] else None,
                    "excerpt_preview": row[3][:200] if row[3] else None,
                }
                for row in evidence_result
            ]

            return {
                "query": {
                    "case_id": request.case_id,
                    "at_event_time": at_event.isoformat(),
                    "at_ingest_time": at_ingest.isoformat(),
                },
                "beliefs": {
                    "edges_visible": edges,
                    "claims_visible": claims,
                    "evidence_visible": evidence,
                    "edge_count": len(edges),
                    "claim_count": len(claims),
                    "evidence_count": len(evidence),
                },
                "explanation": f"Showing state of graph as of event_time={at_event.isoformat()} "
                              f"with data ingested by {at_ingest.isoformat()}",
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/bitemporal/case/{case_id}/replay")
async def replay_case_decisions(
    case_id: str,
) -> Dict[str, Any]:
    """
    Replay a case's decision timeline.

    Shows how the system's understanding evolved as data was ingested.
    Useful for auditing: "Why did we make this decision at time T?"

    Args:
        case_id: Case ID to replay

    Returns:
        Timeline of state transitions with timestamps
    """
    from sqlalchemy import text
    from ..db.engine import SessionLocal

    try:
        with SessionLocal() as session:
            # Get trace events in order
            result = session.execute(
                text("""
                    SELECT seq, event_type, ref_type, meta, created_at
                    FROM trace_event
                    WHERE case_id = :case_id
                    ORDER BY seq
                """),
                {"case_id": case_id}
            )

            timeline = []
            for row in result:
                event = {
                    "seq": row[0],
                    "event_type": row[1],
                    "ref_type": row[2],
                    "timestamp": row[4].isoformat() if row[4] else None,
                }

                # Extract key info from meta
                meta = row[3] if isinstance(row[3], dict) else {}
                if meta.get("state"):
                    event["state"] = meta["state"]
                if meta.get("description"):
                    event["description"] = meta["description"]
                if meta.get("evidence_count") is not None:
                    event["evidence_count"] = meta["evidence_count"]
                if meta.get("risk_level"):
                    event["risk_level"] = meta["risk_level"]
                if meta.get("recommended_posture"):
                    event["posture"] = meta["recommended_posture"]
                if meta.get("verdict"):
                    event["verdict"] = meta["verdict"]

                timeline.append(event)

            # Get evidence ingestion timestamps
            evidence_result = session.execute(
                text("""
                    SELECT e.source_system, e.retrieved_at
                    FROM evidence e
                    JOIN trace_event t ON t.ref_id::text = e.id::text
                    WHERE t.case_id = :case_id AND t.ref_type = 'evidence'
                    ORDER BY e.retrieved_at
                """),
                {"case_id": case_id}
            )

            evidence_timeline = [
                {
                    "source": row[0],
                    "ingested_at": row[1].isoformat() if row[1] else None,
                }
                for row in evidence_result
            ]

            return {
                "case_id": case_id,
                "decision_timeline": timeline,
                "evidence_ingestion_timeline": evidence_timeline,
                "total_events": len(timeline),
                "total_evidence": len(evidence_timeline),
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
