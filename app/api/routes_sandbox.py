# app/api/routes_sandbox.py
"""
Sandbox API - Full system exploration with REAL data.

Fetches real data from all 5 APIs, runs the complete pipeline,
and returns everything: evidence, claims, graph, cascade, posture, actions.
"""

import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..ingestion.registry import get_registry, AirportIngestionResult
from ..graph.traversal import cascade_from_airport, get_shipments_with_booking_evidence
from ..db.engine import SessionLocal

router = APIRouter(prefix="/sandbox", tags=["sandbox"])


class SandboxRequest(BaseModel):
    """Request for sandbox run."""
    airport: str  # ICAO code
    include_opensky: bool = True
    # Optional custom overrides (applied on top of real data)
    custom_faa_status: Optional[Dict[str, Any]] = None
    custom_metar: Optional[Dict[str, Any]] = None
    custom_alerts: Optional[List[Dict[str, Any]]] = None


def _serialize_ingestion_data(data: Any) -> Any:
    """Serialize ingestion data to JSON-safe format."""
    if data is None:
        return {"status": "normal_operations", "message": "No disruptions reported"}
    if isinstance(data, list):
        return [
            item.__dict__ if hasattr(item, '__dict__') else item
            for item in data
        ]
    if hasattr(data, '__dict__'):
        return data.__dict__
    return data


def _derive_claims(ingestion: AirportIngestionResult) -> List[Dict[str, Any]]:
    """Derive claims from ingestion data (what the system believes)."""
    claims = []

    # FAA claims
    if ingestion.faa_status and ingestion.faa_status.success:
        faa_data = ingestion.faa_status.data
        if faa_data is None:
            claims.append({
                "id": str(uuid4()),
                "text": f"{ingestion.icao} has no FAA disruptions (normal operations)",
                "status": "FACT",
                "confidence": 0.95,
                "source": "FAA_NAS",
                "evidence_bound": True,
            })
        else:
            faa_dict = faa_data.__dict__ if hasattr(faa_data, '__dict__') else faa_data
            if faa_dict.get("delay") or faa_dict.get("closure"):
                delay_type = faa_dict.get("delay_type", "DELAY")
                reason = faa_dict.get("reason", "Unknown")
                claims.append({
                    "id": str(uuid4()),
                    "text": f"{ingestion.icao} has {delay_type}: {reason}",
                    "status": "FACT",
                    "confidence": 0.95,
                    "source": "FAA_NAS",
                    "evidence_bound": True,
                })

    # METAR claims
    if ingestion.metar and ingestion.metar.success and ingestion.metar.data:
        metar = ingestion.metar.data
        metar_dict = metar.__dict__ if hasattr(metar, '__dict__') else metar

        # Flight category claim
        category = metar_dict.get("flight_category", "VFR")
        claims.append({
            "id": str(uuid4()),
            "text": f"{ingestion.icao} current conditions: {category}",
            "status": "FACT",
            "confidence": 0.90,
            "source": "METAR",
            "evidence_bound": True,
        })

        # Visibility claim
        vis = metar_dict.get("visibility_miles")
        if vis is not None and vis < 3:
            claims.append({
                "id": str(uuid4()),
                "text": f"{ingestion.icao} has low visibility ({vis} miles)",
                "status": "FACT",
                "confidence": 0.90,
                "source": "METAR",
                "evidence_bound": True,
            })

        # Wind claim
        wind = metar_dict.get("wind_speed_kts", 0)
        gust = metar_dict.get("wind_gust_kts")
        if wind >= 25 or (gust and gust >= 35):
            claims.append({
                "id": str(uuid4()),
                "text": f"{ingestion.icao} has strong winds ({wind}kt, gusts {gust}kt)",
                "status": "FACT",
                "confidence": 0.90,
                "source": "METAR",
                "evidence_bound": True,
            })

    # NWS Alert claims
    if ingestion.nws_alerts and ingestion.nws_alerts.success:
        alerts = ingestion.nws_alerts.data or []
        if len(alerts) == 0:
            claims.append({
                "id": str(uuid4()),
                "text": f"{ingestion.icao} has no active NWS weather alerts",
                "status": "FACT",
                "confidence": 0.85,
                "source": "NWS_ALERTS",
                "evidence_bound": True,
            })
        else:
            for alert in alerts:
                alert_dict = alert.__dict__ if hasattr(alert, '__dict__') else alert
                event = alert_dict.get("event", "Weather Alert")
                severity = alert_dict.get("severity", "Unknown")
                claims.append({
                    "id": str(uuid4()),
                    "text": f"{ingestion.icao} has {event} ({severity} severity)",
                    "status": "FACT",
                    "confidence": 0.85,
                    "source": "NWS_ALERTS",
                    "evidence_bound": True,
                })

    # OpenSky claims
    if ingestion.opensky:
        if ingestion.opensky.success and ingestion.opensky.data:
            opensky = ingestion.opensky.data
            opensky_dict = opensky.__dict__ if hasattr(opensky, '__dict__') else opensky
            aircraft_count = opensky_dict.get("aircraft_count", 0)
            claims.append({
                "id": str(uuid4()),
                "text": f"{ingestion.icao} has {aircraft_count} aircraft in vicinity",
                "status": "FACT",
                "confidence": 0.70,
                "source": "OPENSKY",
                "evidence_bound": True,
            })
        else:
            claims.append({
                "id": str(uuid4()),
                "text": f"{ingestion.icao} OpenSky data unavailable (degraded)",
                "status": "HYPOTHESIS",
                "confidence": 0.50,
                "source": "OPENSKY",
                "evidence_bound": False,
            })

    return claims


def _detect_contradictions(claims: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Detect contradictions between claims."""
    contradictions = []

    # Look for contradictions (simplified)
    faa_normal = any("no FAA disruptions" in c["text"] for c in claims)
    has_low_vis = any("low visibility" in c["text"] for c in claims)
    has_strong_wind = any("strong winds" in c["text"] for c in claims)
    has_ifr = any("IFR" in c["text"] or "LIFR" in c["text"] for c in claims)

    if faa_normal and (has_low_vis or has_ifr):
        contradictions.append({
            "id": str(uuid4()),
            "type": "FAA_WEATHER_MISMATCH",
            "description": "FAA reports normal but weather shows IFR/low visibility conditions",
            "claim_a": next(c for c in claims if "no FAA disruptions" in c["text"]),
            "claim_b": next((c for c in claims if "low visibility" in c["text"] or "IFR" in c["text"] or "LIFR" in c["text"]), None),
            "resolution_status": "OPEN",
        })

    return contradictions


def _assess_risk(claims: List[Dict[str, Any]], contradictions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Assess risk based on claims and contradictions."""
    # Count risk factors
    risk_factors = []

    for claim in claims:
        text = claim["text"].lower()
        if "ground_stop" in text or "closure" in text:
            risk_factors.append({"factor": "FAA_GROUND_STOP", "weight": 1.0})
        elif "ground_delay" in text:
            risk_factors.append({"factor": "FAA_GROUND_DELAY", "weight": 0.7})
        elif "lifr" in text:
            risk_factors.append({"factor": "LIFR_CONDITIONS", "weight": 0.8})
        elif "ifr" in text:
            risk_factors.append({"factor": "IFR_CONDITIONS", "weight": 0.5})
        elif "low visibility" in text:
            risk_factors.append({"factor": "LOW_VISIBILITY", "weight": 0.6})
        elif "strong winds" in text:
            risk_factors.append({"factor": "STRONG_WINDS", "weight": 0.5})
        elif "severe" in text or "extreme" in text:
            risk_factors.append({"factor": "SEVERE_WEATHER_ALERT", "weight": 0.8})
        elif "warning" in text:
            risk_factors.append({"factor": "WEATHER_WARNING", "weight": 0.6})

    # Add contradiction risk
    if contradictions:
        risk_factors.append({"factor": "UNRESOLVED_CONTRADICTIONS", "weight": 0.3})

    # Calculate risk level
    total_weight = sum(f["weight"] for f in risk_factors)

    if total_weight >= 1.5:
        risk_level = "CRITICAL"
        recommended_posture = "ESCALATE"
    elif total_weight >= 1.0:
        risk_level = "HIGH"
        recommended_posture = "HOLD"
    elif total_weight >= 0.5:
        risk_level = "MEDIUM"
        recommended_posture = "RESTRICT"
    else:
        risk_level = "LOW"
        recommended_posture = "ACCEPT"

    # Calculate confidence
    fact_claims = [c for c in claims if c["status"] == "FACT"]
    avg_confidence = sum(c["confidence"] for c in fact_claims) / len(fact_claims) if fact_claims else 0.5

    return {
        "risk_level": risk_level,
        "recommended_posture": recommended_posture,
        "risk_factors": risk_factors,
        "total_risk_weight": round(total_weight, 2),
        "overall_confidence": round(avg_confidence, 2),
        "evidence_count": len(fact_claims),
        "contradiction_count": len(contradictions),
    }


def _propose_actions(
    risk_assessment: Dict[str, Any],
    airport: str,
    has_booking_evidence: bool,
) -> List[Dict[str, Any]]:
    """Propose actions based on risk assessment."""
    actions = []

    posture = risk_assessment["recommended_posture"]
    risk_level = risk_assessment["risk_level"]

    # Always propose SET_POSTURE
    actions.append({
        "type": "SET_POSTURE",
        "args": {
            "airport": airport,
            "posture": posture,
            "reason": f"Risk level {risk_level} based on {risk_assessment['evidence_count']} evidence sources",
        },
        "risk_level": risk_level,
        "requires_approval": risk_level in ("HIGH", "CRITICAL"),
    })

    # PUBLISH_GATEWAY_ADVISORY for RESTRICT or higher
    if posture in ("RESTRICT", "HOLD", "ESCALATE"):
        actions.append({
            "type": "PUBLISH_GATEWAY_ADVISORY",
            "args": {
                "airport": airport,
                "posture": posture,
                "advisory_text": f"Gateway advisory: {posture} posture for {airport}",
            },
            "risk_level": "LOW",
            "requires_approval": False,
        })

    # ESCALATE_OPS for critical
    if posture == "ESCALATE":
        actions.append({
            "type": "ESCALATE_OPS",
            "args": {
                "airport": airport,
                "reason": "Critical risk level requires human review",
            },
            "risk_level": "HIGH",
            "requires_approval": True,
        })

    # Shipment-level actions only if we have booking evidence
    if has_booking_evidence and posture in ("HOLD", "ESCALATE"):
        actions.append({
            "type": "HOLD_CARGO",
            "args": {
                "airport": airport,
                "reason": f"Holding cargo at {airport} due to {posture} posture",
            },
            "risk_level": risk_level,
            "requires_approval": True,
        })

    return actions


def _get_graph_context(airport: str) -> Dict[str, Any]:
    """Get graph context for the airport from database."""
    session = SessionLocal()
    try:
        # Get cascade impact
        cascade = cascade_from_airport(airport, session=session)

        # Get shipments with booking evidence
        shipments_with_booking = get_shipments_with_booking_evidence(airport, session=session)

        # Get node count
        node_result = session.execute(
            "SELECT type, COUNT(*) FROM node GROUP BY type"
        )
        node_counts = {row[0]: row[1] for row in node_result}

        # Get edge count
        edge_result = session.execute(
            "SELECT type, COUNT(*) FROM edge GROUP BY type"
        )
        edge_counts = {row[0]: row[1] for row in edge_result}

        return {
            "cascade_analysis": {
                "airport": cascade.airport_icao,
                "affected_flights": cascade.affected_flights[:10],
                "affected_shipments": cascade.affected_shipments[:10],
                "affected_bookings": cascade.affected_bookings[:10],
                "totals": {
                    "flights": len(cascade.affected_flights),
                    "shipments": cascade.total_shipments,
                    "bookings": cascade.total_bookings,
                    "revenue_at_risk_usd": cascade.total_revenue_at_risk,
                    "weight_kg": cascade.total_weight_kg,
                    "sla_at_risk": cascade.sla_at_risk_count,
                    "premium_sla_at_risk": cascade.premium_sla_at_risk,
                    "express_sla_at_risk": cascade.express_sla_at_risk,
                },
            },
            "shipments_with_booking_evidence": shipments_with_booking[:10],
            "has_booking_evidence": len(shipments_with_booking) > 0,
            "graph_statistics": {
                "node_counts": node_counts,
                "edge_counts": edge_counts,
            },
        }
    except Exception as e:
        return {
            "error": str(e),
            "cascade_analysis": None,
            "shipments_with_booking_evidence": [],
            "has_booking_evidence": False,
            "graph_statistics": {"node_counts": {}, "edge_counts": {}},
        }
    finally:
        session.close()


@router.post("")
async def run_sandbox(request: SandboxRequest) -> Dict[str, Any]:
    """
    Run full sandbox with REAL API data.

    Fetches live data from all 5 sources, derives claims, detects contradictions,
    assesses risk, proposes actions, and returns everything.

    Args:
        request: Sandbox request with airport and optional overrides

    Returns:
        Complete system output including:
        - Raw API data (real)
        - Evidence
        - Claims
        - Contradictions
        - Risk assessment
        - Posture decision
        - Proposed actions
        - Graph context (cascade, edges, nodes)
        - Bi-temporal data
    """
    airport = request.airport.upper()

    # Validate US airport
    prefix1 = airport[:1]
    prefix2 = airport[:2]
    if prefix1 not in ('K', 'P') and prefix2 not in ('TJ', 'TI'):
        raise HTTPException(
            status_code=400,
            detail=f"Only US airports supported (K*, P*, TJ*, TI*). Got: {airport}"
        )

    now = datetime.now(timezone.utc)

    # ================================================================
    # STEP 1: FETCH REAL DATA FROM ALL 5 APIS
    # ================================================================
    registry = get_registry()
    try:
        ingestion = registry.ingest_airport(airport, include_opensky=request.include_opensky)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Build raw data response
    raw_data = {
        "airport": airport,
        "fetched_at": now.isoformat(),
        "sources": {},
    }

    for result in ingestion.all_results:
        raw_data["sources"][result.source] = {
            "success": result.success,
            "retrieved_at": result.retrieved_at.isoformat() if result.retrieved_at else None,
            "data": _serialize_ingestion_data(result.data),
            "error": result.error,
        }

    # ================================================================
    # STEP 2: CREATE EVIDENCE RECORDS
    # ================================================================
    evidence = []
    for result in ingestion.all_results:
        if result.success:
            evidence.append({
                "id": str(uuid4()),
                "source_system": result.source,
                "retrieved_at": result.retrieved_at.isoformat() if result.retrieved_at else now.isoformat(),
                "content_type": "application/json",
                "data_summary": _serialize_ingestion_data(result.data),
                "status": "normal_operations" if result.data is None or (isinstance(result.data, list) and len(result.data) == 0) else "has_data",
            })

    # ================================================================
    # STEP 3: DERIVE CLAIMS (what the system believes)
    # ================================================================
    claims = _derive_claims(ingestion)

    # ================================================================
    # STEP 4: DETECT CONTRADICTIONS
    # ================================================================
    contradictions = _detect_contradictions(claims)

    # ================================================================
    # STEP 5: ASSESS RISK
    # ================================================================
    risk_assessment = _assess_risk(claims, contradictions)

    # ================================================================
    # STEP 6: GET GRAPH CONTEXT (cascade, nodes, edges)
    # ================================================================
    graph_context = _get_graph_context(airport)
    has_booking_evidence = graph_context.get("has_booking_evidence", False)

    # ================================================================
    # STEP 7: PROPOSE ACTIONS
    # ================================================================
    proposed_actions = _propose_actions(risk_assessment, airport, has_booking_evidence)

    # ================================================================
    # STEP 8: BUILD DECISION PACKET
    # ================================================================
    posture = risk_assessment["recommended_posture"]

    decision_packet = {
        "case_id": str(uuid4()),  # Ephemeral case
        "case_type": "AIRPORT_DISRUPTION",
        "scope": {"airport": airport},
        "created_at": now.isoformat(),
        "posture_decision": {
            "posture": posture,
            "airport": airport,
            "effective_at": now.isoformat(),
            "reason": f"Risk level {risk_assessment['risk_level']} from {risk_assessment['evidence_count']} sources",
        },
        "metrics": {
            "first_signal_at": min(e["retrieved_at"] for e in evidence) if evidence else now.isoformat(),
            "posture_emitted_at": now.isoformat(),
            "evidence_count": len(evidence),
            "claim_count": len(claims),
            "contradiction_count": len(contradictions),
            "action_count": len(proposed_actions),
        },
    }

    # ================================================================
    # STEP 9: BUILD FULL RESPONSE
    # ================================================================
    return {
        "sandbox_run_id": str(uuid4()),
        "airport": airport,
        "timestamp": now.isoformat(),

        # ============================================================
        # RAW API DATA (from real sources)
        # ============================================================
        "raw_api_data": raw_data,

        # ============================================================
        # EVIDENCE (what we stored)
        # ============================================================
        "evidence": evidence,

        # ============================================================
        # CLAIMS (what the system believes based on evidence)
        # ============================================================
        "claims": claims,

        # ============================================================
        # CONTRADICTIONS (conflicts between claims)
        # ============================================================
        "contradictions": contradictions,

        # ============================================================
        # RISK ASSESSMENT
        # ============================================================
        "risk_assessment": risk_assessment,

        # ============================================================
        # POSTURE DECISION
        # ============================================================
        "posture_decision": {
            "posture": posture,
            "airport": airport,
            "effective_at": now.isoformat(),
            "reason": f"Based on {risk_assessment['evidence_count']} evidence sources, {len(risk_assessment['risk_factors'])} risk factors",
        },

        # ============================================================
        # PROPOSED ACTIONS
        # ============================================================
        "proposed_actions": proposed_actions,

        # ============================================================
        # GRAPH CONTEXT (cascade analysis, nodes, edges)
        # ============================================================
        "graph_context": graph_context,

        # ============================================================
        # DECISION PACKET (complete output)
        # ============================================================
        "decision_packet": decision_packet,

        # ============================================================
        # SUMMARY
        # ============================================================
        "summary": {
            "airport": airport,
            "posture": posture,
            "risk_level": risk_assessment["risk_level"],
            "confidence": risk_assessment["overall_confidence"],
            "sources_fetched": len([r for r in ingestion.all_results if r.success]),
            "sources_failed": len([r for r in ingestion.all_results if not r.success]),
            "claims_derived": len(claims),
            "contradictions_detected": len(contradictions),
            "actions_proposed": len(proposed_actions),
            "actions_requiring_approval": len([a for a in proposed_actions if a.get("requires_approval")]),
            "has_booking_evidence": has_booking_evidence,
            "cascade_impact": {
                "flights": graph_context.get("cascade_analysis", {}).get("totals", {}).get("flights", 0),
                "shipments": graph_context.get("cascade_analysis", {}).get("totals", {}).get("shipments", 0),
                "bookings": graph_context.get("cascade_analysis", {}).get("totals", {}).get("bookings", 0),
                "revenue_at_risk": graph_context.get("cascade_analysis", {}).get("totals", {}).get("revenue_at_risk_usd", 0),
            },
        },
    }


@router.get("/status")
async def sandbox_status() -> Dict[str, Any]:
    """Get sandbox status and available sources."""
    return {
        "status": "ready",
        "available_sources": [
            {"id": "FAA_NAS", "name": "FAA NAS Status", "endpoint": "nasstatus.faa.gov"},
            {"id": "METAR", "name": "Aviation Weather METAR", "endpoint": "aviationweather.gov"},
            {"id": "TAF", "name": "Aviation Weather TAF", "endpoint": "aviationweather.gov"},
            {"id": "NWS_ALERTS", "name": "NWS Weather Alerts", "endpoint": "api.weather.gov"},
            {"id": "OPENSKY", "name": "OpenSky ADS-B", "endpoint": "opensky-network.org"},
        ],
        "supported_airports": "US airports only (K*, P*, TJ*, TI* ICAO prefixes)",
        "example_airports": ["KJFK", "KLAX", "KORD", "KATL", "KDFW", "KDEN", "PHNL"],
    }
