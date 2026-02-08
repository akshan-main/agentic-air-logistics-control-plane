# simulation/api.py
"""
API endpoints for simulation.

Two types of simulation:
1. Disruption Scenarios - Use controlled FAA/weather data for testing posture logic
2. Operational Data Seeding - Populate graph with flights, shipments, bookings
   for testing cascade analysis and shipment-level actions
"""

import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .scenarios import SCENARIOS, get_scenario, list_scenarios
from .runner import SimulationRunner
from .graph_seeder import (
    GraphSeeder,
    seed_graph_for_airport,
    clear_seeded_operational_data_for_airport,
)
from .operational_data import OperationalDataGenerator
from .seeders import seed_playbooks, list_seeded_playbooks, clear_playbooks


router = APIRouter(prefix="/simulation", tags=["simulation"])


class RunScenarioRequest(BaseModel):
    """Request to run a simulation scenario."""
    scenario_id: str
    use_streaming: bool = False


class RunBatchRequest(BaseModel):
    """Request to run multiple scenarios."""
    scenario_ids: Optional[List[str]] = None  # None = run all


class SeedOperationalDataRequest(BaseModel):
    """Request to seed operational data for an airport."""
    airport_icao: str
    num_flights: int = 20
    shipments_per_flight: int = 5
    seed: Optional[int] = None


@router.get("/scenarios")
async def get_scenarios() -> Dict[str, Any]:
    """
    List all available simulation scenarios.

    Returns scenario metadata including expected posture and risk level.
    """
    scenarios = list_scenarios()
    return {
        "scenarios": scenarios,
        "count": len(scenarios),
        "categories": {
            "normal_operations": [s for s in scenarios if s["expected_posture"] == "ACCEPT"],
            "restrict": [s for s in scenarios if s["expected_posture"] == "RESTRICT"],
            "hold": [s for s in scenarios if s["expected_posture"] == "HOLD"],
            "escalate": [s for s in scenarios if s["expected_posture"] == "ESCALATE"],
            "contradiction": [s for s in scenarios if s["has_contradiction"]],
            "degraded": [s for s in scenarios if s["has_missing_source"]],
        },
    }


@router.get("/scenarios/{scenario_id}")
async def get_scenario_detail(scenario_id: str) -> Dict[str, Any]:
    """
    Get detailed information about a specific scenario.

    Includes all source data (FAA, METAR, TAF, NWS, OpenSky).
    """
    scenario = get_scenario(scenario_id)
    if scenario is None:
        raise HTTPException(status_code=404, detail=f"Scenario not found: {scenario_id}")

    return {
        "id": scenario.id,
        "name": scenario.name,
        "description": scenario.description.strip(),
        "airport_icao": scenario.airport_icao,
        "expected_posture": scenario.expected_posture.value,
        "expected_risk_level": scenario.expected_risk_level,
        "has_contradiction": scenario.has_contradiction,
        "has_missing_source": scenario.has_missing_source,
        "missing_source": scenario.missing_source,
        "source_data": {
            "faa": scenario.faa_data,
            "metar": scenario.metar_data,
            "taf": scenario.taf_data,
            "nws_alerts": scenario.nws_alerts,
            "opensky": scenario.opensky_data,
        },
    }


@router.post("/run/{scenario_id}")
async def run_scenario(scenario_id: str) -> Dict[str, Any]:
    """
    Run a simulation scenario through the full multi-agent pipeline.

    Returns the simulation result including whether the actual posture
    matched the expected posture.
    """
    scenario = get_scenario(scenario_id)
    if scenario is None:
        raise HTTPException(status_code=404, detail=f"Scenario not found: {scenario_id}")

    try:
        with SimulationRunner() as runner:
            result = runner.run_scenario(scenario_id)
            return result.to_dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run/{scenario_id}/stream")
async def run_scenario_streaming(scenario_id: str):
    """
    Run a simulation scenario with streaming progress.

    Returns Server-Sent Events with progress updates.
    """
    scenario = get_scenario(scenario_id)
    if scenario is None:
        raise HTTPException(status_code=404, detail=f"Scenario not found: {scenario_id}")

    async def event_generator():
        try:
            with SimulationRunner() as runner:
                for event in runner.run_scenario_streaming(scenario_id):
                    yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'event': 'error', 'error': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@router.post("/run-batch")
async def run_batch(request: RunBatchRequest) -> Dict[str, Any]:
    """
    Run multiple scenarios and return aggregated results.

    If scenario_ids is None, runs all scenarios.
    """
    try:
        with SimulationRunner() as runner:
            if request.scenario_ids is None:
                # Run all
                batch = runner.run_all_scenarios()
            else:
                # Run specific scenarios
                from .runner import SimulationBatchResult
                batch = SimulationBatchResult(
                    total=len(request.scenario_ids),
                    passed=0,
                    failed=0,
                )
                for scenario_id in request.scenario_ids:
                    result = runner.run_scenario(scenario_id)
                    batch.results.append(result)
                    if result.passed:
                        batch.passed += 1
                    else:
                        batch.failed += 1
                batch.completed_at = datetime.now(timezone.utc)
                batch.duration_seconds = (
                    batch.completed_at - batch.started_at
                ).total_seconds()

            return batch.to_dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/run-all")
async def run_all_scenarios() -> Dict[str, Any]:
    """
    Run all available scenarios and return summary.

    Useful for validation testing.
    """
    try:
        with SimulationRunner() as runner:
            batch = runner.run_all_scenarios()
            return batch.to_dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/validate")
async def validate_scenarios() -> Dict[str, Any]:
    """
    Quick validation that all scenarios are properly defined.

    Checks that each scenario has required data for its expected outcome.
    """
    issues = []

    for scenario_id, scenario in SCENARIOS.items():
        scenario_issues = []

        # Check expected posture has supporting data
        if scenario.expected_posture.value == "HOLD":
            # HOLD should have either ground stop or severe conditions
            if scenario.faa_data is None and scenario.metar_data is None:
                scenario_issues.append("HOLD posture but no FAA or METAR data")
            elif scenario.faa_data and not scenario.faa_data.get("ground_stop"):
                if scenario.metar_data and scenario.metar_data.get("flight_category") not in ["IFR", "LIFR"]:
                    scenario_issues.append("HOLD posture but no severe conditions")

        if scenario.expected_posture.value == "ESCALATE":
            # ESCALATE should have critical conditions
            if scenario.faa_data is None or not scenario.faa_data.get("closure"):
                if not any(a.get("severity") == "Extreme" for a in scenario.nws_alerts):
                    scenario_issues.append("ESCALATE posture but no critical conditions")

        if scenario.expected_posture.value == "ACCEPT":
            # ACCEPT should have normal conditions
            if scenario.faa_data and scenario.faa_data.get("ground_stop"):
                scenario_issues.append("ACCEPT posture but has ground stop")

        # Check contradiction flag
        if scenario.has_contradiction:
            # Should have conflicting data
            if scenario.faa_data is None and scenario.metar_data:
                # FAA normal but METAR bad - good
                pass
            elif scenario.faa_data and scenario.metar_data:
                # Both present - check for contradiction
                pass
            else:
                scenario_issues.append("has_contradiction but no contradicting data")

        # Check missing source flag
        if scenario.has_missing_source:
            if scenario.missing_source is None:
                scenario_issues.append("has_missing_source but missing_source not specified")

        if scenario_issues:
            issues.append({
                "scenario_id": scenario_id,
                "issues": scenario_issues,
            })

    return {
        "valid": len(issues) == 0,
        "scenario_count": len(SCENARIOS),
        "issues": issues,
    }


# ============================================================
# OPERATIONAL DATA SEEDING ENDPOINTS
# ============================================================
# These endpoints populate the context graph with simulated
# operational data (flights, shipments, bookings) for testing
# cascade analysis and shipment-level actions.


@router.post("/seed/airport/{airport_icao}")
async def seed_operational_data(
    airport_icao: str,
    num_flights: int = 20,
    shipments_per_flight: int = 5,
    seed: Optional[int] = None,
    refresh: bool = True,
) -> Dict[str, Any]:
    """
    Seed the context graph with simulated operational data for an airport.

    Creates:
    - CARRIER nodes (airlines)
    - FLIGHT nodes (connecting airports)
    - SHIPMENT nodes (cargo on flights)
    - BOOKING nodes (evidence for shipment actions)
    - DOCUMENT nodes (AWB, customs, invoices)

    Edges connecting all entities enable cascade traversal:
    Airport disruption → affected flights → affected shipments

    Args:
        airport_icao: Airport ICAO code (e.g., KJFK)
        num_flights: Number of flights to generate
        shipments_per_flight: Shipments per flight
        refresh: If True, clears existing SIMULATION ops data touching this airport before seeding

    Returns:
        Summary of seeded data
    """
    # Validate ICAO format
    airport_icao = airport_icao.upper()
    if not airport_icao.startswith(('K', 'P', 'TJ', 'TI')):
        raise HTTPException(
            status_code=400,
            detail=f"Only US airports supported (K*, P*, TJ*, TI*): {airport_icao}"
        )

    try:
        from app.db.engine import session_scope

        cleared: Optional[Dict[str, Any]] = None
        result: Dict[str, Any]

        # Refresh is atomic: if seeding fails, clear is rolled back.
        with session_scope() as session:
            if refresh:
                cleared = clear_seeded_operational_data_for_airport(
                    airport_icao,
                    session=session,
                    commit=False,
                )
            result = seed_graph_for_airport(
                airport_icao=airport_icao,
                num_flights=num_flights,
                shipments_per_flight=shipments_per_flight,
                seed=seed,
                session=session,
                commit=False,
            )
        return {
            "status": "success",
            "message": (
                f"Refreshed operational data for {airport_icao}"
                if refresh
                else f"Seeded operational data for {airport_icao}"
            ),
            "refreshed": refresh,
            "cleared": cleared,
            **result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/seed/batch")
async def seed_batch_airports(request: SeedOperationalDataRequest) -> Dict[str, Any]:
    """
    Seed operational data for a single airport (POST body version).

    Same as /seed/airport/{icao} but accepts JSON body.
    """
    return await seed_operational_data(
        airport_icao=request.airport_icao,
        num_flights=request.num_flights,
        shipments_per_flight=request.shipments_per_flight,
        seed=request.seed,
    )


@router.delete("/seed/airport/{airport_icao}")
async def clear_operational_data(airport_icao: str) -> Dict[str, Any]:
    """
    Clear SIMULATION-seeded operational data for an airport.

    This removes simulated operational nodes/edges (flights/shipments/bookings)
    without touching disruption evidence/claims.
    """
    airport_icao = airport_icao.upper()
    if not airport_icao.startswith(('K', 'P', 'TJ', 'TI')):
        raise HTTPException(
            status_code=400,
            detail=f"Only US airports supported (K*, P*, TJ*, TI*): {airport_icao}"
        )

    try:
        result = clear_seeded_operational_data_for_airport(airport_icao)
        return {
            "status": "success",
            "message": f"Cleared SIMULATION operational data for {airport_icao}",
            **result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/seed/preview/{airport_icao}")
async def preview_operational_data(
    airport_icao: str,
    num_flights: int = 10,
    shipments_per_flight: int = 3
) -> Dict[str, Any]:
    """
    Preview what operational data would be generated (without saving).

    Useful for understanding the data structure before seeding.
    """
    airport_icao = airport_icao.upper()
    if not airport_icao.startswith(('K', 'P', 'TJ', 'TI')):
        raise HTTPException(
            status_code=400,
            detail=f"Only US airports supported: {airport_icao}"
        )

    try:
        generator = OperationalDataGenerator(seed=42)  # Deterministic for preview
        data = generator.generate_full_dataset_for_airport(
            airport_icao=airport_icao,
            num_flights=num_flights,
            shipments_per_flight=shipments_per_flight
        )

        # Convert to serializable format
        return {
            "airport": data["airport"],
            "preview": True,
            "stats": data["stats"],
            "sample_data": {
                "carriers": [
                    {"id": c.id, "name": c.name, "iata_code": c.iata_code}
                    for c in data["carriers"][:3]
                ],
                "flights": [
                    {
                        "id": f.id,
                        "flight_number": f.flight_number,
                        "origin": f.origin_icao,
                        "destination": f.destination_icao,
                        "status": f.status,
                    }
                    for f in data["flights"][:5]
                ],
                "shipments": [
                    {
                        "id": s.id,
                        "tracking_number": s.tracking_number,
                        "commodity": s.commodity,
                        "weight_kg": s.weight_kg,
                        "service_level": s.service_level,
                    }
                    for s in data["shipments"][:5]
                ],
                "bookings": [
                    {
                        "id": b.id,
                        "booking_reference": b.booking_reference,
                        "total_charge_usd": b.total_charge_usd,
                        "rate_per_kg": b.rate_per_kg,
                        "margin_percent": b.margin_percent,
                        "sla_deadline": b.sla_deadline.isoformat(),
                    }
                    for b in data["bookings"][:5]
                ],
            },
            "edge_types_created": [
                "CARRIER_OPERATES_FLIGHT",
                "FLIGHT_DEPARTS_FROM",
                "FLIGHT_ARRIVES_AT",
                "SHIPMENT_ON_FLIGHT",
                "SHIPMENT_ORIGIN",
                "SHIPMENT_DESTINATION",
                "BOOKING_FOR_SHIPMENT",
                "BOOKING_WITH_CARRIER",
                "DOCUMENT_FOR_SHIPMENT",
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/graph/operational-stats")
async def get_operational_graph_stats() -> Dict[str, Any]:
    """
    Get statistics about operational data in the context graph.

    Shows counts of carriers, flights, shipments, bookings, documents.
    """
    from app.db.engine import SessionLocal
    from sqlalchemy import text

    try:
        with SessionLocal() as session:
            # Count nodes by type
            result = session.execute(text("""
                SELECT type, COUNT(*) as count
                FROM node
                WHERE type IN ('CARRIER', 'FLIGHT', 'SHIPMENT', 'BOOKING', 'DOCUMENT', 'AIRPORT')
                GROUP BY type
                ORDER BY type
            """))
            node_counts = {row[0]: row[1] for row in result}

            # Count edges by type
            result = session.execute(text("""
                SELECT type, COUNT(*) as count
                FROM edge
                WHERE type IN (
                    'CARRIER_OPERATES_FLIGHT', 'FLIGHT_DEPARTS_FROM', 'FLIGHT_ARRIVES_AT',
                    'SHIPMENT_ON_FLIGHT', 'SHIPMENT_ORIGIN', 'SHIPMENT_DESTINATION',
                    'BOOKING_FOR_SHIPMENT', 'BOOKING_WITH_CARRIER', 'DOCUMENT_FOR_SHIPMENT'
                )
                GROUP BY type
                ORDER BY type
            """))
            edge_counts = {row[0]: row[1] for row in result}

            # Get airports with operational data
            result = session.execute(text("""
                SELECT DISTINCT n.identifier
                FROM node n
                JOIN edge e ON n.id = e.dst
                WHERE n.type = 'AIRPORT'
                AND e.type IN ('FLIGHT_DEPARTS_FROM', 'FLIGHT_ARRIVES_AT')
                ORDER BY n.identifier
            """))
            airports_with_ops = [row[0] for row in result]

        return {
            "node_counts": node_counts,
            "edge_counts": edge_counts,
            "airports_with_operational_data": airports_with_ops,
            "has_operational_data": len(node_counts) > 1,  # More than just AIRPORT
            "total_nodes": sum(node_counts.values()),
            "total_edges": sum(edge_counts.values()),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# PLAYBOOK SEEDING ENDPOINTS
# ============================================================
# Seed playbooks from simulation scenarios so the orchestrator
# can match against known patterns.


@router.post("/seed/playbooks")
async def seed_playbooks_endpoint() -> Dict[str, Any]:
    """
    Seed playbooks from simulation scenarios.

    Creates one playbook per scenario representing the expected resolution
    pattern. These playbooks can then be matched during case execution to
    guide decisions based on learned patterns.

    Returns:
        List of created playbooks
    """
    try:
        created = seed_playbooks()
        return {
            "status": "success",
            "message": f"Seeded {len(created)} playbooks from scenarios",
            "playbooks": created,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/playbooks")
async def list_playbooks_endpoint() -> Dict[str, Any]:
    """
    List all seeded playbooks.

    Shows playbook metadata including use count and success rate.
    """
    try:
        playbooks = list_seeded_playbooks()
        return {
            "playbooks": playbooks,
            "count": len(playbooks),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/playbooks")
async def clear_playbooks_endpoint() -> Dict[str, Any]:
    """
    Clear all playbooks (for re-seeding).

    Warning: This deletes all playbooks and their usage stats.
    """
    try:
        count = clear_playbooks()
        return {
            "status": "success",
            "message": f"Deleted {count} playbooks",
            "deleted_count": count,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
