# app/api/routes_ingest.py
"""
Ingestion API routes.

Endpoints for ingesting data from external sources.
"""

import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from ..db.engine import SessionLocal
from ..ingestion.registry import get_registry
from ..evidence.store import store_evidence, EVIDENCE_ROOT
from ..evidence.extract import extract_excerpt

router = APIRouter(prefix="/ingest", tags=["ingestion"])


class IngestAirportRequest(BaseModel):
    """Request to ingest data for an airport."""
    include_opensky: bool = True  # Optional - can be slow/rate-limited


class IngestAirportResponse(BaseModel):
    """Response from airport ingestion."""
    icao: str
    sources_attempted: List[str]
    sources_succeeded: List[str]
    sources_failed: List[str]
    errors: List[Dict[str, Any]]


class BatchIngestRequest(BaseModel):
    """Request to ingest multiple airports."""
    airports: List[str]  # ICAO codes
    include_opensky: bool = True


class BatchIngestResponse(BaseModel):
    """Response from batch ingestion."""
    total_airports: int
    succeeded: int
    failed: int
    results: List[IngestAirportResponse]


@router.post("/airport/{icao}", response_model=IngestAirportResponse)
async def ingest_airport(
    icao: str,
    request: Optional[IngestAirportRequest] = None,
) -> IngestAirportResponse:
    """
    Ingest all sources for a US airport.

    Fetches data from all sources and STORES it in the database.
    This data can then be used by cases created for this airport.

    Args:
        icao: Airport ICAO code (US airports: K*, P*, TJ*, TI*)
              Examples: KJFK, KLAX, PHNL, TJSJ, TISX
        request: Optional request with source filters

    Returns:
        Ingestion results

    Raises:
        HTTPException 400: If airport is not a US airport
    """
    icao = icao.upper()
    include_opensky = request.include_opensky if request else True

    registry = get_registry()

    try:
        result = registry.ingest_airport(icao, include_opensky=include_opensky)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Collect results
    sources_attempted = []
    sources_succeeded = []
    sources_failed = []
    errors = []

    # Store evidence to database
    session = SessionLocal()
    try:
        for ing_result in result.all_results:
            sources_attempted.append(ing_result.source)
            if ing_result.success:
                sources_succeeded.append(ing_result.source)

                # Store evidence - ALWAYS store for successful ingestions
                # Normal/empty conditions are important evidence too
                if ing_result.data:
                    # Handle lists properly (e.g., NWS alerts)
                    if isinstance(ing_result.data, list):
                        if len(ing_result.data) > 0:
                            raw_bytes = json.dumps(
                                [item.__dict__ if hasattr(item, '__dict__') else item
                                 for item in ing_result.data],
                                default=str
                            ).encode('utf-8')
                        else:
                            # Empty list - evidence of normal conditions
                            raw_bytes = json.dumps({
                                "status": "normal_operations",
                                "source": ing_result.source,
                                "message": f"No active alerts/data from {ing_result.source} (normal conditions)"
                            }).encode('utf-8')
                    elif isinstance(ing_result.data, dict) and not ing_result.data:
                        # Empty dict - evidence of normal conditions
                        raw_bytes = json.dumps({
                            "status": "normal_operations",
                            "source": ing_result.source,
                            "message": f"No disruptions reported by {ing_result.source} (normal conditions)"
                        }).encode('utf-8')
                    else:
                        raw_bytes = json.dumps(
                            ing_result.data.__dict__ if hasattr(ing_result.data, '__dict__')
                            else ing_result.data,  # Don't use str() - let json.dumps handle it
                            default=str
                        ).encode('utf-8')
                else:
                    # None/falsy data - evidence of normal conditions
                    # This is IMPORTANT: FAA returns None for normal operations,
                    # and we must store this as evidence that we checked and it was normal
                    raw_bytes = json.dumps({
                        "status": "normal_operations",
                        "source": ing_result.source,
                        "message": f"No disruptions reported by {ing_result.source} (normal conditions)",
                        "checked_at": ing_result.retrieved_at.isoformat() if ing_result.retrieved_at else datetime.now(timezone.utc).isoformat()
                    }).encode('utf-8')

                sha256 = store_evidence(raw_bytes)
                excerpt = extract_excerpt(raw_bytes)

                # Create evidence record (idempotent - skip if same source+ref+hash exists)
                evidence_id = uuid4()
                try:
                    result = session.execute(
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
                            "source_system": ing_result.source,
                            "source_ref": f"airport:{icao}",
                            "retrieved_at": ing_result.retrieved_at,
                            "content_type": "application/json",
                            "payload_sha256": sha256,
                            "raw_path": str(EVIDENCE_ROOT / f"{sha256}.bin"),
                            "excerpt": excerpt,
                            "meta": json.dumps({"airport_icao": icao}),
                        }
                    )
                except ProgrammingError as e:
                    # Common failure mode when migrations weren't applied (missing unique index for ON CONFLICT).
                    session.rollback()
                    msg = str(getattr(e, "orig", e))
                    if "no unique or exclusion constraint matching the ON CONFLICT specification" in msg:
                        raise HTTPException(
                            status_code=500,
                            detail=(
                                "Database schema is missing the evidence dedup constraint required for ingestion upserts. "
                                "Apply migrations: `./setup.sh` (idempotent) or `make migrate`."
                            ),
                        )
                    raise
                # Use the actual ID (could be existing row on conflict)
                row = result.fetchone()
                if row:
                    evidence_id = row[0]

                # If this source previously failed for this airport, mark that missing evidence as resolved.
                session.execute(
                    text("""
                        UPDATE missing_evidence_request
                        SET resolved_at = :resolved_at,
                            resolved_by_evidence_id = :evidence_id
                        WHERE source_system = :source_system
                          AND request_type = :request_type
                          AND resolved_at IS NULL
                    """),
                    {
                        "resolved_at": datetime.now(timezone.utc),
                        "evidence_id": evidence_id,
                        "source_system": ing_result.source,
                        "request_type": f"airport_ingestion:{icao}",
                    }
                )
            else:
                sources_failed.append(ing_result.source)
                error_msg = ing_result.error or "Unknown error"
                errors.append({
                    "source": ing_result.source,
                    "error": error_msg,
                })

                # Persist failure as missing_evidence_request (no case_id yet —
                # will be linked when a case picks up this airport)
                criticality = "DEGRADED"
                if ing_result.source == "FAA_NAS":
                    criticality = "BLOCKING"  # FAA is critical for posture
                elif ing_result.source == "METAR":
                    criticality = "BLOCKING"  # METAR is current conditions; required for defensible posture
                elif ing_result.source == "TAF":
                    criticality = "DEGRADED"
                elif ing_result.source == "OPENSKY":
                    criticality = "INFORMATIONAL"

                session.execute(
                    text("""
                        INSERT INTO missing_evidence_request
                        (id, case_id, source_system, request_type, request_params,
                         reason, criticality, created_at)
                        VALUES
                        (:id, NULL, :source_system, :request_type,
                         CAST(:request_params AS jsonb), :reason, :criticality, :created_at)
                    """),
                    {
                        "id": uuid4(),
                        "source_system": ing_result.source,
                        "request_type": f"airport_ingestion:{icao}",
                        "request_params": json.dumps({"airport_icao": icao}),
                        "reason": error_msg,
                        "criticality": criticality,
                        "created_at": datetime.now(timezone.utc),
                    }
                )

        session.commit()
    finally:
        session.close()

    return IngestAirportResponse(
        icao=icao,
        sources_attempted=sources_attempted,
        sources_succeeded=sources_succeeded,
        sources_failed=sources_failed,
        errors=errors,
    )


@router.post("/batch", response_model=BatchIngestResponse)
async def ingest_batch(
    request: BatchIngestRequest,
) -> BatchIngestResponse:
    """
    Batch ingest multiple airports.

    Args:
        request: Batch ingestion request

    Returns:
        Batch ingestion results
    """
    results = []
    succeeded = 0
    failed = 0

    for icao in request.airports:
        try:
            result = await ingest_airport(
                icao,
                IngestAirportRequest(include_opensky=request.include_opensky),
            )
            results.append(result)
            if result.sources_succeeded:
                succeeded += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            results.append(IngestAirportResponse(
                icao=icao.upper(),
                sources_attempted=[],
                sources_succeeded=[],
                sources_failed=["all"],
                errors=[{"error": str(e)}],
            ))

    return BatchIngestResponse(
        total_airports=len(request.airports),
        succeeded=succeeded,
        failed=failed,
        results=results,
    )


@router.get("/sources")
async def list_sources() -> Dict[str, Any]:
    """
    List available ingestion sources.

    Returns:
        Available sources and their status
    """
    return {
        "coverage": {
            "region": "US and US territories",
            "icao_prefixes": ["K*", "P*", "TJ*", "TI*"],
            "description": "Continental US (K*), Pacific territories (P*), Puerto Rico (TJ*), US Virgin Islands (TI*)",
        },
        "sources": [
            {
                "id": "faa_nas",
                "name": "FAA NAS Status",
                "url": "https://nasstatus.faa.gov/api/airport-status-information",
                "data_types": ["ground_stops", "ground_delays", "closures"],
            },
            {
                "id": "nws_alerts",
                "name": "NWS Weather Alerts",
                "url": "https://api.weather.gov/alerts/active",
                "data_types": ["weather_alerts", "warnings", "watches"],
            },
            {
                "id": "metar",
                "name": "Aviation Weather METAR",
                "url": "https://aviationweather.gov/api/data/metar",
                "data_types": ["current_conditions", "wind", "visibility", "ceiling"],
            },
            {
                "id": "taf",
                "name": "Aviation Weather TAF",
                "url": "https://aviationweather.gov/api/data/taf",
                "data_types": ["forecast", "predicted_conditions"],
            },
            {
                "id": "opensky",
                "name": "OpenSky ADS-B",
                "url": "https://opensky-network.org/api/states/all",
                "data_types": ["aircraft_positions", "movement_data"],
                "note": "Rate limited - may be slow or unavailable",
            },
        ],
    }


# =============================================================================
# SIMULATION ENDPOINTS - For testing different posture outcomes
# =============================================================================

SIMULATION_SCENARIOS = {
    "ground_stop": {
        "description": "FAA Ground Stop - Tests HOLD posture",
        "faa_status": {
            "delay": True,
            "delay_type": "GROUND_STOP",
            "reason": "WEATHER / THUNDERSTORMS",
            "avg_delay_minutes": 120,
            "closure": False,
        },
        "metar": {
            "wind_speed": 25,
            "wind_gust": 45,
            "visibility_miles": 3,
            "ceiling_feet": 800,
        },
        "expected_posture": "HOLD",
    },
    "severe_weather": {
        "description": "Severe Weather - Tests RESTRICT posture",
        "faa_status": {
            "delay": True,
            "delay_type": "GROUND_DELAY",
            "reason": "WEATHER / LOW VISIBILITY",
            "avg_delay_minutes": 45,
            "closure": False,
        },
        "metar": {
            "wind_speed": 20,
            "wind_gust": 35,
            "visibility_miles": 2,
            "ceiling_feet": 500,
        },
        "expected_posture": "RESTRICT",
    },
    "major_disruption": {
        "description": "Major Disruption - Tests ESCALATE posture",
        "faa_status": {
            "delay": True,
            "delay_type": "GROUND_STOP",
            "reason": "EQUIPMENT / AIRPORT CLOSED",
            "avg_delay_minutes": 240,
            "closure": True,
        },
        "metar": {
            "wind_speed": 40,
            "wind_gust": 60,
            "visibility_miles": 0.5,
            "ceiling_feet": 200,
        },
        "nws_alerts": [
            {"event": "Severe Thunderstorm Warning", "severity": "Severe"},
            {"event": "Tornado Watch", "severity": "Extreme"},
        ],
        "expected_posture": "ESCALATE",
    },
    "normal": {
        "description": "Normal Operations - Tests ACCEPT posture",
        "faa_status": None,
        "metar": {
            "wind_speed": 8,
            "wind_gust": None,
            "visibility_miles": 10,
            "ceiling_feet": 5000,
        },
        "expected_posture": "ACCEPT",
    },
}


@router.get("/simulate/scenarios")
async def list_simulation_scenarios() -> Dict[str, Any]:
    """
    List available simulation scenarios for testing.

    Returns:
        Available scenarios with expected postures
    """
    return {
        "scenarios": [
            {
                "id": scenario_id,
                "description": scenario["description"],
                "expected_posture": scenario["expected_posture"],
            }
            for scenario_id, scenario in SIMULATION_SCENARIOS.items()
        ],
        "usage": "POST /ingest/simulate/{icao}?scenario={scenario_id}",
    }


@router.post("/simulate/{icao}")
async def simulate_ingestion(
    icao: str,
    scenario: str = "ground_stop",
) -> Dict[str, Any]:
    """
    Simulate ingestion with a test scenario.

    This injects synthetic data to test different posture outcomes
    without waiting for real-world disruptions.

    Args:
        icao: Airport ICAO code (any code works for simulation)
        scenario: Scenario ID (ground_stop, severe_weather, major_disruption, normal)

    Returns:
        Simulated ingestion results
    """
    if scenario not in SIMULATION_SCENARIOS:
        return {
            "error": f"Unknown scenario: {scenario}",
            "available": list(SIMULATION_SCENARIOS.keys()),
        }

    sim = SIMULATION_SCENARIOS[scenario]

    return {
        "icao": icao.upper(),
        "scenario": scenario,
        "description": sim["description"],
        "expected_posture": sim["expected_posture"],
        "simulated_data": {
            "faa_status": sim.get("faa_status"),
            "metar": sim.get("metar"),
            "nws_alerts": sim.get("nws_alerts", []),
        },
        "sources_attempted": ["FAA_NAS", "METAR", "TAF", "NWS_ALERTS"],
        "sources_succeeded": ["FAA_NAS", "METAR", "TAF", "NWS_ALERTS"],
        "sources_failed": [],
        "errors": [],
        "note": "This is SIMULATED data for testing. Use POST /cases to create a case, then POST /cases/{id}/run to see posture.",
    }
