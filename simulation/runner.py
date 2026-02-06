# simulation/runner.py
"""
Simulation runner that executes scenarios through the full multi-agent pipeline.

This runner integrates with the real orchestrator but uses simulated data
instead of real API calls. It validates that the agent produces the expected
posture for each scenario.
"""

import json
import signal
from typing import Dict, Any, Optional, List, Generator
from uuid import UUID, uuid4
from datetime import datetime, timezone
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

from sqlalchemy import text
from sqlalchemy.orm import Session

from .scenarios import Scenario, SCENARIOS, get_scenario, ExpectedPosture
from .generators import SimulationIngestionRegistry
from .graph_seeder import GraphSeeder

# Global timeout for entire simulation (in seconds)
SIMULATION_TIMEOUT = 90  # 90 seconds max per scenario


@dataclass
class SimulationResult:
    """Result of running a simulation scenario."""
    scenario_id: str
    scenario_name: str
    airport_icao: str

    # Expected vs Actual
    expected_posture: str
    actual_posture: Optional[str]
    expected_risk_level: str
    actual_risk_level: Optional[str]

    # Pass/Fail
    posture_match: bool
    risk_match: bool

    # Metrics
    pdl_seconds: float
    evidence_count: int
    claim_count: int
    uncertainty_count: int
    contradiction_count: int

    # Execution details
    case_id: Optional[UUID] = None
    duration_seconds: float = 0.0
    error: Optional[str] = None

    # Full packet for inspection
    decision_packet: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        self.passed = self.posture_match and self.error is None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "scenario_name": self.scenario_name,
            "airport_icao": self.airport_icao,
            "expected_posture": self.expected_posture,
            "actual_posture": self.actual_posture,
            "expected_risk_level": self.expected_risk_level,
            "actual_risk_level": self.actual_risk_level,
            "posture_match": self.posture_match,
            "risk_match": self.risk_match,
            "passed": self.passed,
            "pdl_seconds": self.pdl_seconds,
            "evidence_count": self.evidence_count,
            "claim_count": self.claim_count,
            "uncertainty_count": self.uncertainty_count,
            "contradiction_count": self.contradiction_count,
            "case_id": str(self.case_id) if self.case_id else None,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
        }


@dataclass
class SimulationBatchResult:
    """Result of running multiple scenarios."""
    total: int
    passed: int
    failed: int
    results: List[SimulationResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0.0

    @property
    def pass_rate(self) -> float:
        return self.passed / self.total if self.total > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "pass_rate": f"{self.pass_rate * 100:.1f}%",
            "duration_seconds": self.duration_seconds,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "results": [r.to_dict() for r in self.results],
        }


class SimulationRunner:
    """
    Runs simulation scenarios through the full multi-agent pipeline.

    This runner:
    1. Creates a case in the database
    2. Injects simulated data (instead of hitting real APIs)
    3. Runs the orchestrator
    4. Compares actual vs expected posture
    5. Reports results
    """

    def __init__(self, session: Optional[Session] = None):
        """
        Initialize simulation runner.

        Args:
            session: SQLAlchemy session. If None, creates one from engine.
        """
        if session is None:
            from app.db.engine import SessionLocal
            self._owns_session = True
            self.session = SessionLocal()
        else:
            self._owns_session = False
            self.session = session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._owns_session:
            self.session.close()

    def run_scenario(
        self,
        scenario_id: str,
        use_streaming: bool = False,
    ) -> SimulationResult:
        """
        Run a single scenario through the pipeline.

        Args:
            scenario_id: ID of scenario to run (e.g., "jfk_ground_stop")
            use_streaming: Whether to use streaming orchestrator

        Returns:
            SimulationResult with pass/fail and metrics
        """
        scenario = get_scenario(scenario_id)
        if scenario is None:
            return SimulationResult(
                scenario_id=scenario_id,
                scenario_name="Unknown",
                airport_icao="UNKNOWN",
                expected_posture="UNKNOWN",
                actual_posture=None,
                expected_risk_level="UNKNOWN",
                actual_risk_level=None,
                posture_match=False,
                risk_match=False,
                pdl_seconds=0,
                evidence_count=0,
                claim_count=0,
                uncertainty_count=0,
                contradiction_count=0,
                error=f"Scenario not found: {scenario_id}",
            )

        return self._run_scenario_impl(scenario, use_streaming)

    def run_all_scenarios(self) -> SimulationBatchResult:
        """Run all available scenarios and return batch results."""
        batch = SimulationBatchResult(
            total=len(SCENARIOS),
            passed=0,
            failed=0,
        )

        for scenario_id in SCENARIOS:
            result = self.run_scenario(scenario_id)
            batch.results.append(result)
            if result.passed:
                batch.passed += 1
            else:
                batch.failed += 1

        batch.completed_at = datetime.now(timezone.utc)
        batch.duration_seconds = (
            batch.completed_at - batch.started_at
        ).total_seconds()

        return batch

    def run_scenario_streaming(
        self,
        scenario_id: str,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Run scenario with streaming progress events.

        Yields progress events as the orchestrator runs.
        """
        scenario = get_scenario(scenario_id)
        if scenario is None:
            yield {
                "event": "error",
                "error": f"Scenario not found: {scenario_id}",
            }
            return

        yield {
            "event": "started",
            "scenario": scenario.to_dict(),
        }

        try:
            # Create case
            case_id = self._create_case(scenario)
            yield {
                "event": "case_created",
                "case_id": str(case_id),
            }

            # Create simulation registry
            sim_registry = SimulationIngestionRegistry(scenario)

            # Import and patch the investigator to use simulation registry
            from app.agents.orchestrator import Orchestrator

            orchestrator = Orchestrator(case_id, self.session)

            # Patch the investigator's registry with simulation registry
            # This is the key integration point
            self._patch_orchestrator_for_simulation(orchestrator, sim_registry)

            # Run with streaming (using run_with_progress - the actual method name)
            for progress in orchestrator.run_with_progress():
                yield progress

            # Get final packet
            packet = orchestrator.last_packet

            # Extract results
            actual_posture = packet.get("posture_decision", {}).get("posture")
            metrics = packet.get("metrics", {})

            yield {
                "event": "completed",
                "expected_posture": scenario.expected_posture.value,
                "actual_posture": actual_posture,
                "posture_match": actual_posture == scenario.expected_posture.value,
                "packet": packet,
            }

        except Exception as e:
            yield {
                "event": "error",
                "error": str(e),
            }

    def _run_scenario_impl(
        self,
        scenario: Scenario,
        use_streaming: bool,
    ) -> SimulationResult:
        """Implementation of scenario execution with global timeout."""
        start_time = datetime.now(timezone.utc)

        def _run_with_orchestrator():
            """Inner function to run in thread with timeout."""
            # Create case
            case_id = self._create_case(scenario)

            # Seed enterprise data (flights, shipments, bookings)
            # This enables cascade analysis: airport -> flights -> shipments
            self._seed_enterprise_data(scenario.airport_icao)

            # Create simulation registry
            sim_registry = SimulationIngestionRegistry(scenario)

            # Import and create orchestrator
            from app.agents.orchestrator import Orchestrator

            orchestrator = Orchestrator(case_id, self.session)

            # Patch for simulation
            self._patch_orchestrator_for_simulation(orchestrator, sim_registry)

            # Limit orchestrator iterations to prevent infinite loops
            orchestrator.belief_state.max_iterations = 10

            # Run orchestrator
            if use_streaming:
                # Consume all streaming events to get final packet
                for _ in orchestrator.run_with_progress():
                    pass
                packet = orchestrator.last_packet
            else:
                packet = orchestrator.run()

            return case_id, packet

        try:
            # Run with timeout using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_run_with_orchestrator)
                try:
                    case_id, packet = future.result(timeout=SIMULATION_TIMEOUT)
                except FuturesTimeoutError:
                    # Timeout! Return error result
                    end_time = datetime.now(timezone.utc)
                    return SimulationResult(
                        scenario_id=scenario.id,
                        scenario_name=scenario.name,
                        airport_icao=scenario.airport_icao,
                        expected_posture=scenario.expected_posture.value,
                        actual_posture=None,
                        expected_risk_level=scenario.expected_risk_level,
                        actual_risk_level=None,
                        posture_match=False,
                        risk_match=False,
                        pdl_seconds=0,
                        evidence_count=0,
                        claim_count=0,
                        uncertainty_count=0,
                        contradiction_count=0,
                        duration_seconds=(end_time - start_time).total_seconds(),
                        error=f"Simulation timed out after {SIMULATION_TIMEOUT} seconds",
                    )

            # Calculate duration
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            # Extract results
            posture_decision = packet.get("posture_decision", {})
            actual_posture = posture_decision.get("posture")
            metrics = packet.get("metrics", {})

            # Get risk level from confidence breakdown or claims
            actual_risk_level = self._extract_risk_level(packet)

            return SimulationResult(
                scenario_id=scenario.id,
                scenario_name=scenario.name,
                airport_icao=scenario.airport_icao,
                expected_posture=scenario.expected_posture.value,
                actual_posture=actual_posture,
                expected_risk_level=scenario.expected_risk_level,
                actual_risk_level=actual_risk_level,
                posture_match=actual_posture == scenario.expected_posture.value,
                risk_match=actual_risk_level == scenario.expected_risk_level,
                pdl_seconds=metrics.get("pdl_seconds", 0),
                evidence_count=metrics.get("evidence_count", 0),
                claim_count=len(packet.get("top_claims", [])),
                uncertainty_count=0,  # Should be 0 after resolution
                contradiction_count=len(packet.get("contradictions", [])),
                case_id=case_id,
                duration_seconds=duration,
                decision_packet=packet,
            )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            return SimulationResult(
                scenario_id=scenario.id,
                scenario_name=scenario.name,
                airport_icao=scenario.airport_icao,
                expected_posture=scenario.expected_posture.value,
                actual_posture=None,
                expected_risk_level=scenario.expected_risk_level,
                actual_risk_level=None,
                posture_match=False,
                risk_match=False,
                pdl_seconds=0,
                evidence_count=0,
                claim_count=0,
                uncertainty_count=0,
                contradiction_count=0,
                duration_seconds=(end_time - start_time).total_seconds(),
                error=str(e),
            )

    def _create_case(self, scenario: Scenario) -> UUID:
        """Create a case in the database for the scenario."""
        case_id = uuid4()

        self.session.execute(
            text("""
                INSERT INTO "case" (id, case_type, scope, created_at, status)
                VALUES (:id, :case_type, CAST(:scope AS jsonb), :created_at, :status)
            """),
            {
                "id": case_id,
                "case_type": "AIRPORT_DISRUPTION",
                "scope": json.dumps({
                    "airport": scenario.airport_icao,
                    "simulation": True,
                    "scenario_id": scenario.id,
                }),
                "created_at": datetime.now(timezone.utc),
                "status": "OPEN",
            }
        )
        self.session.commit()

        return case_id

    def _seed_enterprise_data(self, airport_icao: str) -> None:
        """
        Seed the graph with simulated enterprise data for an airport.

        Creates:
        - CARRIER nodes (airlines)
        - FLIGHT nodes (connecting airports)
        - SHIPMENT nodes (cargo on flights)
        - BOOKING nodes (evidence for shipment actions)
        - DOCUMENT nodes (AWB, customs, invoices)

        This enables cascade analysis:
        Airport disruption -> affected flights -> affected shipments -> SLA exposure
        """
        from app.graph.store import GraphStore

        # Check if already seeded for this airport
        result = self.session.execute(
            text("""
                SELECT COUNT(*) FROM edge
                WHERE type = 'FLIGHT_DEPARTS_FROM'
                AND EXISTS (
                    SELECT 1 FROM node n
                    WHERE n.id = edge.dst
                    AND n.type = 'AIRPORT'
                    AND n.identifier = :icao
                )
            """),
            {"icao": airport_icao}
        )
        existing_count = result.scalar()

        if existing_count > 0:
            # Already seeded, skip
            return

        # Seed enterprise data
        graph_store = GraphStore(self.session)
        seeder = GraphSeeder(graph_store)
        seeder.seed_airport(
            airport_icao=airport_icao,
            num_flights=20,
            shipments_per_flight=5
        )

    def _patch_orchestrator_for_simulation(
        self,
        orchestrator: "Orchestrator",
        sim_registry: SimulationIngestionRegistry,
    ):
        """
        Patch the orchestrator to use simulation data.

        This patches the investigator's registry to use simulation data
        instead of hitting real APIs.
        """
        # Store original registry getter
        original_registry = None

        def patched_start_investigation():
            """Patched version that uses simulation registry."""
            from app.agents.roles.investigator import InvestigatorAgent

            investigator = InvestigatorAgent(
                case_id=orchestrator.case_id,
                session=orchestrator.session,
                skip_cache=True,  # IMPORTANT: Skip evidence cache for simulation
            )
            # Replace registry with simulation registry
            investigator.registry = sim_registry
            investigator.investigate(orchestrator.belief_state)

        # Patch the start_investigation handler
        orchestrator.start_investigation = patched_start_investigation

    def _extract_risk_level(self, packet: Dict[str, Any]) -> Optional[str]:
        """Extract risk level from decision packet."""
        # Try to get from claims
        for claim in packet.get("top_claims", []):
            text = claim.get("text", "")
            if "Risk level is" in text:
                # Extract risk level from "Risk level is HIGH..."
                for level in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
                    if level in text:
                        return level

        # Try to get from confidence breakdown
        breakdown = packet.get("confidence_breakdown", {})
        if breakdown:
            # Infer from confidence
            confidence = breakdown.get("final", 0.5)
            if confidence >= 0.8:
                return "LOW"
            elif confidence >= 0.6:
                return "MEDIUM"
            else:
                return "HIGH"

        return None


def run_quick_test(scenario_id: str = "lax_normal") -> Dict[str, Any]:
    """
    Quick test function to run a single scenario.

    Usage:
        from simulation.runner import run_quick_test
        result = run_quick_test("jfk_ground_stop")
        print(result)
    """
    with SimulationRunner() as runner:
        result = runner.run_scenario(scenario_id)
        return result.to_dict()


def run_all_tests() -> Dict[str, Any]:
    """
    Run all scenarios and return summary.

    Usage:
        from simulation.runner import run_all_tests
        results = run_all_tests()
        print(f"Pass rate: {results['pass_rate']}")
    """
    with SimulationRunner() as runner:
        batch = runner.run_all_scenarios()
        return batch.to_dict()
