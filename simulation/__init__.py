# simulation/__init__.py
"""
Simulation module for Agentic Air Logistics Control Plane.

Two types of simulation:

1. DISRUPTION SCENARIOS (scenarios/, generators/)
   - Pre-built test cases with controlled FAA/weather data
   - For testing posture decision logic
   - Does NOT replace real APIs - supplements them for testing

2. OPERATIONAL DATA (operational_data.py, graph_seeder.py)
   - Simulates flights, shipments, bookings in the context graph
   - Enables cascade analysis: airport disruption → affected shipments
   - Required for testing shipment-level actions (HOLD_CARGO, REBOOK_FLIGHT)
   - This is what makes the graph useful beyond just storing airports

Usage:
    # Run disruption scenario
    from simulation import SimulationRunner
    runner = SimulationRunner()
    result = runner.run_scenario("jfk_ground_stop")

    # Seed operational data for cascade testing
    from simulation import seed_graph_for_airport
    result = seed_graph_for_airport("KJFK", num_flights=20)
"""

from .runner import SimulationRunner
from .scenarios import SCENARIOS, Scenario, get_scenario
from .generators import (
    FAASimulator,
    METARSimulator,
    TAFSimulator,
    NWSSimulator,
    OpenSkySimulator,
)
from .operational_data import (
    OperationalDataGenerator,
    SimulatedCarrier,
    SimulatedFlight,
    SimulatedShipment,
    SimulatedBooking,
    SimulatedDocument,
)
from .graph_seeder import GraphSeeder, seed_graph_for_airport

__all__ = [
    # Disruption scenario testing
    "SimulationRunner",
    "SCENARIOS",
    "Scenario",
    "get_scenario",
    "FAASimulator",
    "METARSimulator",
    "TAFSimulator",
    "NWSSimulator",
    "OpenSkySimulator",
    # Operational data simulation
    "OperationalDataGenerator",
    "SimulatedCarrier",
    "SimulatedFlight",
    "SimulatedShipment",
    "SimulatedBooking",
    "SimulatedDocument",
    "GraphSeeder",
    "seed_graph_for_airport",
]
