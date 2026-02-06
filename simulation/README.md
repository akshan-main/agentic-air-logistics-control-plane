# Simulation Module

Realistic simulation of aviation disruption scenarios for testing the Agentic Air Logistics Control Plane.

## Overview

This module provides:
1. **Pre-built scenarios** - Real-world disruption patterns (ground stops, weather, hurricanes)
2. **Data generators** - Convert scenarios to ingestion-layer format
3. **Simulation runner** - Execute scenarios through the full multi-agent pipeline
4. **API endpoints** - Web interface for running simulations

## Scenarios

Each scenario represents a realistic aviation disruption with coordinated data across all sources.

### Available Scenarios

| ID | Airport | Expected Posture | Description |
|----|---------|------------------|-------------|
| `jfk_ground_stop` | KJFK | HOLD | Winter storm causing ground stop |
| `ord_thunderstorms` | KORD | RESTRICT | Summer convection with GDP |
| `mia_hurricane` | KMIA | ESCALATE | Category 2 hurricane approach |
| `lax_normal` | KLAX | ACCEPT | Clear day, normal operations |
| `den_fog` | KDEN | RESTRICT | Dense morning fog |
| `atl_runway_closure` | KATL | RESTRICT | Equipment failure, capacity reduced |
| `sfo_marine_layer` | KSFO | RESTRICT | Low ceilings from marine layer |
| `sea_contradiction` | KSEA | RESTRICT | FAA vs Weather data mismatch |
| `dfw_opensky_timeout` | KDFW | RESTRICT | OpenSky API timeout (degraded mode) |
| `bos_post_storm` | KBOS | ACCEPT | Post-storm recovery |

### Scenario Categories

- **Normal Operations**: LAX, BOS post-storm
- **RESTRICT**: Weather delays, GDP, equipment issues
- **HOLD**: Ground stops, severe weather
- **ESCALATE**: Hurricanes, critical events
- **Contradiction**: Conflicting source data
- **Degraded**: Missing source data (API failures)

## Usage

### Python API

```python
from simulation import SimulationRunner, get_scenario

# Run a single scenario
with SimulationRunner() as runner:
    result = runner.run_scenario("jfk_ground_stop")
    print(f"Expected: {result.expected_posture}")
    print(f"Actual: {result.actual_posture}")
    print(f"Passed: {result.passed}")

# Run all scenarios
with SimulationRunner() as runner:
    batch = runner.run_all_scenarios()
    print(f"Pass rate: {batch.pass_rate * 100:.1f}%")
```

### REST API

```bash
# List all scenarios
curl http://localhost:8000/simulation/scenarios

# Get scenario details
curl http://localhost:8000/simulation/scenarios/jfk_ground_stop

# Run a scenario
curl -X POST http://localhost:8000/simulation/run/jfk_ground_stop

# Run with streaming
curl http://localhost:8000/simulation/run/jfk_ground_stop/stream

# Run all scenarios
curl http://localhost:8000/simulation/run-all

# Validate scenario definitions
curl http://localhost:8000/simulation/validate
```

## Data Sources

Each scenario includes realistic data for:

### FAA NAS Status
- Ground stops, GDP, departure delays
- Reason codes (weather, equipment, volume)
- Expected end times

### METAR (Current Weather)
- Wind speed/direction/gusts
- Visibility (statute miles)
- Ceiling (feet AGL)
- Weather phenomena (snow, thunderstorms, fog)
- Flight category (VFR, MVFR, IFR, LIFR)

### TAF (Forecast)
- 24-hour forecast periods
- Expected improvement times
- Trend indicators

### NWS Alerts
- Winter Storm Warnings
- Severe Thunderstorm Warnings
- Hurricane Warnings
- Dense Fog Advisories
- Wind Advisories

### OpenSky ADS-B
- Aircraft count in terminal area
- Baseline comparison (% change)
- Movement collapse detection

## Scenario Data Structure

```python
@dataclass
class Scenario:
    id: str                           # Unique identifier
    name: str                         # Human-readable name
    description: str                  # Detailed description
    airport_icao: str                 # ICAO code (e.g., "KJFK")
    expected_posture: ExpectedPosture # ACCEPT, RESTRICT, HOLD, ESCALATE
    expected_risk_level: str          # LOW, MEDIUM, HIGH, CRITICAL

    # Source data (matches real API formats)
    faa_data: Optional[Dict]
    metar_data: Optional[Dict]
    taf_data: Optional[Dict]
    nws_alerts: List[Dict]
    opensky_data: Optional[Dict]

    # Special flags
    has_contradiction: bool           # Conflicting source data
    has_missing_source: bool          # API failure simulation
    missing_source: Optional[str]     # Which source failed
```

## Adding New Scenarios

1. Create scenario in `scenarios/__init__.py`:

```python
NEW_SCENARIO = Scenario(
    id="new_scenario",
    name="New Scenario Description",
    description="...",
    airport_icao="KXXX",
    expected_posture=ExpectedPosture.RESTRICT,
    expected_risk_level="MEDIUM",

    faa_data={...},
    metar_data={...},
    taf_data={...},
    nws_alerts=[...],
    opensky_data={...},
)
```

2. Add to SCENARIOS dict:

```python
SCENARIOS["new_scenario"] = NEW_SCENARIO
```

3. Run validation:

```bash
curl http://localhost:8000/simulation/validate
```

## Integration with Orchestrator

The simulation runner patches the InvestigatorAgent to use `SimulationIngestionRegistry`
instead of the real `IngestionRegistry`. This allows:

- Full multi-agent orchestration (Investigator → RiskQuant → Critic → PolicyJudge → Executor)
- Real database writes (cases, evidence, claims, trace events)
- Actual LLM calls for risk assessment
- Authentic decision packets

The only difference from production: data comes from scenarios instead of real APIs.

## Validation Testing

Run full validation suite:

```bash
# Quick test
python -c "from simulation.runner import run_all_tests; print(run_all_tests())"

# Or via API
curl http://localhost:8000/simulation/run-all
```

Expected output:
```json
{
  "total": 10,
  "passed": 10,
  "failed": 0,
  "pass_rate": "100.0%",
  "results": [...]
}
```

## Metrics Tracked

Each simulation result includes:
- **PDL (Posture Decision Latency)**: Time from first signal to posture directive
- **Evidence count**: Number of evidence records created
- **Claim count**: Number of claims generated
- **Contradiction count**: Number of contradictions detected
- **Duration**: Total execution time

## Best Practices

1. **Run scenarios after code changes** to catch regressions
2. **Add new scenarios** for edge cases you encounter
3. **Use streaming** for debugging to see state transitions
4. **Check contradiction scenarios** work correctly
5. **Verify degraded mode** handles missing sources gracefully
