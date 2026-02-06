# simulation/generators/__init__.py
"""
Data generators that convert scenario data into ingestion-layer format.

These generators produce data in the exact format expected by the real
ingestion clients (FAANASStatusClient, AviationWeatherClient, etc.) so
the simulation can seamlessly replace real API calls.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from ..scenarios import Scenario


@dataclass
class SimulatedAirportStatus:
    """Matches app/ingestion/faa_nasstatus.py AirportStatus"""
    airport_code: str
    delay: bool
    delay_type: Optional[str] = None
    reason: Optional[str] = None
    avg_delay_minutes: Optional[int] = None
    closure: bool = False
    ground_stop: bool = False
    ground_delay_program: bool = False


@dataclass
class SimulatedMetarObservation:
    """Matches app/ingestion/aviationweather.py MetarObservation"""
    icao: str
    observation_time: datetime  # matches real MetarObservation
    raw_text: str  # matches real MetarObservation
    # Wind
    wind_direction: Optional[int] = None  # degrees
    wind_speed: Optional[int] = None  # knots (matches real)
    wind_gust: Optional[int] = None  # knots (matches real)
    # Visibility
    visibility_miles: Optional[float] = None
    # Ceiling
    ceiling_feet: Optional[int] = None
    ceiling_type: Optional[str] = None  # BKN, OVC, etc.
    # Weather phenomena
    weather: List[str] = None  # RA, SN, FG, etc. (matches real)
    # Flight category
    flight_category: Optional[str] = "VFR"  # VFR, MVFR, IFR, LIFR
    # Temperature
    temp_c: Optional[float] = None  # matches real
    dewpoint_c: Optional[float] = None
    # Pressure
    altimeter_inhg: Optional[float] = None
    # Metadata
    retrieved_at: datetime = None
    raw_data: Dict[str, Any] = None

    def __post_init__(self):
        if self.weather is None:
            self.weather = []
        if self.retrieved_at is None:
            self.retrieved_at = datetime.now(timezone.utc)
        if self.raw_data is None:
            self.raw_data = {}


@dataclass
class SimulatedTafForecast:
    """Matches app/ingestion/aviationweather.py TafForecast"""
    icao: str
    issue_time: datetime  # matches real TafForecast
    valid_from: datetime
    valid_to: datetime
    raw_text: str  # matches real TafForecast
    forecast_periods: List[Dict[str, Any]] = None  # matches real TafForecast
    retrieved_at: datetime = None
    raw_data: Dict[str, Any] = None

    def __post_init__(self):
        if self.forecast_periods is None:
            self.forecast_periods = []
        if self.retrieved_at is None:
            self.retrieved_at = datetime.now(timezone.utc)
        if self.raw_data is None:
            self.raw_data = {}


@dataclass
class SimulatedWeatherAlert:
    """Matches app/ingestion/nws_alerts.py WeatherAlert"""
    id: str
    event: str
    severity: str
    certainty: str
    urgency: str
    headline: str
    description: str
    instruction: str
    effective: datetime
    expires: datetime
    area: str


@dataclass
class SimulatedOpenSkyResponse:
    """Matches app/ingestion/opensky.py OpenSkyResponse"""
    time: int  # Unix timestamp (matches real OpenSkyResponse)
    aircraft_count: int
    retrieved_at: datetime = None
    raw_data: Dict[str, Any] = None
    states: List[Any] = None  # List of AircraftState-like objects
    # Extra fields for simulation metadata (not in real OpenSkyResponse)
    bounding_box: Dict[str, float] = None

    def __post_init__(self):
        if self.retrieved_at is None:
            self.retrieved_at = datetime.now(timezone.utc)
        if self.raw_data is None:
            self.raw_data = {}
        if self.states is None:
            self.states = []
        if self.bounding_box is None:
            self.bounding_box = {}


class FAASimulator:
    """Generates FAA NAS Status data from scenarios."""

    def generate(self, scenario: Scenario) -> Optional[SimulatedAirportStatus]:
        """
        Generate FAA status from scenario.

        Returns None if scenario has no FAA disruption (normal operations).
        """
        if scenario.faa_data is None:
            return None

        data = scenario.faa_data
        return SimulatedAirportStatus(
            airport_code=data.get("airport", scenario.airport_icao[1:]),
            delay=data.get("delay", False),
            delay_type=data.get("delay_type"),
            reason=data.get("reason"),
            avg_delay_minutes=data.get("avg_delay_minutes"),
            closure=data.get("closure", False),
            ground_stop=data.get("ground_stop", False),
            ground_delay_program=data.get("ground_delay_program", False),
        )

    def to_ingestion_result(self, scenario: Scenario) -> Dict[str, Any]:
        """Convert to IngestionResult format."""
        status = self.generate(scenario)
        return {
            "source": "FAA_NAS",
            "success": True,
            "data": status,
            "error": None,
            "retrieved_at": datetime.now(timezone.utc),
        }


class METARSimulator:
    """Generates METAR data from scenarios."""

    def generate(self, scenario: Scenario) -> Optional[SimulatedMetarObservation]:
        """Generate METAR observation from scenario."""
        if scenario.metar_data is None:
            return None

        data = scenario.metar_data
        now = datetime.now(timezone.utc)

        # Parse observation time - support both old and new key names
        obs_time = None
        for key in ["observation_time", "observed_at"]:
            if key in data:
                val = data[key]
                if isinstance(val, str):
                    obs_time = datetime.fromisoformat(val.replace("Z", "+00:00"))
                else:
                    obs_time = val
                break
        if obs_time is None:
            obs_time = now

        # Parse raw text - support both old and new key names
        raw_text = data.get("raw_text", data.get("raw", ""))

        # Parse wind - support both old and new key names
        wind_speed = data.get("wind_speed", data.get("wind_speed_kts"))
        wind_gust = data.get("wind_gust", data.get("wind_gust_kts"))

        # Parse weather - support both old and new key names
        weather = data.get("weather", data.get("conditions", []))

        # Parse temp - support both old and new key names
        temp_c = data.get("temp_c", data.get("temperature_c"))

        return SimulatedMetarObservation(
            icao=data.get("icao", scenario.airport_icao),
            observation_time=obs_time,
            raw_text=raw_text,
            wind_direction=data.get("wind_direction"),
            wind_speed=wind_speed,
            wind_gust=wind_gust,
            visibility_miles=data.get("visibility_miles"),
            ceiling_feet=data.get("ceiling_feet"),
            ceiling_type=data.get("ceiling_type"),
            weather=weather if weather else [],
            flight_category=data.get("flight_category", "VFR"),
            temp_c=temp_c,
            dewpoint_c=data.get("dewpoint_c"),
            altimeter_inhg=data.get("altimeter_inhg"),
            retrieved_at=now,
            raw_data=data,
        )

    def to_ingestion_result(self, scenario: Scenario) -> Dict[str, Any]:
        """Convert to IngestionResult format."""
        metar = self.generate(scenario)
        return {
            "source": "METAR",
            "success": metar is not None,
            "data": metar,
            "error": None if metar else "No METAR data available",
            "retrieved_at": datetime.now(timezone.utc),
        }


class TAFSimulator:
    """Generates TAF data from scenarios."""

    def generate(self, scenario: Scenario) -> Optional[SimulatedTafForecast]:
        """Generate TAF forecast from scenario."""
        if scenario.taf_data is None:
            return None

        data = scenario.taf_data
        now = datetime.now(timezone.utc)

        # Parse issue time - support both old and new key names
        issue_time = None
        for key in ["issue_time", "issued_at"]:
            if key in data:
                val = data[key]
                if isinstance(val, str):
                    issue_time = datetime.fromisoformat(val.replace("Z", "+00:00"))
                else:
                    issue_time = val
                break
        if issue_time is None:
            issue_time = now

        # Parse valid_from
        valid_from = None
        if "valid_from" in data:
            val = data["valid_from"]
            if isinstance(val, str):
                valid_from = datetime.fromisoformat(val.replace("Z", "+00:00"))
            else:
                valid_from = val
        if valid_from is None:
            valid_from = now

        # Parse valid_to
        valid_to = None
        if "valid_to" in data:
            val = data["valid_to"]
            if isinstance(val, str):
                valid_to = datetime.fromisoformat(val.replace("Z", "+00:00"))
            else:
                valid_to = val
        if valid_to is None:
            valid_to = now

        # Parse raw text - support both old and new key names
        raw_text = data.get("raw_text", data.get("raw", ""))

        # Parse forecast periods - support both old and new key names
        forecast_periods = data.get("forecast_periods", data.get("periods", []))

        return SimulatedTafForecast(
            icao=data.get("icao", scenario.airport_icao),
            issue_time=issue_time,
            valid_from=valid_from,
            valid_to=valid_to,
            raw_text=raw_text,
            forecast_periods=forecast_periods if forecast_periods else [],
            retrieved_at=now,
            raw_data=data,
        )

    def to_ingestion_result(self, scenario: Scenario) -> Dict[str, Any]:
        """Convert to IngestionResult format."""
        taf = self.generate(scenario)
        return {
            "source": "TAF",
            "success": taf is not None,
            "data": taf,
            "error": None if taf else "No TAF data available",
            "retrieved_at": datetime.now(timezone.utc),
        }


class NWSSimulator:
    """Generates NWS Alert data from scenarios."""

    def generate(self, scenario: Scenario) -> List[SimulatedWeatherAlert]:
        """Generate NWS alerts from scenario."""
        alerts = []
        for alert_data in scenario.nws_alerts:
            alerts.append(SimulatedWeatherAlert(
                id=alert_data.get("id", ""),
                event=alert_data.get("event", ""),
                severity=alert_data.get("severity", "Minor"),
                certainty=alert_data.get("certainty", "Possible"),
                urgency=alert_data.get("urgency", "Future"),
                headline=alert_data.get("headline", ""),
                description=alert_data.get("description", ""),
                instruction=alert_data.get("instruction", ""),
                effective=datetime.fromisoformat(alert_data["effective"].replace("Z", "+00:00"))
                    if isinstance(alert_data.get("effective"), str)
                    else alert_data.get("effective", datetime.now(timezone.utc)),
                expires=datetime.fromisoformat(alert_data["expires"].replace("Z", "+00:00"))
                    if isinstance(alert_data.get("expires"), str)
                    else alert_data.get("expires", datetime.now(timezone.utc)),
                area=alert_data.get("area", ""),
            ))
        return alerts

    def to_ingestion_result(self, scenario: Scenario) -> Dict[str, Any]:
        """Convert to IngestionResult format."""
        alerts = self.generate(scenario)
        return {
            "source": "NWS_ALERTS",
            "success": True,  # Empty list is still success
            "data": alerts,
            "error": None,
            "retrieved_at": datetime.now(timezone.utc),
        }


class OpenSkySimulator:
    """Generates OpenSky ADS-B data from scenarios."""

    def generate(self, scenario: Scenario) -> Optional[SimulatedOpenSkyResponse]:
        """Generate OpenSky response from scenario."""
        # Handle missing source scenario
        if scenario.has_missing_source and scenario.missing_source == "OPENSKY":
            return None

        if scenario.opensky_data is None:
            return None

        data = scenario.opensky_data
        now = datetime.now(timezone.utc)

        # Parse timestamp - convert to Unix timestamp (int) as required by real OpenSkyResponse
        timestamp_val = data.get("timestamp")
        if isinstance(timestamp_val, str):
            ts_dt = datetime.fromisoformat(timestamp_val.replace("Z", "+00:00"))
            time_unix = int(ts_dt.timestamp())
        elif isinstance(timestamp_val, datetime):
            time_unix = int(timestamp_val.timestamp())
        elif isinstance(timestamp_val, (int, float)):
            time_unix = int(timestamp_val)
        else:
            time_unix = int(now.timestamp())

        return SimulatedOpenSkyResponse(
            time=time_unix,  # Unix timestamp (matches real OpenSkyResponse)
            aircraft_count=data.get("aircraft_count", 0),
            retrieved_at=now,
            raw_data=data,
            states=data.get("states", []),
            bounding_box=data.get("bounding_box", {}),
        )

    def to_ingestion_result(self, scenario: Scenario) -> Dict[str, Any]:
        """Convert to IngestionResult format."""
        # Simulate timeout for missing source scenario
        if scenario.has_missing_source and scenario.missing_source == "OPENSKY":
            return {
                "source": "OPENSKY",
                "success": False,
                "data": None,
                "error": "Connection timeout after 10s",
                "retrieved_at": datetime.now(timezone.utc),
            }

        response = self.generate(scenario)
        return {
            "source": "OPENSKY",
            "success": response is not None,
            "data": response,
            "error": None if response else "No OpenSky data available",
            "retrieved_at": datetime.now(timezone.utc),
        }


class SimulationIngestionRegistry:
    """
    Simulation version of IngestionRegistry.

    Produces data from scenarios instead of hitting real APIs.
    Drop-in replacement for app/ingestion/registry.py IngestionRegistry.
    """

    def __init__(self, scenario: Scenario):
        self.scenario = scenario
        self.faa_sim = FAASimulator()
        self.metar_sim = METARSimulator()
        self.taf_sim = TAFSimulator()
        self.nws_sim = NWSSimulator()
        self.opensky_sim = OpenSkySimulator()

    def ingest_airport(
        self,
        icao: str,
        case_id: Optional[str] = None,
        include_opensky: bool = True,
    ) -> "SimulatedAirportIngestionResult":
        """
        Simulate ingestion for an airport using scenario data.

        Returns SimulatedAirportIngestionResult in same format as
        real AirportIngestionResult.
        """
        return SimulatedAirportIngestionResult(
            icao=icao,
            faa_status=self._make_ingestion_result("FAA_NAS", self.faa_sim.generate(self.scenario)),
            metar=self._make_ingestion_result("METAR", self.metar_sim.generate(self.scenario)),
            taf=self._make_ingestion_result("TAF", self.taf_sim.generate(self.scenario)),
            nws_alerts=self._make_ingestion_result("NWS_ALERTS", self.nws_sim.generate(self.scenario)),
            opensky=self._make_opensky_result() if include_opensky else None,
        )

    def _make_ingestion_result(
        self,
        source: str,
        data: Any,
    ) -> "SimulatedIngestionResult":
        """Create IngestionResult for a source."""
        return SimulatedIngestionResult(
            source=source,
            success=True,  # Data None still means "no disruption" which is success
            data=data,
            error=None,
            missing_evidence=None,
            retrieved_at=datetime.now(timezone.utc),
        )

    def _make_opensky_result(self) -> "SimulatedIngestionResult":
        """Create IngestionResult for OpenSky with potential timeout."""
        if self.scenario.has_missing_source and self.scenario.missing_source == "OPENSKY":
            return SimulatedIngestionResult(
                source="OPENSKY",
                success=False,
                data=None,
                error="Connection timeout after 10s",
                missing_evidence=SimulatedMissingEvidenceRequest(
                    source_system="OPENSKY",
                    request_type="aircraft_states",
                    reason="Connection timeout after 10s",
                    criticality="DEGRADED",
                ),
                retrieved_at=datetime.now(timezone.utc),
            )

        return SimulatedIngestionResult(
            source="OPENSKY",
            success=True,
            data=self.opensky_sim.generate(self.scenario),
            error=None,
            missing_evidence=None,
            retrieved_at=datetime.now(timezone.utc),
        )


@dataclass
class SimulatedIngestionResult:
    """Matches app/ingestion/registry.py IngestionResult"""
    source: str
    success: bool
    data: Any
    error: Optional[str] = None
    missing_evidence: Optional["SimulatedMissingEvidenceRequest"] = None
    retrieved_at: datetime = None

    def __post_init__(self):
        if self.retrieved_at is None:
            self.retrieved_at = datetime.now(timezone.utc)


@dataclass
class SimulatedMissingEvidenceRequest:
    """Matches MissingEvidenceRequest structure"""
    source_system: str
    request_type: str
    reason: str
    criticality: str = "DEGRADED"
    request_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.request_params is None:
            self.request_params = {}


@dataclass
class SimulatedAirportIngestionResult:
    """Matches app/ingestion/registry.py AirportIngestionResult"""
    icao: str
    faa_status: Optional[SimulatedIngestionResult] = None
    metar: Optional[SimulatedIngestionResult] = None
    taf: Optional[SimulatedIngestionResult] = None
    nws_alerts: Optional[SimulatedIngestionResult] = None
    opensky: Optional[SimulatedIngestionResult] = None
    ingested_at: datetime = None

    def __post_init__(self):
        if self.ingested_at is None:
            self.ingested_at = datetime.now(timezone.utc)

    @property
    def all_results(self) -> List[SimulatedIngestionResult]:
        """Get all non-None results."""
        results = [
            self.faa_status,
            self.metar,
            self.taf,
            self.nws_alerts,
            self.opensky,
        ]
        return [r for r in results if r is not None]

    @property
    def success_count(self) -> int:
        """Count of successful ingestions."""
        return sum(1 for r in self.all_results if r.success)

    @property
    def failure_count(self) -> int:
        """Count of failed ingestions."""
        return sum(1 for r in self.all_results if not r.success)

    @property
    def missing_evidence_requests(self) -> List[SimulatedMissingEvidenceRequest]:
        """Get all MissingEvidenceRequests from failed ingestions."""
        return [
            r.missing_evidence
            for r in self.all_results
            if r.missing_evidence is not None
        ]


# Export all generators
__all__ = [
    "FAASimulator",
    "METARSimulator",
    "TAFSimulator",
    "NWSSimulator",
    "OpenSkySimulator",
    "SimulationIngestionRegistry",
    "SimulatedAirportStatus",
    "SimulatedMetarObservation",
    "SimulatedTafForecast",
    "SimulatedWeatherAlert",
    "SimulatedOpenSkyResponse",
    "SimulatedIngestionResult",
    "SimulatedAirportIngestionResult",
    "SimulatedMissingEvidenceRequest",
]
