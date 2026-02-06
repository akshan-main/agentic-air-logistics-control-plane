# simulation/scenarios/__init__.py
"""
Pre-built disruption scenarios for simulation.

Each scenario represents a realistic aviation disruption event with
coordinated data across all sources (FAA, METAR, TAF, NWS, OpenSky).

Scenarios are designed to produce specific posture outcomes:
- ACCEPT: Normal operations, no disruptions
- RESTRICT: Elevated risk, limit premium SLAs
- HOLD: Significant disruption, pause new bookings
- ESCALATE: Critical situation, human decision required
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone, timedelta
from enum import Enum


class ExpectedPosture(Enum):
    """Expected posture outcome for a scenario."""
    ACCEPT = "ACCEPT"
    RESTRICT = "RESTRICT"
    HOLD = "HOLD"
    ESCALATE = "ESCALATE"


@dataclass
class Scenario:
    """
    A complete simulation scenario.

    Contains coordinated data for all sources that tells a coherent story
    about an aviation disruption event.
    """
    id: str
    name: str
    description: str
    airport_icao: str
    expected_posture: ExpectedPosture
    expected_risk_level: str  # LOW, MEDIUM, HIGH, CRITICAL

    # Source data
    faa_data: Optional[Dict[str, Any]] = None
    metar_data: Optional[Dict[str, Any]] = None
    taf_data: Optional[Dict[str, Any]] = None
    nws_alerts: List[Dict[str, Any]] = field(default_factory=list)
    opensky_data: Optional[Dict[str, Any]] = None

    # Scenario flags
    has_contradiction: bool = False
    has_missing_source: bool = False
    missing_source: Optional[str] = None

    # Timing (for bi-temporal testing)
    event_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "airport_icao": self.airport_icao,
            "expected_posture": self.expected_posture.value,
            "expected_risk_level": self.expected_risk_level,
            "has_contradiction": self.has_contradiction,
            "has_missing_source": self.has_missing_source,
            "missing_source": self.missing_source,
        }


# =============================================================================
# SCENARIO: JFK Ground Stop (Winter Storm)
# =============================================================================
# Real-world inspiration: JFK frequently experiences ground stops during
# nor'easters. FAA issues GDP (Ground Delay Program) or ground stop.
# METAR shows snow/freezing rain, low visibility. NWS has winter storm warning.

JFK_GROUND_STOP = Scenario(
    id="jfk_ground_stop",
    name="JFK Ground Stop - Winter Storm",
    description="""
    Scenario: Major winter storm hitting the Northeast. FAA has issued a ground
    stop at JFK due to snow and ice. METAR shows heavy snow, 1/4 mile visibility.
    TAF predicts conditions improving in 6 hours. NWS has Winter Storm Warning.
    OpenSky shows 80% reduction in aircraft movements.

    Expected Outcome: HOLD posture - significant disruption, pause bookings
    until conditions improve.
    """,
    airport_icao="KJFK",
    expected_posture=ExpectedPosture.HOLD,
    expected_risk_level="HIGH",

    faa_data={
        "airport": "JFK",
        "icao": "KJFK",
        "delay": True,
        "delay_type": "Ground Stop",
        "reason": "snow/ice",
        "reason_detail": "Heavy snow and ice conditions",
        "avg_delay_minutes": None,  # Ground stop = no avg delay
        "closure": False,
        "ground_stop": True,
        "ground_delay_program": False,
        "issued_at": "2026-02-04T14:00:00Z",
        "expected_end": "2026-02-04T20:00:00Z",
    },

    metar_data={
        "icao": "KJFK",
        "observed_at": "2026-02-04T15:56:00Z",
        "raw": "KJFK 041556Z 02018G28KT 1/4SM +SN FZFG VV002 M08/M10 A2965 RMK AO2 SLP045 P0025 T10831100",
        "wind_speed_kts": 18,
        "wind_gust_kts": 28,
        "wind_direction": 20,
        "visibility_miles": 0.25,
        "ceiling_feet": 200,  # Vertical visibility
        "temperature_c": -8,
        "dewpoint_c": -10,
        "altimeter_inhg": 29.65,
        "conditions": ["heavy snow", "freezing fog"],
        "flight_category": "LIFR",  # Low IFR
        "weather_phenomena": ["+SN", "FZFG"],
    },

    taf_data={
        "icao": "KJFK",
        "issued_at": "2026-02-04T12:00:00Z",
        "valid_from": "2026-02-04T12:00:00Z",
        "valid_to": "2026-02-05T18:00:00Z",
        "raw": "KJFK 041200Z 0412/0518 02015G25KT 1/4SM +SN VV002 TEMPO 0412/0418 1/8SM +SN FZFG FM041800 36012KT 2SM -SN BKN015 FM050000 36008KT P6SM SCT025",
        "periods": [
            {
                "from": "2026-02-04T12:00:00Z",
                "to": "2026-02-04T18:00:00Z",
                "wind_speed_kts": 15,
                "wind_gust_kts": 25,
                "visibility_miles": 0.25,
                "ceiling_feet": 200,
                "conditions": ["heavy snow"],
                "flight_category": "LIFR",
            },
            {
                "from": "2026-02-04T18:00:00Z",
                "to": "2026-02-05T00:00:00Z",
                "wind_speed_kts": 12,
                "visibility_miles": 2,
                "ceiling_feet": 1500,
                "conditions": ["light snow"],
                "flight_category": "IFR",
            },
            {
                "from": "2026-02-05T00:00:00Z",
                "to": "2026-02-05T18:00:00Z",
                "wind_speed_kts": 8,
                "visibility_miles": 6,
                "ceiling_feet": 2500,
                "conditions": [],
                "flight_category": "VFR",
            },
        ],
    },

    nws_alerts=[
        {
            "id": "urn:oid:2.49.0.1.840.0.2026.2.4.1234",
            "event": "Winter Storm Warning",
            "severity": "Severe",
            "certainty": "Likely",
            "urgency": "Expected",
            "headline": "Winter Storm Warning in effect until 6 PM EST Wednesday",
            "description": "Heavy snow expected. Total snow accumulations of 8 to 12 inches. Winds gusting as high as 35 mph.",
            "instruction": "Travel should be restricted to emergencies only.",
            "effective": "2026-02-04T06:00:00Z",
            "expires": "2026-02-04T23:00:00Z",
            "area": "New York City, Long Island, Northeast New Jersey",
        }
    ],

    opensky_data={
        "aircraft_count": 12,  # Normally ~60 at JFK
        "aircraft_count_baseline": 60,
        "delta_percent": -80,  # 80% reduction
        "bounding_box": {"min_lat": 40.5, "max_lat": 40.8, "min_lon": -74.0, "max_lon": -73.5},
        "timestamp": "2026-02-04T15:55:00Z",
        "states": [
            {"icao24": "a1b2c3", "callsign": "DAL123", "on_ground": True, "altitude": 0},
            {"icao24": "d4e5f6", "callsign": "UAL456", "on_ground": True, "altitude": 0},
            # Most aircraft are on ground during ground stop
        ],
    },
)


# =============================================================================
# SCENARIO: ORD Thunderstorms (Summer Convection)
# =============================================================================
# Real-world inspiration: Chicago O'Hare frequently experiences ground delays
# during summer thunderstorm season. Fast-moving cells can cause rapid changes.

ORD_THUNDERSTORMS = Scenario(
    id="ord_thunderstorms",
    name="ORD Thunderstorms - Summer Convection",
    description="""
    Scenario: Line of severe thunderstorms moving through Chicago area.
    FAA has issued Ground Delay Program with 90 minute average delays.
    METAR shows thunderstorms, 3 mile visibility. TAF shows storms clearing
    in 2-3 hours. NWS has Severe Thunderstorm Warning.

    Expected Outcome: RESTRICT posture - elevated risk, limit premium SLAs
    while monitoring for improvement.
    """,
    airport_icao="KORD",
    expected_posture=ExpectedPosture.RESTRICT,
    expected_risk_level="MEDIUM",

    faa_data={
        "airport": "ORD",
        "icao": "KORD",
        "delay": True,
        "delay_type": "Ground Delay Program",
        "reason": "thunderstorms",
        "reason_detail": "Severe thunderstorms in terminal area",
        "avg_delay_minutes": 90,
        "closure": False,
        "ground_stop": False,
        "ground_delay_program": True,
        "issued_at": "2026-02-04T18:00:00Z",
        "expected_end": "2026-02-04T21:00:00Z",
    },

    metar_data={
        "icao": "KORD",
        "observed_at": "2026-02-04T19:56:00Z",
        "raw": "KORD 041956Z 27015G30KT 3SM +TSRA BKN025CB OVC040 24/22 A2985 RMK AO2 TSB45 SLP105 FRQ LTGICCC OHD TS OHD MOV E P0035",
        "wind_speed_kts": 15,
        "wind_gust_kts": 30,
        "wind_direction": 270,
        "visibility_miles": 3,
        "ceiling_feet": 2500,
        "temperature_c": 24,
        "dewpoint_c": 22,
        "altimeter_inhg": 29.85,
        "conditions": ["thunderstorms", "heavy rain"],
        "flight_category": "MVFR",
        "weather_phenomena": ["+TSRA"],
        "remarks": "Frequent lightning in clouds, cloud-to-cloud, cloud-to-ground overhead",
    },

    taf_data={
        "icao": "KORD",
        "issued_at": "2026-02-04T18:00:00Z",
        "valid_from": "2026-02-04T18:00:00Z",
        "valid_to": "2026-02-05T18:00:00Z",
        "raw": "KORD 041800Z 0418/0518 27012G25KT 3SM TSRA BKN030CB FM042100 30010KT P6SM SCT040 FM050600 VRB05KT P6SM SKC",
        "periods": [
            {
                "from": "2026-02-04T18:00:00Z",
                "to": "2026-02-04T21:00:00Z",
                "wind_speed_kts": 12,
                "wind_gust_kts": 25,
                "visibility_miles": 3,
                "ceiling_feet": 3000,
                "conditions": ["thunderstorms", "rain"],
                "flight_category": "MVFR",
            },
            {
                "from": "2026-02-04T21:00:00Z",
                "to": "2026-02-05T06:00:00Z",
                "wind_speed_kts": 10,
                "visibility_miles": 6,
                "ceiling_feet": 4000,
                "conditions": [],
                "flight_category": "VFR",
            },
        ],
    },

    nws_alerts=[
        {
            "id": "urn:oid:2.49.0.1.840.0.2026.2.4.5678",
            "event": "Severe Thunderstorm Warning",
            "severity": "Severe",
            "certainty": "Observed",
            "urgency": "Immediate",
            "headline": "Severe Thunderstorm Warning in effect until 8 PM CDT",
            "description": "At 656 PM CDT, a severe thunderstorm was located over O'Hare Airport, moving east at 30 mph. 60 mph wind gusts and quarter size hail.",
            "instruction": "Move to an interior room on the lowest floor of a building.",
            "effective": "2026-02-04T18:56:00Z",
            "expires": "2026-02-04T20:00:00Z",
            "area": "Cook County, DuPage County",
        }
    ],

    opensky_data={
        "aircraft_count": 45,  # Normally ~90 at ORD
        "aircraft_count_baseline": 90,
        "delta_percent": -50,  # 50% reduction
        "bounding_box": {"min_lat": 41.8, "max_lat": 42.1, "min_lon": -88.0, "max_lon": -87.6},
        "timestamp": "2026-02-04T19:55:00Z",
    },
)


# =============================================================================
# SCENARIO: MIA Hurricane Approach
# =============================================================================
# Real-world inspiration: Miami often faces hurricane threats. Airports close
# 12-24 hours before landfall. Full ground stop, flights diverted.

MIA_HURRICANE = Scenario(
    id="mia_hurricane",
    name="MIA Hurricane Approach",
    description="""
    Scenario: Category 2 hurricane approaching South Florida. FAA has issued
    full ground stop at MIA with potential closure. Outer bands bringing
    tropical storm force winds. NWS Hurricane Warning in effect.
    All commercial operations suspended.

    Expected Outcome: ESCALATE posture - critical situation requiring
    human duty manager decision on operational recovery timeline.
    """,
    airport_icao="KMIA",
    expected_posture=ExpectedPosture.ESCALATE,
    expected_risk_level="CRITICAL",

    faa_data={
        "airport": "MIA",
        "icao": "KMIA",
        "delay": True,
        "delay_type": "Ground Stop",
        "reason": "hurricane",
        "reason_detail": "Hurricane approach - all operations suspended",
        "avg_delay_minutes": None,
        "closure": True,  # Airport is closing
        "ground_stop": True,
        "ground_delay_program": False,
        "issued_at": "2026-02-04T06:00:00Z",
        "expected_end": None,  # TBD based on storm
    },

    metar_data={
        "icao": "KMIA",
        "observed_at": "2026-02-04T14:56:00Z",
        "raw": "KMIA 041456Z 09045G65KT 1SM +RA BR SCT015 BKN025 OVC035 28/27 A2945 RMK AO2 PK WND 09065/1445 SLP000 P0145 T02830272",
        "wind_speed_kts": 45,
        "wind_gust_kts": 65,
        "wind_direction": 90,
        "visibility_miles": 1,
        "ceiling_feet": 1500,
        "temperature_c": 28,
        "dewpoint_c": 27,
        "altimeter_inhg": 29.45,  # Low pressure from hurricane
        "conditions": ["heavy rain", "mist"],
        "flight_category": "IFR",
        "weather_phenomena": ["+RA", "BR"],
        "remarks": "Peak wind 090 at 65 knots",
    },

    taf_data={
        "icao": "KMIA",
        "issued_at": "2026-02-04T12:00:00Z",
        "valid_from": "2026-02-04T12:00:00Z",
        "valid_to": "2026-02-05T18:00:00Z",
        "raw": "KMIA 041200Z 0412/0518 09040G60KT 1SM +RA OVC015 TEMPO 0414/0420 09055G75KT 1/2SM +RA VV010 FM050000 18025G40KT 3SM RA OVC020 FM050800 18015KT P6SM SCT030",
        "periods": [
            {
                "from": "2026-02-04T12:00:00Z",
                "to": "2026-02-04T20:00:00Z",
                "wind_speed_kts": 40,
                "wind_gust_kts": 60,
                "visibility_miles": 1,
                "ceiling_feet": 1500,
                "conditions": ["heavy rain"],
                "flight_category": "IFR",
            },
            {
                "from": "2026-02-04T20:00:00Z",
                "to": "2026-02-05T00:00:00Z",
                "wind_speed_kts": 55,
                "wind_gust_kts": 75,
                "visibility_miles": 0.5,
                "ceiling_feet": 1000,
                "conditions": ["heavy rain", "hurricane conditions"],
                "flight_category": "LIFR",
            },
        ],
    },

    nws_alerts=[
        {
            "id": "urn:oid:2.49.0.1.840.0.2026.2.4.9999",
            "event": "Hurricane Warning",
            "severity": "Extreme",
            "certainty": "Likely",
            "urgency": "Immediate",
            "headline": "Hurricane Warning in effect for Miami-Dade County",
            "description": "Hurricane Maria with maximum sustained winds of 100 mph is expected to make landfall near Miami tonight. Life-threatening storm surge, destructive winds, and flooding rainfall expected.",
            "instruction": "Complete preparations immediately. Evacuate if ordered to do so.",
            "effective": "2026-02-04T06:00:00Z",
            "expires": "2026-02-05T12:00:00Z",
            "area": "Miami-Dade County, Broward County",
        },
        {
            "id": "urn:oid:2.49.0.1.840.0.2026.2.4.9998",
            "event": "Storm Surge Warning",
            "severity": "Extreme",
            "certainty": "Likely",
            "urgency": "Immediate",
            "headline": "Storm Surge Warning for coastal Miami-Dade",
            "description": "Life-threatening storm surge of 6 to 10 feet expected.",
            "instruction": "Evacuate coastal areas immediately.",
            "effective": "2026-02-04T06:00:00Z",
            "expires": "2026-02-05T12:00:00Z",
            "area": "Coastal Miami-Dade County",
        }
    ],

    opensky_data={
        "aircraft_count": 0,  # Airport closed
        "aircraft_count_baseline": 55,
        "delta_percent": -100,  # Complete halt
        "bounding_box": {"min_lat": 25.6, "max_lat": 26.0, "min_lon": -80.5, "max_lon": -80.0},
        "timestamp": "2026-02-04T14:55:00Z",
    },
)


# =============================================================================
# SCENARIO: LAX Normal Operations
# =============================================================================
# Real-world inspiration: Many days at LAX are routine with no disruptions.
# System should correctly identify normal operations and recommend ACCEPT.

LAX_NORMAL = Scenario(
    id="lax_normal",
    name="LAX Normal Operations",
    description="""
    Scenario: Typical clear day at Los Angeles International. No FAA delays.
    METAR shows clear skies, good visibility. No NWS alerts.
    Normal aircraft movement levels.

    Expected Outcome: ACCEPT posture - normal operations, accept all bookings.
    """,
    airport_icao="KLAX",
    expected_posture=ExpectedPosture.ACCEPT,
    expected_risk_level="LOW",

    faa_data=None,  # No disruptions reported

    metar_data={
        "icao": "KLAX",
        "observed_at": "2026-02-04T15:53:00Z",
        "raw": "KLAX 041553Z 25008KT 10SM FEW025 22/12 A3002 RMK AO2 SLP168 T02220117",
        "wind_speed_kts": 8,
        "wind_gust_kts": None,
        "wind_direction": 250,
        "visibility_miles": 10,
        "ceiling_feet": None,  # No ceiling (few clouds at 2500)
        "temperature_c": 22,
        "dewpoint_c": 12,
        "altimeter_inhg": 30.02,
        "conditions": [],
        "flight_category": "VFR",
        "weather_phenomena": [],
    },

    taf_data={
        "icao": "KLAX",
        "issued_at": "2026-02-04T12:00:00Z",
        "valid_from": "2026-02-04T12:00:00Z",
        "valid_to": "2026-02-05T18:00:00Z",
        "raw": "KLAX 041200Z 0412/0518 25008KT P6SM FEW025 FM050000 VRB03KT P6SM SKC",
        "periods": [
            {
                "from": "2026-02-04T12:00:00Z",
                "to": "2026-02-05T00:00:00Z",
                "wind_speed_kts": 8,
                "visibility_miles": 10,
                "ceiling_feet": None,
                "conditions": [],
                "flight_category": "VFR",
            },
            {
                "from": "2026-02-05T00:00:00Z",
                "to": "2026-02-05T18:00:00Z",
                "wind_speed_kts": 3,
                "visibility_miles": 10,
                "ceiling_feet": None,
                "conditions": [],
                "flight_category": "VFR",
            },
        ],
    },

    nws_alerts=[],  # No alerts

    opensky_data={
        "aircraft_count": 85,
        "aircraft_count_baseline": 80,
        "delta_percent": 6,  # Slightly above normal
        "bounding_box": {"min_lat": 33.8, "max_lat": 34.1, "min_lon": -118.6, "max_lon": -118.2},
        "timestamp": "2026-02-04T15:55:00Z",
    },
)


# =============================================================================
# SCENARIO: DEN Fog (Morning Marine Layer)
# =============================================================================
# Real-world inspiration: Denver sometimes experiences dense fog in winter,
# causing IFR conditions and delays. Usually clears by mid-morning.

DEN_FOG = Scenario(
    id="den_fog",
    name="DEN Dense Fog - Morning IFR",
    description="""
    Scenario: Dense fog at Denver International causing IFR conditions.
    FAA Ground Delay Program with 45-minute average delays.
    TAF shows fog lifting by noon. Limited visibility operations in effect.

    Expected Outcome: RESTRICT posture - elevated risk while fog persists,
    but conditions improving.
    """,
    airport_icao="KDEN",
    expected_posture=ExpectedPosture.RESTRICT,
    expected_risk_level="MEDIUM",

    faa_data={
        "airport": "DEN",
        "icao": "KDEN",
        "delay": True,
        "delay_type": "Ground Delay Program",
        "reason": "fog",
        "reason_detail": "Dense fog - IFR conditions",
        "avg_delay_minutes": 45,
        "closure": False,
        "ground_stop": False,
        "ground_delay_program": True,
        "issued_at": "2026-02-04T13:00:00Z",
        "expected_end": "2026-02-04T17:00:00Z",
    },

    metar_data={
        "icao": "KDEN",
        "observed_at": "2026-02-04T14:53:00Z",
        "raw": "KDEN 041453Z 00000KT 1/4SM FG VV001 M02/M03 A3025 RMK AO2 SLP285 T10221028 $",
        "wind_speed_kts": 0,
        "wind_gust_kts": None,
        "wind_direction": 0,  # Calm
        "visibility_miles": 0.25,
        "ceiling_feet": 100,  # Vertical visibility
        "temperature_c": -2,
        "dewpoint_c": -3,
        "altimeter_inhg": 30.25,
        "conditions": ["fog"],
        "flight_category": "LIFR",
        "weather_phenomena": ["FG"],
    },

    taf_data={
        "icao": "KDEN",
        "issued_at": "2026-02-04T12:00:00Z",
        "valid_from": "2026-02-04T12:00:00Z",
        "valid_to": "2026-02-05T18:00:00Z",
        "raw": "KDEN 041200Z 0412/0518 VRB03KT 1/4SM FG VV001 TEMPO 0412/0416 1/8SM FG FM041600 18008KT P6SM SKC",
        "periods": [
            {
                "from": "2026-02-04T12:00:00Z",
                "to": "2026-02-04T16:00:00Z",
                "wind_speed_kts": 3,
                "visibility_miles": 0.25,
                "ceiling_feet": 100,
                "conditions": ["fog"],
                "flight_category": "LIFR",
            },
            {
                "from": "2026-02-04T16:00:00Z",
                "to": "2026-02-05T18:00:00Z",
                "wind_speed_kts": 8,
                "visibility_miles": 10,
                "ceiling_feet": None,
                "conditions": [],
                "flight_category": "VFR",
            },
        ],
    },

    nws_alerts=[
        {
            "id": "urn:oid:2.49.0.1.840.0.2026.2.4.3333",
            "event": "Dense Fog Advisory",
            "severity": "Minor",
            "certainty": "Observed",
            "urgency": "Expected",
            "headline": "Dense Fog Advisory until 10 AM MST",
            "description": "Dense fog with visibility below 1/4 mile in spots. Conditions will improve by mid-morning.",
            "instruction": "Slow down and use low beam headlights.",
            "effective": "2026-02-04T06:00:00Z",
            "expires": "2026-02-04T17:00:00Z",
            "area": "Denver Metro Area",
        }
    ],

    opensky_data={
        "aircraft_count": 40,  # Reduced from normal ~70
        "aircraft_count_baseline": 70,
        "delta_percent": -43,
        "bounding_box": {"min_lat": 39.7, "max_lat": 40.0, "min_lon": -105.0, "max_lon": -104.5},
        "timestamp": "2026-02-04T14:55:00Z",
    },
)


# =============================================================================
# SCENARIO: ATL Equipment Failure (Runway Closure)
# =============================================================================
# Real-world inspiration: Runway equipment failures (lighting, ILS) can cause
# reduced capacity and delays even in good weather.

ATL_RUNWAY_CLOSURE = Scenario(
    id="atl_runway_closure",
    name="ATL Runway Closure - Equipment Failure",
    description="""
    Scenario: Runway 27L closed at Atlanta due to ILS equipment failure.
    Weather is good but capacity reduced by 30%. FAA has issued Ground
    Delay Program with 60-minute average delays. No NWS alerts.

    Expected Outcome: RESTRICT posture - operational constraint (not weather)
    causing elevated delays but manageable.
    """,
    airport_icao="KATL",
    expected_posture=ExpectedPosture.RESTRICT,
    expected_risk_level="MEDIUM",

    faa_data={
        "airport": "ATL",
        "icao": "KATL",
        "delay": True,
        "delay_type": "Ground Delay Program",
        "reason": "runway/equipment",
        "reason_detail": "Runway 27L closed - ILS out of service",
        "avg_delay_minutes": 60,
        "closure": False,
        "ground_stop": False,
        "ground_delay_program": True,
        "issued_at": "2026-02-04T10:00:00Z",
        "expected_end": "2026-02-04T18:00:00Z",
    },

    metar_data={
        "icao": "KATL",
        "observed_at": "2026-02-04T15:53:00Z",
        "raw": "KATL 041553Z 27010KT 10SM SCT050 18/08 A3010 RMK AO2 SLP195 T01830078",
        "wind_speed_kts": 10,
        "wind_gust_kts": None,
        "wind_direction": 270,
        "visibility_miles": 10,
        "ceiling_feet": None,
        "temperature_c": 18,
        "dewpoint_c": 8,
        "altimeter_inhg": 30.10,
        "conditions": [],
        "flight_category": "VFR",
        "weather_phenomena": [],
    },

    taf_data={
        "icao": "KATL",
        "issued_at": "2026-02-04T12:00:00Z",
        "valid_from": "2026-02-04T12:00:00Z",
        "valid_to": "2026-02-05T18:00:00Z",
        "raw": "KATL 041200Z 0412/0518 27008KT P6SM SCT050 FM050000 VRB05KT P6SM SKC",
        "periods": [
            {
                "from": "2026-02-04T12:00:00Z",
                "to": "2026-02-05T18:00:00Z",
                "wind_speed_kts": 8,
                "visibility_miles": 10,
                "ceiling_feet": None,
                "conditions": [],
                "flight_category": "VFR",
            },
        ],
    },

    nws_alerts=[],  # No weather alerts - equipment issue

    opensky_data={
        "aircraft_count": 75,  # Reduced from normal ~100
        "aircraft_count_baseline": 100,
        "delta_percent": -25,
        "bounding_box": {"min_lat": 33.5, "max_lat": 33.8, "min_lon": -84.6, "max_lon": -84.2},
        "timestamp": "2026-02-04T15:55:00Z",
    },
)


# =============================================================================
# SCENARIO: SFO Low Ceilings (Marine Layer)
# =============================================================================

SFO_MARINE_LAYER = Scenario(
    id="sfo_marine_layer",
    name="SFO Marine Layer - Low Ceilings",
    description="""
    Scenario: Classic San Francisco marine layer causing low ceilings.
    FAA has issued Ground Delay Program. METAR shows 800ft ceilings, IFR.
    TAF predicts clearing by afternoon. Common summer pattern.

    Expected Outcome: RESTRICT posture - predictable pattern with
    known improvement timeline.
    """,
    airport_icao="KSFO",
    expected_posture=ExpectedPosture.RESTRICT,
    expected_risk_level="MEDIUM",

    faa_data={
        "airport": "SFO",
        "icao": "KSFO",
        "delay": True,
        "delay_type": "Ground Delay Program",
        "reason": "low ceilings",
        "reason_detail": "Marine layer - IFR conditions",
        "avg_delay_minutes": 75,
        "closure": False,
        "ground_stop": False,
        "ground_delay_program": True,
        "issued_at": "2026-02-04T14:00:00Z",
        "expected_end": "2026-02-04T20:00:00Z",
    },

    metar_data={
        "icao": "KSFO",
        "observed_at": "2026-02-04T15:56:00Z",
        "raw": "KSFO 041556Z 28012KT 4SM BR OVC008 14/12 A2998 RMK AO2 SLP152 T01390117",
        "wind_speed_kts": 12,
        "wind_gust_kts": None,
        "wind_direction": 280,
        "visibility_miles": 4,
        "ceiling_feet": 800,
        "temperature_c": 14,
        "dewpoint_c": 12,
        "altimeter_inhg": 29.98,
        "conditions": ["mist"],
        "flight_category": "IFR",
        "weather_phenomena": ["BR"],
    },

    taf_data={
        "icao": "KSFO",
        "issued_at": "2026-02-04T12:00:00Z",
        "valid_from": "2026-02-04T12:00:00Z",
        "valid_to": "2026-02-05T18:00:00Z",
        "raw": "KSFO 041200Z 0412/0518 28010KT 3SM BR OVC008 FM041900 28015KT P6SM SCT025 FM050600 VRB05KT P6SM SKC",
        "periods": [
            {
                "from": "2026-02-04T12:00:00Z",
                "to": "2026-02-04T19:00:00Z",
                "wind_speed_kts": 10,
                "visibility_miles": 3,
                "ceiling_feet": 800,
                "conditions": ["mist"],
                "flight_category": "IFR",
            },
            {
                "from": "2026-02-04T19:00:00Z",
                "to": "2026-02-05T18:00:00Z",
                "wind_speed_kts": 15,
                "visibility_miles": 10,
                "ceiling_feet": 2500,
                "conditions": [],
                "flight_category": "VFR",
            },
        ],
    },

    nws_alerts=[],

    opensky_data={
        "aircraft_count": 35,  # Reduced from normal ~55
        "aircraft_count_baseline": 55,
        "delta_percent": -36,
        "bounding_box": {"min_lat": 37.5, "max_lat": 37.8, "min_lon": -122.5, "max_lon": -122.2},
        "timestamp": "2026-02-04T15:55:00Z",
    },
)


# =============================================================================
# SCENARIO: Contradiction - FAA vs Weather
# =============================================================================
# Real-world inspiration: Sometimes FAA status doesn't match current conditions
# (stale data, system delays). System should detect contradiction.

SEA_CONTRADICTION = Scenario(
    id="sea_contradiction",
    name="SEA Contradiction - FAA vs Weather Mismatch",
    description="""
    Scenario: FAA shows normal operations at Seattle, but METAR shows
    heavy snow with 1/4 mile visibility. Either FAA status is stale
    or conditions changed rapidly. System should detect contradiction.

    Expected Outcome: RESTRICT or HOLD posture - contradiction detected,
    err on side of caution until resolved.
    """,
    airport_icao="KSEA",
    expected_posture=ExpectedPosture.RESTRICT,
    expected_risk_level="MEDIUM",
    has_contradiction=True,

    faa_data=None,  # FAA shows normal (no disruptions)

    metar_data={
        "icao": "KSEA",
        "observed_at": "2026-02-04T15:53:00Z",
        "raw": "KSEA 041553Z 02020G30KT 1/4SM +SN FZFG VV003 M04/M05 A2970 RMK AO2 SLP065 P0018",
        "wind_speed_kts": 20,
        "wind_gust_kts": 30,
        "wind_direction": 20,
        "visibility_miles": 0.25,
        "ceiling_feet": 300,
        "temperature_c": -4,
        "dewpoint_c": -5,
        "altimeter_inhg": 29.70,
        "conditions": ["heavy snow", "freezing fog"],
        "flight_category": "LIFR",
        "weather_phenomena": ["+SN", "FZFG"],
    },

    taf_data={
        "icao": "KSEA",
        "issued_at": "2026-02-04T12:00:00Z",
        "valid_from": "2026-02-04T12:00:00Z",
        "valid_to": "2026-02-05T18:00:00Z",
        "raw": "KSEA 041200Z 0412/0518 02015G25KT 1/2SM +SN VV003 FM041800 36010KT 3SM -SN OVC015",
        "periods": [
            {
                "from": "2026-02-04T12:00:00Z",
                "to": "2026-02-04T18:00:00Z",
                "wind_speed_kts": 15,
                "wind_gust_kts": 25,
                "visibility_miles": 0.5,
                "ceiling_feet": 300,
                "conditions": ["heavy snow"],
                "flight_category": "LIFR",
            },
        ],
    },

    nws_alerts=[
        {
            "id": "urn:oid:2.49.0.1.840.0.2026.2.4.7777",
            "event": "Winter Storm Warning",
            "severity": "Severe",
            "certainty": "Likely",
            "urgency": "Expected",
            "headline": "Winter Storm Warning for Seattle Area",
            "description": "Heavy snow expected. 6 to 10 inches accumulation.",
            "instruction": "Travel should be restricted.",
            "effective": "2026-02-04T06:00:00Z",
            "expires": "2026-02-05T00:00:00Z",
            "area": "Seattle, King County",
        }
    ],

    opensky_data={
        "aircraft_count": 15,  # Very low for SEA
        "aircraft_count_baseline": 50,
        "delta_percent": -70,
        "bounding_box": {"min_lat": 47.3, "max_lat": 47.6, "min_lon": -122.5, "max_lon": -122.1},
        "timestamp": "2026-02-04T15:55:00Z",
    },
)


# =============================================================================
# SCENARIO: Degraded - OpenSky Timeout
# =============================================================================
# Real-world inspiration: OpenSky API can be rate-limited or timeout.
# System should handle gracefully with reduced confidence.

DFW_OPENSKY_TIMEOUT = Scenario(
    id="dfw_opensky_timeout",
    name="DFW OpenSky Timeout - Degraded Mode",
    description="""
    Scenario: Dallas-Fort Worth with moderate weather delay. OpenSky API
    times out, so aircraft movement data is unavailable. System should
    proceed with degraded confidence.

    Expected Outcome: RESTRICT posture - cannot fully assess situation
    without movement data, but FAA/weather indicate manageable delays.
    """,
    airport_icao="KDFW",
    expected_posture=ExpectedPosture.RESTRICT,
    expected_risk_level="MEDIUM",
    has_missing_source=True,
    missing_source="OPENSKY",

    faa_data={
        "airport": "DFW",
        "icao": "KDFW",
        "delay": True,
        "delay_type": "Ground Delay Program",
        "reason": "wind",
        "reason_detail": "High crosswinds requiring runway configuration change",
        "avg_delay_minutes": 45,
        "closure": False,
        "ground_stop": False,
        "ground_delay_program": True,
        "issued_at": "2026-02-04T14:00:00Z",
        "expected_end": "2026-02-04T18:00:00Z",
    },

    metar_data={
        "icao": "KDFW",
        "observed_at": "2026-02-04T15:53:00Z",
        "raw": "KDFW 041553Z 18025G35KT 10SM SCT040 BKN060 28/14 A2995 RMK AO2 PK WND 18035/1545",
        "wind_speed_kts": 25,
        "wind_gust_kts": 35,
        "wind_direction": 180,
        "visibility_miles": 10,
        "ceiling_feet": 4000,
        "temperature_c": 28,
        "dewpoint_c": 14,
        "altimeter_inhg": 29.95,
        "conditions": [],
        "flight_category": "VFR",
        "weather_phenomena": [],
    },

    taf_data={
        "icao": "KDFW",
        "issued_at": "2026-02-04T12:00:00Z",
        "valid_from": "2026-02-04T12:00:00Z",
        "valid_to": "2026-02-05T18:00:00Z",
        "raw": "KDFW 041200Z 0412/0518 18020G30KT P6SM SCT040 FM041800 18015KT P6SM FEW050",
        "periods": [
            {
                "from": "2026-02-04T12:00:00Z",
                "to": "2026-02-04T18:00:00Z",
                "wind_speed_kts": 20,
                "wind_gust_kts": 30,
                "visibility_miles": 10,
                "ceiling_feet": 4000,
                "conditions": [],
                "flight_category": "VFR",
            },
        ],
    },

    nws_alerts=[
        {
            "id": "urn:oid:2.49.0.1.840.0.2026.2.4.4444",
            "event": "Wind Advisory",
            "severity": "Minor",
            "certainty": "Observed",
            "urgency": "Expected",
            "headline": "Wind Advisory until 6 PM CDT",
            "description": "South winds 20 to 30 mph with gusts to 40 mph expected.",
            "instruction": "Secure outdoor objects.",
            "effective": "2026-02-04T12:00:00Z",
            "expires": "2026-02-04T23:00:00Z",
            "area": "Dallas-Fort Worth Metroplex",
        }
    ],

    opensky_data=None,  # TIMEOUT - data unavailable
)


# =============================================================================
# SCENARIO: BOS Clear After Storm
# =============================================================================

BOS_POST_STORM = Scenario(
    id="bos_post_storm",
    name="BOS Post-Storm Recovery",
    description="""
    Scenario: Boston recovering from overnight storm. FAA shows some
    residual delays but improving. METAR shows clearing conditions.

    Expected Outcome: ACCEPT posture - conditions have improved,
    system should recognize recovery in progress.
    """,
    airport_icao="KBOS",
    expected_posture=ExpectedPosture.ACCEPT,
    expected_risk_level="LOW",

    faa_data={
        "airport": "BOS",
        "icao": "KBOS",
        "delay": True,
        "delay_type": "Departure Delay",
        "reason": "volume",
        "reason_detail": "Departure delays due to catch-up volume",
        "avg_delay_minutes": 20,
        "closure": False,
        "ground_stop": False,
        "ground_delay_program": False,
        "issued_at": "2026-02-04T12:00:00Z",
        "expected_end": "2026-02-04T16:00:00Z",
    },

    metar_data={
        "icao": "KBOS",
        "observed_at": "2026-02-04T15:54:00Z",
        "raw": "KBOS 041554Z 31012KT 10SM FEW040 SCT100 08/M02 A3015 RMK AO2 SLP215",
        "wind_speed_kts": 12,
        "wind_gust_kts": None,
        "wind_direction": 310,
        "visibility_miles": 10,
        "ceiling_feet": None,
        "temperature_c": 8,
        "dewpoint_c": -2,
        "altimeter_inhg": 30.15,
        "conditions": [],
        "flight_category": "VFR",
        "weather_phenomena": [],
    },

    taf_data={
        "icao": "KBOS",
        "issued_at": "2026-02-04T12:00:00Z",
        "valid_from": "2026-02-04T12:00:00Z",
        "valid_to": "2026-02-05T18:00:00Z",
        "raw": "KBOS 041200Z 0412/0518 31010KT P6SM SCT040 FM050000 VRB05KT P6SM SKC",
        "periods": [
            {
                "from": "2026-02-04T12:00:00Z",
                "to": "2026-02-05T18:00:00Z",
                "wind_speed_kts": 10,
                "visibility_miles": 10,
                "ceiling_feet": None,
                "conditions": [],
                "flight_category": "VFR",
            },
        ],
    },

    nws_alerts=[],  # Storm has passed

    opensky_data={
        "aircraft_count": 52,  # Near normal
        "aircraft_count_baseline": 55,
        "delta_percent": -5,
        "bounding_box": {"min_lat": 42.2, "max_lat": 42.5, "min_lon": -71.2, "max_lon": -70.8},
        "timestamp": "2026-02-04T15:55:00Z",
    },
)


# =============================================================================
# ALL SCENARIOS
# =============================================================================

SCENARIOS: Dict[str, Scenario] = {
    "jfk_ground_stop": JFK_GROUND_STOP,
    "ord_thunderstorms": ORD_THUNDERSTORMS,
    "mia_hurricane": MIA_HURRICANE,
    "lax_normal": LAX_NORMAL,
    "den_fog": DEN_FOG,
    "atl_runway_closure": ATL_RUNWAY_CLOSURE,
    "sfo_marine_layer": SFO_MARINE_LAYER,
    "sea_contradiction": SEA_CONTRADICTION,
    "dfw_opensky_timeout": DFW_OPENSKY_TIMEOUT,
    "bos_post_storm": BOS_POST_STORM,
}


def get_scenario(scenario_id: str) -> Optional[Scenario]:
    """Get a scenario by ID."""
    return SCENARIOS.get(scenario_id)


def list_scenarios() -> List[Dict[str, Any]]:
    """List all available scenarios with metadata."""
    return [
        {
            "id": s.id,
            "name": s.name,
            "airport": s.airport_icao,
            "expected_posture": s.expected_posture.value,
            "expected_risk": s.expected_risk_level,
            "has_contradiction": s.has_contradiction,
            "has_missing_source": s.has_missing_source,
        }
        for s in SCENARIOS.values()
    ]
