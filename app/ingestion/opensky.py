# app/ingestion/opensky.py
"""
OpenSky Network ADS-B ingestion.

Source: https://opensky-network.org/api/states/all

Returns real-time aircraft state vectors (position, altitude, velocity)
for a bounding box around airport metro areas.

IMPORTANT: On failure, creates MissingEvidenceRequest instead of silent fail.
This provides first-class tracking of degraded data sources.
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
import httpx

from .http import HttpClient, HttpClientError, HttpTimeoutError

# OpenSky API endpoint
OPENSKY_URL = "https://opensky-network.org/api/states/all"

# Bounding boxes around major airports (lat_min, lat_max, lon_min, lon_max)
# Approx 50nm radius. Covers all major US (K*) and Pacific (P*) airports.
AIRPORT_BBOXES: Dict[str, Tuple[float, float, float, float]] = {
    # ==========================================================================
    # MAJOR HUBS (Top 30 US by passenger traffic)
    # ==========================================================================
    "KATL": (33.0, 34.3, -85.1, -83.7),    # Atlanta Hartsfield-Jackson
    "KORD": (41.3, 42.6, -88.6, -87.2),    # Chicago O'Hare
    "KDFW": (32.2, 33.5, -97.7, -96.3),    # Dallas/Fort Worth
    "KDEN": (39.2, 40.5, -105.4, -104.0),  # Denver
    "KJFK": (40.0, 41.3, -74.5, -73.0),    # New York JFK
    "KLAX": (33.3, 34.6, -119.0, -117.5),  # Los Angeles
    "KMIA": (25.1, 26.4, -81.0, -79.6),    # Miami
    "KLAS": (35.7, 36.6, -115.6, -114.6),  # Las Vegas
    "KMCO": (27.9, 28.9, -81.8, -80.8),    # Orlando
    "KSEA": (46.8, 48.1, -123.0, -121.6),  # Seattle
    "KEWR": (40.2, 41.2, -74.7, -73.7),    # Newark
    "KSFO": (36.9, 38.2, -123.1, -121.7),  # San Francisco
    "KPHX": (32.9, 33.9, -112.6, -111.6),  # Phoenix
    "KBOS": (41.7, 43.0, -71.8, -70.4),    # Boston
    "KLGA": (40.3, 41.3, -74.4, -73.4),    # New York LaGuardia
    "KFLL": (25.6, 26.6, -80.6, -79.6),    # Fort Lauderdale
    "KMSP": (44.4, 45.4, -93.8, -92.8),    # Minneapolis
    "KDTW": (41.8, 42.8, -84.0, -83.0),    # Detroit
    "KPHL": (39.5, 40.5, -75.7, -74.7),    # Philadelphia
    "KCLT": (34.8, 35.8, -81.3, -80.3),    # Charlotte
    "KSLC": (40.3, 41.3, -112.5, -111.5),  # Salt Lake City
    "KIAD": (38.5, 39.5, -77.9, -76.9),    # Washington Dulles
    "KDCA": (38.4, 39.4, -77.5, -76.5),    # Washington Reagan
    "KBWI": (38.8, 39.8, -77.2, -76.2),    # Baltimore
    "KTPA": (27.4, 28.4, -83.0, -82.0),    # Tampa
    "KSAN": (32.3, 33.3, -117.7, -116.7),  # San Diego
    "KPDX": (45.1, 46.1, -123.1, -122.1),  # Portland
    "KSTL": (38.2, 39.2, -91.0, -90.0),    # St. Louis
    "KMDW": (41.3, 42.3, -88.3, -87.3),    # Chicago Midway
    "KHOU": (29.1, 30.1, -95.8, -94.8),    # Houston Hobby

    # ==========================================================================
    # REGIONAL HUBS
    # ==========================================================================
    "KIAH": (29.5, 30.5, -96.0, -95.0),    # Houston Intercontinental
    "KAUS": (29.8, 30.8, -98.2, -97.2),    # Austin
    "KDAL": (32.4, 33.4, -97.4, -96.4),    # Dallas Love Field
    "KORF": (36.4, 37.4, -76.7, -75.7),    # Norfolk
    "KSAT": (29.0, 30.0, -99.0, -98.0),    # San Antonio
    "KBNA": (35.6, 36.6, -87.3, -86.3),    # Nashville
    "KRDU": (35.4, 36.4, -79.4, -78.4),    # Raleigh-Durham
    "KSMF": (38.2, 39.2, -122.1, -121.1),  # Sacramento
    "KSJC": (36.9, 37.9, -122.4, -121.4),  # San Jose
    "KOAK": (37.2, 38.2, -122.7, -121.7),  # Oakland
    "KSNA": (33.2, 34.2, -118.2, -117.2),  # Orange County
    "KONT": (33.6, 34.6, -118.0, -117.0),  # Ontario CA
    "KBUR": (33.9, 34.9, -118.8, -117.8),  # Burbank
    "KIND": (39.2, 40.2, -86.8, -85.8),    # Indianapolis
    "KCMH": (39.5, 40.5, -83.4, -82.4),    # Columbus OH
    "KCLE": (40.9, 41.9, -82.3, -81.3),    # Cleveland
    "KPIT": (40.0, 41.0, -80.7, -79.7),    # Pittsburgh
    "KMCI": (38.8, 39.8, -95.1, -94.1),    # Kansas City
    "KMKE": (42.5, 43.5, -88.4, -87.4),    # Milwaukee
    "KCVG": (38.6, 39.6, -85.1, -84.1),    # Cincinnati
    "KRSW": (26.1, 27.1, -82.3, -81.3),    # Fort Myers
    "KPBI": (26.2, 27.2, -80.6, -79.6),    # West Palm Beach
    "KJAX": (29.6, 30.6, -82.2, -81.2),    # Jacksonville
    "KSDF": (37.8, 38.8, -86.2, -85.2),    # Louisville
    "KMEM": (34.6, 35.6, -90.5, -89.5),    # Memphis
    "KOMA": (40.9, 41.9, -96.5, -95.5),    # Omaha
    "KOKC": (35.0, 36.0, -97.9, -96.9),    # Oklahoma City
    "KTUL": (35.6, 36.6, -96.3, -95.3),    # Tulsa
    "KABQ": (34.6, 35.6, -107.0, -106.0),  # Albuquerque
    "KELP": (31.3, 32.3, -106.8, -105.8),  # El Paso
    "KBOI": (43.1, 44.1, -116.7, -115.7),  # Boise
    "KRNO": (39.0, 40.0, -120.4, -119.4),  # Reno
    "KBDL": (41.4, 42.4, -73.2, -72.2),    # Hartford/Bradley
    "KBUF": (42.4, 43.4, -79.2, -78.2),    # Buffalo
    "KPVD": (41.2, 42.2, -71.9, -70.9),    # Providence
    "KALB": (42.2, 43.2, -74.2, -73.2),    # Albany NY
    "KSYR": (42.6, 43.6, -76.6, -75.6),    # Syracuse
    "KROC": (42.7, 43.7, -78.1, -77.1),    # Rochester NY

    # ==========================================================================
    # CARGO HUBS (Important for freight forwarders)
    # ==========================================================================
    "KSDF": (37.8, 38.8, -86.2, -85.2),    # Louisville (UPS Hub)
    "KMEM": (34.6, 35.6, -90.5, -89.5),    # Memphis (FedEx Hub)
    "PANC": (60.7, 61.7, -150.5, -149.5),  # Anchorage (Asia-US cargo)
    "KONT": (33.6, 34.6, -118.0, -117.0),  # Ontario CA (Amazon)
    "KCVG": (38.6, 39.6, -85.1, -84.1),    # Cincinnati (DHL Hub)
    "KRIC": (37.0, 38.0, -77.8, -76.8),    # Richmond
    "KGSO": (35.6, 36.6, -80.4, -79.4),    # Greensboro (FedEx)
    "KRFD": (41.9, 42.9, -89.6, -88.6),    # Rockford (UPS)
    "KAFW": (32.5, 33.5, -97.8, -96.8),    # Fort Worth Alliance (FedEx)

    # ==========================================================================
    # PACIFIC TERRITORIES (P* prefix)
    # ==========================================================================
    "PHNL": (21.0, 21.8, -158.4, -157.4),  # Honolulu
    "PHOG": (20.4, 21.2, -156.9, -155.9),  # Maui/Kahului
    "PHKO": (19.3, 20.1, -156.5, -155.5),  # Kona
    "PHLI": (21.8, 22.6, -159.8, -158.8),  # Lihue (Kauai)
    "PHTO": (19.2, 20.0, -155.6, -154.6),  # Hilo
    "PGUM": (13.0, 14.0, 144.3, 145.3),    # Guam
    "PGSN": (14.7, 15.5, 145.2, 146.2),    # Saipan
    "PANC": (60.7, 61.7, -150.5, -149.5),  # Anchorage
    "PAFA": (64.3, 65.3, -148.5, -147.5),  # Fairbanks
    "PAJN": (57.8, 58.8, -135.2, -134.2),  # Juneau

    # ==========================================================================
    # ADDITIONAL REGIONAL (Complete US coverage)
    # ==========================================================================
    "KPSP": (33.3, 34.3, -117.1, -116.1),  # Palm Springs
    "KGEG": (47.1, 48.1, -118.0, -117.0),  # Spokane
    "KCOS": (38.3, 39.3, -105.1, -104.1),  # Colorado Springs
    "KICT": (37.2, 38.2, -97.9, -96.9),    # Wichita
    "KLIT": (34.4, 35.4, -92.7, -91.7),    # Little Rock
    "KDSM": (41.0, 42.0, -94.0, -93.0),    # Des Moines
    "KBTV": (44.0, 45.0, -73.6, -72.6),    # Burlington VT
    "KPWM": (43.2, 44.2, -70.8, -69.8),    # Portland ME
    "KMHT": (42.5, 43.5, -72.0, -71.0),    # Manchester NH
    "KGSP": (34.4, 35.4, -82.7, -81.7),    # Greenville-Spartanburg
    "KCHS": (32.4, 33.4, -80.5, -79.5),    # Charleston SC
    "KMYR": (33.2, 34.2, -79.4, -78.4),    # Myrtle Beach
    "KSAV": (31.6, 32.6, -81.7, -80.7),    # Savannah
    "KBHM": (33.1, 34.1, -87.3, -86.3),    # Birmingham AL
    "KMOB": (30.2, 31.2, -88.7, -87.7),    # Mobile
    "KJAN": (32.0, 33.0, -90.5, -89.5),    # Jackson MS
    "KBTTR": (29.8, 30.8, -91.6, -90.6),   # Baton Rouge
    "KMSY": (29.5, 30.5, -90.8, -89.8),    # New Orleans
    "KSHV": (32.0, 33.0, -94.3, -93.3),    # Shreveport
    "KLBB": (33.2, 34.2, -102.3, -101.3),  # Lubbock
    "KAMA": (34.7, 35.7, -102.0, -101.0),  # Amarillo
    "KMAF": (31.5, 32.5, -102.7, -101.7),  # Midland-Odessa
    "KCORP": (27.3, 28.3, -97.8, -96.8),   # Corpus Christi
    "KBRO": (25.4, 26.4, -97.9, -96.9),    # Brownsville
    "KMFE": (26.0, 27.0, -98.7, -97.7),    # McAllen
    "KLAR": (41.0, 42.0, -106.0, -105.0),  # Laramie
    "KCPR": (42.3, 43.3, -106.9, -105.9),  # Casper
    "KBZN": (45.3, 46.3, -111.6, -110.6),  # Bozeman
    "KMSO": (46.4, 47.4, -114.6, -113.6),  # Missoula
    "KBIL": (45.3, 46.3, -109.0, -108.0),  # Billings
    "KFAT": (36.3, 37.3, -120.2, -119.2),  # Fresno
    "KSBP": (35.0, 36.0, -121.0, -120.0),  # San Luis Obispo
    "KSTS": (38.0, 39.0, -123.1, -122.1),  # Santa Rosa/Sonoma
    "KMRY": (36.1, 37.1, -122.2, -121.2),  # Monterey
    "KRDM": (43.9, 44.9, -121.6, -120.6),  # Redmond/Bend OR
    "KEUG": (43.6, 44.6, -123.7, -122.7),  # Eugene OR
    "KMFR": (42.0, 43.0, -123.2, -122.2),  # Medford OR

    # ==========================================================================
    # CARIBBEAN TERRITORIES (TJ*, TI* prefixes)
    # ==========================================================================
    "TJSJ": (17.9, 18.9, -66.5, -65.5),    # San Juan, Puerto Rico
    "TJBQ": (18.0, 19.0, -67.6, -66.6),    # Aguadilla, Puerto Rico
    "TJPS": (17.5, 18.5, -67.1, -66.1),    # Ponce, Puerto Rico
    "TIST": (17.8, 18.8, -65.5, -64.5),    # St Thomas, USVI
    "TISX": (17.2, 18.2, -65.3, -64.3),    # St Croix, USVI

    # ==========================================================================
    # ADDITIONAL MISSING AIRPORTS
    # ==========================================================================
    "KFAR": (46.4, 47.4, -97.3, -96.3),    # Fargo ND
    "KBIS": (46.3, 47.3, -101.2, -100.2),  # Bismarck ND
    "KFSD": (43.1, 44.1, -97.2, -96.2),    # Sioux Falls SD
    "KRAP": (43.5, 44.5, -103.6, -102.6),  # Rapid City SD
    "KCRW": (37.9, 38.9, -82.1, -81.1),    # Charleston WV
    "KJAC": (43.1, 44.1, -110.9, -109.9),  # Jackson Hole WY
    "KGPT": (29.9, 30.9, -89.6, -88.6),    # Gulfport MS
    "KBGR": (44.3, 45.3, -69.3, -68.3),    # Bangor ME
    "KFCA": (47.8, 48.8, -114.8, -113.8),  # Kalispell MT
    "KIDA": (43.0, 44.0, -112.6, -111.6),  # Idaho Falls ID
    "KTWF": (42.0, 43.0, -114.9, -113.9),  # Twin Falls ID
    "KHSV": (34.1, 35.1, -87.3, -86.3),    # Huntsville AL
    "KPNS": (30.0, 31.0, -87.7, -86.7),    # Pensacola FL
    "KVPS": (30.0, 31.0, -87.0, -86.0),    # Fort Walton Beach FL
    "KECP": (29.9, 30.9, -86.3, -85.3),    # Panama City FL
    # Additional from airports.js
    "KTYS": (35.3, 36.3, -84.5, -83.5),    # Knoxville TN
    "KCHA": (34.5, 35.5, -85.7, -84.7),    # Chattanooga TN
    "KLEX": (37.5, 38.5, -85.1, -84.1),    # Lexington KY
    "KDAY": (39.4, 40.4, -84.7, -83.7),    # Dayton OH
    "KCAK": (40.4, 41.4, -81.9, -80.9),    # Akron-Canton OH
    "KFWA": (40.5, 41.5, -85.7, -84.7),    # Fort Wayne IN
    "KSBN": (41.2, 42.2, -86.8, -85.8),    # South Bend IN
    "KLAN": (42.3, 43.3, -85.1, -84.1),    # Lansing MI
    "KFNT": (42.5, 43.5, -84.2, -83.2),    # Flint MI
    "KAZO": (41.7, 42.7, -86.0, -85.0),    # Kalamazoo MI
    "KMSN": (42.6, 43.6, -89.8, -88.8),    # Madison WI
    "KGRB": (44.0, 45.0, -88.6, -87.6),    # Green Bay WI
    "KSPI": (39.3, 40.3, -90.2, -89.2),    # Springfield IL
    "KMLI": (41.0, 42.0, -91.0, -90.0),    # Moline/Quad City IL
    "KBLI": (48.3, 49.3, -123.0, -122.0),  # Bellingham WA
}


@dataclass
class AircraftState:
    """Single aircraft state from OpenSky."""
    icao24: str  # ICAO 24-bit transponder address
    callsign: Optional[str]
    origin_country: str
    time_position: Optional[int]  # Unix timestamp
    last_contact: int  # Unix timestamp
    longitude: Optional[float]
    latitude: Optional[float]
    baro_altitude: Optional[float]  # meters
    on_ground: bool
    velocity: Optional[float]  # m/s
    true_track: Optional[float]  # degrees
    vertical_rate: Optional[float]  # m/s
    geo_altitude: Optional[float]  # meters
    squawk: Optional[str]
    spi: bool  # Special purpose indicator


@dataclass
class OpenSkyResponse:
    """Full OpenSky API response."""
    time: int  # Unix timestamp
    states: List[AircraftState]
    aircraft_count: int
    retrieved_at: datetime
    raw_data: Dict[str, Any]


@dataclass
class MissingEvidenceRequest:
    """
    First-class tracking of missing evidence.

    When a data source fails (timeout, rate limit, etc.),
    create this record to track the degradation.
    """
    case_id: Optional[str]
    source_system: str
    request_type: str
    request_params: Dict[str, Any]
    reason: str
    criticality: str  # BLOCKING, DEGRADED, INFORMATIONAL


class OpenSkyClient:
    """
    Client for OpenSky Network API.

    Fetches aircraft states with first-class degradation handling.
    On failure, returns None and creates MissingEvidenceRequest.
    """

    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout
        self.last_missing_evidence: Optional[MissingEvidenceRequest] = None

    def fetch_states(
        self,
        bbox: Tuple[float, float, float, float],
        case_id: Optional[str] = None
    ) -> Optional[OpenSkyResponse]:
        """
        Fetch aircraft states for bounding box.

        Args:
            bbox: (lat_min, lat_max, lon_min, lon_max)
            case_id: Optional case ID for tracking degradation

        Returns:
            OpenSkyResponse if successful, None if failed

        Note:
            On failure, self.last_missing_evidence contains the
            MissingEvidenceRequest that should be persisted.
        """
        lat_min, lat_max, lon_min, lon_max = bbox
        params = {
            "lamin": lat_min,
            "lamax": lat_max,
            "lomin": lon_min,
            "lomax": lon_max,
        }

        self.last_missing_evidence = None

        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.get(OPENSKY_URL, params=params)
                response.raise_for_status()
                data = response.json()

            retrieved_at = datetime.now(timezone.utc)

            if data is None:
                self._record_missing_evidence(
                    case_id=case_id,
                    request_params=params,
                    reason="API returned null response",
                    criticality="DEGRADED",
                )
                return None

            # Check if states is explicitly null (different from missing or empty)
            raw_states = data.get('states')
            if raw_states is None:
                # API returned {"states": null} - this is degraded data
                # Could mean: no aircraft, or API data quality issue
                self._record_missing_evidence(
                    case_id=case_id,
                    request_params=params,
                    reason="API returned null states - possible data quality issue or no aircraft in area",
                    criticality="INFORMATIONAL",  # Less severe than timeout
                )
                # Return response with zero aircraft - this is informative, not failure
                return OpenSkyResponse(
                    time=data.get('time', 0),
                    states=[],
                    aircraft_count=0,
                    retrieved_at=retrieved_at,
                    raw_data=data,
                )

            states = self._parse_states(raw_states)

            return OpenSkyResponse(
                time=data.get('time', 0),
                states=states,
                aircraft_count=len(states),
                retrieved_at=retrieved_at,
                raw_data=data,
            )

        except httpx.TimeoutException as e:
            # First-class tracking of missing evidence
            self._record_missing_evidence(
                case_id=case_id,
                request_params=params,
                reason=f"Timeout after {self.timeout}s: {e}",
                criticality="DEGRADED",
            )
            return None

        except httpx.HTTPStatusError as e:
            # Rate limiting or server error
            self._record_missing_evidence(
                case_id=case_id,
                request_params=params,
                reason=f"HTTP {e.response.status_code}: {e}",
                criticality="DEGRADED",
            )
            return None

        except Exception as e:
            self._record_missing_evidence(
                case_id=case_id,
                request_params=params,
                reason=f"Unexpected error: {e}",
                criticality="DEGRADED",
            )
            return None

    def fetch_states_for_airport(
        self,
        icao: str,
        case_id: Optional[str] = None
    ) -> Optional[OpenSkyResponse]:
        """
        Fetch aircraft states around airport.

        Args:
            icao: ICAO airport code
            case_id: Optional case ID for tracking

        Returns:
            OpenSkyResponse if successful
        """
        bbox = AIRPORT_BBOXES.get(icao.upper())
        if not bbox:
            # Record clear error when airport doesn't have bounding box
            self._record_missing_evidence(
                case_id=case_id,
                request_params={"icao": icao},
                reason=f"No bounding box defined for {icao}. Supported: {', '.join(sorted(AIRPORT_BBOXES.keys()))}",
                criticality="INFORMATIONAL",
            )
            return None

        return self.fetch_states(bbox, case_id)

    def _record_missing_evidence(
        self,
        case_id: Optional[str],
        request_params: Dict[str, Any],
        reason: str,
        criticality: str,
    ):
        """Create MissingEvidenceRequest for tracking."""
        self.last_missing_evidence = MissingEvidenceRequest(
            case_id=case_id,
            source_system="OPENSKY",
            request_type="aircraft_states",
            request_params=request_params,
            reason=reason,
            criticality=criticality,
        )

    def _parse_states(self, states_data: List[List]) -> List[AircraftState]:
        """Parse aircraft states from API response."""
        states = []
        for state in states_data:
            if len(state) < 17:
                continue

            states.append(AircraftState(
                icao24=state[0] or '',
                callsign=state[1].strip() if state[1] else None,
                origin_country=state[2] or '',
                time_position=state[3],
                last_contact=state[4] or 0,
                longitude=state[5],
                latitude=state[6],
                baro_altitude=state[7],
                on_ground=state[8] or False,
                velocity=state[9],
                true_track=state[10],
                vertical_rate=state[11],
                geo_altitude=state[13],
                squawk=state[14],
                spi=state[15] or False,
            ))

        return states


def fetch_opensky(
    icao: str,
    case_id: Optional[str] = None,
    timeout: float = 10.0
) -> Tuple[Optional[OpenSkyResponse], Optional[MissingEvidenceRequest]]:
    """
    Convenience function to fetch OpenSky data for airport.

    Args:
        icao: ICAO airport code
        case_id: Optional case ID for degradation tracking
        timeout: Request timeout

    Returns:
        Tuple of (OpenSkyResponse, MissingEvidenceRequest)
        - On success: (response, None)
        - On failure: (None, missing_evidence_request)
    """
    client = OpenSkyClient(timeout=timeout)
    response = client.fetch_states_for_airport(icao, case_id)
    return response, client.last_missing_evidence


def get_airport_bbox(icao: str) -> Optional[Tuple[float, float, float, float]]:
    """
    Get bounding box for airport.

    Args:
        icao: ICAO airport code

    Returns:
        (lat_min, lat_max, lon_min, lon_max) or None
    """
    return AIRPORT_BBOXES.get(icao.upper())
