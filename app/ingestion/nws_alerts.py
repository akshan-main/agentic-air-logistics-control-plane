# app/ingestion/nws_alerts.py
"""
National Weather Service (NWS) Alerts ingestion.

Source: https://api.weather.gov/alerts/active?point={lat},{lon}

Returns active weather alerts for a geographic location:
- Severe thunderstorm warnings
- Tornado warnings
- Winter storm warnings
- Flood warnings
- etc.
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass

from .http import HttpClient, HttpClientError

# NWS Alerts API endpoint
NWS_ALERTS_URL = "https://api.weather.gov/alerts/active"

# Airport coordinates for all US airports and territories
AIRPORT_COORDINATES: Dict[str, Tuple[float, float]] = {
    # Major Hubs
    "KATL": (33.6407, -84.4277), "KBOS": (42.3656, -71.0096), "KBWI": (39.1754, -76.6683),
    "KCLE": (41.4117, -81.8498), "KCLT": (35.2140, -80.9431), "KCVG": (39.0489, -84.6678),
    "KDCA": (38.8512, -77.0402), "KDEN": (39.8561, -104.6737), "KDFW": (32.8998, -97.0403),
    "KDTW": (42.2124, -83.3534), "KEWR": (40.6925, -74.1687), "KFLL": (26.0726, -80.1527),
    "KHOU": (29.6454, -95.2789), "KIAD": (38.9445, -77.4558), "KIAH": (29.9844, -95.3414),
    "KJFK": (40.6413, -73.7781), "KLAS": (36.0840, -115.1537), "KLAX": (33.9425, -118.4081),
    "KLGA": (40.7769, -73.8740), "KMCI": (39.2976, -94.7139), "KMCO": (28.4294, -81.3089),
    "KMDW": (41.7868, -87.7522), "KMEM": (35.0424, -89.9767), "KMIA": (25.7959, -80.2870),
    "KMKE": (42.9472, -87.8966), "KMSP": (44.8820, -93.2218), "KMSY": (29.9934, -90.2580),
    "KOAK": (37.7213, -122.2208), "KONT": (34.0560, -117.6012), "KORD": (41.9742, -87.9073),
    "KPBI": (26.6832, -80.0956), "KPDX": (45.5887, -122.5975), "KPHL": (39.8719, -75.2411),
    "KPHX": (33.4373, -112.0078), "KPIT": (40.4915, -80.2329), "KRDU": (35.8776, -78.7875),
    "KRSW": (26.5362, -81.7552), "KSAN": (32.7336, -117.1897), "KSAT": (29.5337, -98.4698),
    "KSDF": (38.1744, -85.7360), "KSEA": (47.4502, -122.3088), "KSFO": (37.6213, -122.3790),
    "KSJC": (37.3626, -121.9291), "KSLC": (40.7884, -111.9778), "KSMF": (38.6954, -121.5908),
    "KSNA": (33.6757, -117.8683), "KSTL": (38.7487, -90.3700), "KTPA": (27.9755, -82.5332),
    "KTUS": (32.1161, -110.9410),
    # Secondary/Regional
    "KABQ": (35.0402, -106.6092), "KAUS": (30.1945, -97.6699), "KBDL": (41.9389, -72.6832),
    "KBHM": (33.5629, -86.7535), "KBNA": (36.1245, -86.6782), "KBOI": (43.5644, -116.2228),
    "KBUF": (42.9405, -78.7322), "KBUR": (34.2007, -118.3585), "KCHS": (32.8986, -80.0405),
    "KCMH": (39.9980, -82.8919), "KCOS": (38.8058, -104.7009), "KDAL": (32.8471, -96.8518),
    "KDSM": (41.5340, -93.6631), "KELP": (31.8072, -106.3778), "KGSO": (36.0978, -79.9373),
    "KGRR": (42.8808, -85.5228), "KGSP": (34.8957, -82.2189), "KIND": (39.7173, -86.2944),
    "KJAX": (30.4941, -81.6879), "KLIT": (34.7294, -92.2243), "KMHT": (42.9326, -71.4357),
    "KOKC": (35.3931, -97.6007), "KOMA": (41.3032, -95.8940), "KORF": (36.8946, -76.2012),
    "KPVD": (41.7326, -71.4204), "KRIC": (37.5052, -77.3197), "KRNO": (39.4991, -119.7681),
    "KROC": (43.1189, -77.6724), "KSAV": (32.1276, -81.2021), "KSYR": (43.1112, -76.1063),
    "KTUL": (36.1984, -95.8881),
    # Missing States
    "KJAN": (32.3112, -90.0759), "KGPT": (30.4073, -89.0701), "KPWM": (43.6462, -70.3093),
    "KBGR": (44.8074, -68.8281), "KBIL": (45.8077, -108.5429), "KBZN": (45.7775, -111.1530),
    "KMSO": (46.9163, -114.0906), "KFAR": (46.9207, -96.8158), "KBIS": (46.7727, -100.7468),
    "KFSD": (43.5820, -96.7419), "KRAP": (44.0453, -103.0574), "KBTV": (44.4720, -73.1533),
    "KCRW": (38.3731, -81.5932), "KJAC": (43.6073, -110.7377), "KCPR": (42.9080, -106.4645),
    "KICT": (37.6499, -97.4331),
    # Additional Regional
    "KLEX": (38.0365, -84.6059), "KCHA": (35.0353, -85.2038), "KTYS": (35.8109, -83.9940),
    "KMOB": (30.6914, -88.2428), "KHSV": (34.6372, -86.7751), "KPNS": (30.4734, -87.1866),
    "KVPS": (30.4832, -86.5254), "KECP": (30.3571, -85.7954), "KDAY": (39.9024, -84.2194),
    "KCAK": (40.9161, -81.4422), "KFWA": (40.9785, -85.1951), "KSBN": (41.7087, -86.3173),
    "KLAN": (42.7787, -84.5874), "KFNT": (42.9655, -83.7436), "KAZO": (42.2350, -85.5521),
    "KMSN": (43.1399, -89.3375), "KGRB": (44.4851, -88.1296), "KSPI": (39.8441, -89.6779),
    "KMLI": (41.4485, -90.5075), "KPSP": (33.8297, -116.5070), "KFAT": (36.7762, -119.7181),
    "KSBP": (35.2368, -120.6424), "KGEG": (47.6199, -117.5338), "KBLI": (48.7927, -122.5375),
    "KEUG": (44.1246, -123.2119), "KMFR": (42.3742, -122.8735), "KFCA": (48.3105, -114.2560),
    "KIDA": (43.5146, -112.0702), "KTWF": (42.4818, -114.4878),
    # Pacific Territories
    "PHNL": (21.3187, -157.9225), "PHOG": (20.8986, -156.4305), "PHKO": (19.7388, -156.0456),
    "PHLI": (21.9760, -159.3390), "PANC": (61.1743, -149.9963), "PAFA": (64.8151, -147.8560),
    "PAJN": (58.3547, -134.5762), "PGUM": (13.4834, 144.7959), "PGSN": (15.1190, 145.7294),
    # Caribbean Territories
    "TJSJ": (18.4394, -66.0018), "TJBQ": (18.4949, -67.1294), "TJPS": (18.0083, -66.5630),
    "TIST": (18.3373, -64.9733), "TISX": (17.7019, -64.7986),
}


@dataclass
class WeatherAlert:
    """Parsed NWS weather alert."""
    id: str
    event: str  # e.g., "Severe Thunderstorm Warning"
    severity: str  # Minor, Moderate, Severe, Extreme
    certainty: str  # Observed, Likely, Possible, Unlikely
    urgency: str  # Immediate, Expected, Future, Past
    headline: str
    description: str
    instruction: Optional[str]
    effective: datetime
    expires: datetime
    sender: str
    areas_affected: List[str]
    retrieved_at: datetime
    raw_data: Dict[str, Any]


class NWSAlertsClient:
    """
    Client for NWS Alerts API.

    Fetches and parses weather alerts for geographic locations.
    """

    def __init__(self, timeout: float = 10.0):
        self.client = HttpClient(
            timeout=timeout,
            headers={"User-Agent": "ExceptionOS/1.0 (contact@example.com)"}
        )

    def fetch_alerts_for_point(
        self,
        lat: float,
        lon: float
    ) -> List[WeatherAlert]:
        """
        Fetch active alerts for geographic point.

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            List of active WeatherAlert objects
        """
        try:
            data = self.client.get_json(
                NWS_ALERTS_URL,
                params={"point": f"{lat},{lon}"}
            )
            retrieved_at = datetime.now(timezone.utc)

            features = data.get('features', [])
            alerts = []

            for feature in features:
                props = feature.get('properties', {})
                alerts.append(self._parse_alert(props, retrieved_at))

            return alerts

        except HttpClientError:
            raise
        except Exception as e:
            raise HttpClientError(f"Failed to parse NWS alerts: {e}")

    def fetch_alerts_for_airport(self, icao: str) -> List[WeatherAlert]:
        """
        Fetch active alerts for airport.

        Args:
            icao: ICAO airport code

        Returns:
            List of active WeatherAlert objects
        """
        coords = AIRPORT_COORDINATES.get(icao.upper())
        if not coords:
            # Return empty for unknown airports
            return []

        lat, lon = coords
        return self.fetch_alerts_for_point(lat, lon)

    def _parse_alert(self, props: Dict[str, Any], retrieved_at: datetime) -> WeatherAlert:
        """Parse alert from GeoJSON properties."""
        # Parse times
        effective_str = props.get('effective', '')
        expires_str = props.get('expires', '')

        try:
            effective = datetime.fromisoformat(effective_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            effective = retrieved_at

        try:
            expires = datetime.fromisoformat(expires_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            expires = effective

        # Extract affected areas
        areas = props.get('areaDesc', '')
        areas_list = [a.strip() for a in areas.split(';')] if areas else []

        return WeatherAlert(
            id=props.get('id', ''),
            event=props.get('event', ''),
            severity=props.get('severity', 'Unknown'),
            certainty=props.get('certainty', 'Unknown'),
            urgency=props.get('urgency', 'Unknown'),
            headline=props.get('headline', ''),
            description=props.get('description', ''),
            instruction=props.get('instruction'),
            effective=effective,
            expires=expires,
            sender=props.get('senderName', ''),
            areas_affected=areas_list,
            retrieved_at=retrieved_at,
            raw_data=props,
        )


def fetch_nws_alerts(icao: str, timeout: float = 10.0) -> List[WeatherAlert]:
    """
    Convenience function to fetch NWS alerts for airport.

    Args:
        icao: ICAO airport code
        timeout: Request timeout

    Returns:
        List of active WeatherAlert objects
    """
    client = NWSAlertsClient(timeout=timeout)
    return client.fetch_alerts_for_airport(icao)


def get_airport_coordinates(icao: str) -> Optional[Tuple[float, float]]:
    """
    Get coordinates for airport.

    Args:
        icao: ICAO airport code

    Returns:
        (lat, lon) tuple if known, None otherwise
    """
    return AIRPORT_COORDINATES.get(icao.upper())
