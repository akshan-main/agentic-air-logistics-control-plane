# app/ingestion/faa_nasstatus.py
"""
FAA National Airspace System (NAS) Status ingestion.

Source: https://nasstatus.faa.gov/api/airport-status-information

Returns airport status information including:
- Ground stops
- Ground delays
- Airport closures
- Reason codes (weather, volume, equipment, etc.)
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from .http import HttpClient, HttpClientError

# FAA NAS Status API endpoint
FAA_NAS_API_URL = "https://nasstatus.faa.gov/api/airport-status-information"


@dataclass
class AirportStatus:
    """Parsed airport status from FAA NAS."""
    icao: str
    name: str
    delay: bool
    delay_type: Optional[str]  # GROUND_STOP, GROUND_DELAY, etc.
    reason: Optional[str]
    avg_delay_minutes: Optional[int]
    closure: bool
    retrieved_at: datetime
    raw_data: Dict[str, Any]


class FAANASStatusClient:
    """
    Client for FAA NAS Status API.

    Fetches and parses airport status information.
    """

    def __init__(self, timeout: float = 10.0):
        self.client = HttpClient(timeout=timeout)

    def fetch_all_statuses(self) -> List[AirportStatus]:
        """
        Fetch status for all airports.

        Returns:
            List of AirportStatus objects
        """
        try:
            response = self.client.get(FAA_NAS_API_URL)
            retrieved_at = datetime.now(timezone.utc)

            # API returns XML
            if 'xml' in response.headers.get('content-type', '').lower():
                return self._parse_xml(response.text, retrieved_at)
            else:
                # Try JSON fallback
                return self._parse_json(response.json(), retrieved_at)

        except HttpClientError:
            raise
        except Exception as e:
            raise HttpClientError(f"Failed to parse FAA NAS response: {e}")

    def fetch_airport_status(self, icao: str) -> Optional[AirportStatus]:
        """
        Fetch status for specific airport.

        Args:
            icao: ICAO airport code (e.g., "KJFK")

        Returns:
            AirportStatus if found, None otherwise
        """
        all_statuses = self.fetch_all_statuses()
        for status in all_statuses:
            if status.icao.upper() == icao.upper():
                return status
        return None

    def _parse_xml(self, xml_text: str, retrieved_at: datetime) -> List[AirportStatus]:
        """Parse XML response from FAA NAS API."""
        statuses = []
        try:
            root = ET.fromstring(xml_text)

            # Find all airport elements (structure varies)
            for airport in root.findall('.//Airport'):
                icao = airport.findtext('ICAO', '')
                name = airport.findtext('Name', '')

                # Check for delays
                delay_elem = airport.find('.//Delay')
                has_delay = delay_elem is not None

                delay_type = None
                reason = None
                avg_delay = None

                if delay_elem is not None:
                    delay_type = delay_elem.findtext('Type')
                    reason = delay_elem.findtext('Reason')
                    try:
                        avg_delay = int(delay_elem.findtext('AvgDelay', '0'))
                    except ValueError:
                        avg_delay = None

                # Check for closure
                closure = airport.findtext('Closure', 'false').lower() == 'true'

                statuses.append(AirportStatus(
                    icao=icao,
                    name=name,
                    delay=has_delay,
                    delay_type=delay_type,
                    reason=reason,
                    avg_delay_minutes=avg_delay,
                    closure=closure,
                    retrieved_at=retrieved_at,
                    raw_data={'xml': xml_text[:1000]},  # Truncated for storage
                ))

        except ET.ParseError as e:
            raise HttpClientError(f"Failed to parse FAA XML: {e}")

        return statuses

    def _parse_json(self, data: Any, retrieved_at: datetime) -> List[AirportStatus]:
        """Parse JSON response from FAA NAS API."""
        statuses = []

        # Handle various JSON structures
        airports = data if isinstance(data, list) else data.get('airports', [])

        for airport in airports:
            icao = airport.get('ICAO', airport.get('icao', ''))
            name = airport.get('name', '')

            delay_info = airport.get('delay', {})
            has_delay = bool(delay_info) or airport.get('hasDelay', False)

            statuses.append(AirportStatus(
                icao=icao,
                name=name,
                delay=has_delay,
                delay_type=delay_info.get('type') if isinstance(delay_info, dict) else None,
                reason=delay_info.get('reason') if isinstance(delay_info, dict) else None,
                avg_delay_minutes=delay_info.get('avgDelay') if isinstance(delay_info, dict) else None,
                closure=airport.get('closure', False),
                retrieved_at=retrieved_at,
                raw_data=airport,
            ))

        return statuses


def fetch_faa_status(icao: str, timeout: float = 10.0) -> Optional[AirportStatus]:
    """
    Convenience function to fetch FAA status for an airport.

    Args:
        icao: ICAO airport code
        timeout: Request timeout

    Returns:
        AirportStatus if found
    """
    client = FAANASStatusClient(timeout=timeout)
    return client.fetch_airport_status(icao)
