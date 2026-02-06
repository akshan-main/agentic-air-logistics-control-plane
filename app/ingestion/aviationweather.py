# app/ingestion/aviationweather.py
"""
AviationWeather.gov METAR/TAF ingestion.

Sources:
- METAR: https://aviationweather.gov/api/data/metar?ids={icao}&format=json
- TAF: https://aviationweather.gov/api/data/taf?ids={icao}&format=json

Returns:
- METAR: Current conditions (wind, visibility, ceiling, weather phenomena)
- TAF: Terminal Aerodrome Forecast (predicted conditions)
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from .http import HttpClient, HttpClientError

# AviationWeather API endpoints
METAR_URL = "https://aviationweather.gov/api/data/metar"
TAF_URL = "https://aviationweather.gov/api/data/taf"


@dataclass
class MetarObservation:
    """Parsed METAR observation."""
    icao: str
    observation_time: datetime
    raw_text: str
    # Wind
    wind_direction: Optional[int]  # degrees
    wind_speed: Optional[int]  # knots
    wind_gust: Optional[int]  # knots
    # Visibility
    visibility_miles: Optional[float]
    # Ceiling
    ceiling_feet: Optional[int]
    ceiling_type: Optional[str]  # BKN, OVC, etc.
    # Weather phenomena
    weather: List[str]  # RA, SN, FG, etc.
    # Flight category
    flight_category: Optional[str]  # VFR, MVFR, IFR, LIFR
    # Temperature
    temp_c: Optional[float]
    dewpoint_c: Optional[float]
    # Pressure
    altimeter_inhg: Optional[float]
    # Metadata
    retrieved_at: datetime
    raw_data: Dict[str, Any]


@dataclass
class TafForecast:
    """Parsed TAF forecast."""
    icao: str
    issue_time: datetime
    valid_from: datetime
    valid_to: datetime
    raw_text: str
    forecast_periods: List[Dict[str, Any]]
    retrieved_at: datetime
    raw_data: Dict[str, Any]


class AviationWeatherClient:
    """
    Client for AviationWeather.gov API.

    Fetches and parses METAR and TAF data.
    """

    def __init__(self, timeout: float = 10.0):
        self.client = HttpClient(timeout=timeout)

    def fetch_metar(self, icao: str) -> Optional[MetarObservation]:
        """
        Fetch current METAR for airport.

        Args:
            icao: ICAO airport code

        Returns:
            MetarObservation if available
        """
        try:
            data = self.client.get_json(
                METAR_URL,
                params={"ids": icao.upper(), "format": "json"}
            )
            retrieved_at = datetime.now(timezone.utc)

            if not data:
                return None

            # API returns list of observations
            observations = data if isinstance(data, list) else [data]
            if not observations:
                return None

            obs = observations[0]
            return self._parse_metar(obs, retrieved_at)

        except HttpClientError:
            raise
        except Exception as e:
            raise HttpClientError(f"Failed to parse METAR: {e}")

    def fetch_taf(self, icao: str) -> Optional[TafForecast]:
        """
        Fetch current TAF for airport.

        Args:
            icao: ICAO airport code

        Returns:
            TafForecast if available
        """
        try:
            data = self.client.get_json(
                TAF_URL,
                params={"ids": icao.upper(), "format": "json"}
            )
            retrieved_at = datetime.now(timezone.utc)

            if not data:
                return None

            forecasts = data if isinstance(data, list) else [data]
            if not forecasts:
                return None

            taf = forecasts[0]
            return self._parse_taf(taf, retrieved_at)

        except HttpClientError:
            raise
        except Exception as e:
            raise HttpClientError(f"Failed to parse TAF: {e}")

    def _parse_metar(self, obs: Dict[str, Any], retrieved_at: datetime) -> MetarObservation:
        """Parse METAR observation from JSON."""
        # Parse observation time
        obs_time_str = obs.get('obsTime', obs.get('observation_time', ''))
        try:
            obs_time = datetime.fromisoformat(obs_time_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            obs_time = retrieved_at

        # Extract ceiling from cloud layers
        ceiling_feet = None
        ceiling_type = None
        clouds = obs.get('clouds', [])
        for cloud in clouds:
            cover = cloud.get('cover', '')
            if cover in ('BKN', 'OVC', 'VV'):
                base = cloud.get('base')
                if base is not None:
                    if ceiling_feet is None or base < ceiling_feet:
                        ceiling_feet = base
                        ceiling_type = cover

        # Extract weather phenomena
        weather = []
        wx = obs.get('wxString', obs.get('weather', ''))
        if wx:
            weather = wx.split() if isinstance(wx, str) else wx

        return MetarObservation(
            icao=obs.get('icaoId', obs.get('station_id', '')),
            observation_time=obs_time,
            raw_text=obs.get('rawOb', obs.get('raw_text', '')),
            wind_direction=obs.get('wdir'),
            wind_speed=obs.get('wspd'),
            wind_gust=obs.get('wgst'),
            visibility_miles=obs.get('visib'),
            ceiling_feet=ceiling_feet,
            ceiling_type=ceiling_type,
            weather=weather,
            flight_category=obs.get('fltcat', obs.get('flight_category')),
            temp_c=obs.get('temp'),
            dewpoint_c=obs.get('dewp'),
            altimeter_inhg=obs.get('altim'),
            retrieved_at=retrieved_at,
            raw_data=obs,
        )

    def _parse_taf(self, taf: Dict[str, Any], retrieved_at: datetime) -> TafForecast:
        """Parse TAF forecast from JSON."""
        # Parse issue time
        issue_str = taf.get('issueTime', taf.get('issue_time', ''))
        try:
            issue_time = datetime.fromisoformat(issue_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            issue_time = retrieved_at

        # Parse valid times
        valid_from_str = taf.get('validTimeFrom', taf.get('valid_time_from', ''))
        valid_to_str = taf.get('validTimeTo', taf.get('valid_time_to', ''))

        try:
            valid_from = datetime.fromisoformat(valid_from_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            valid_from = issue_time

        try:
            valid_to = datetime.fromisoformat(valid_to_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            valid_to = valid_from

        # Extract forecast periods
        forecast_periods = taf.get('forecast', [])
        if not isinstance(forecast_periods, list):
            forecast_periods = []

        return TafForecast(
            icao=taf.get('icaoId', taf.get('station_id', '')),
            issue_time=issue_time,
            valid_from=valid_from,
            valid_to=valid_to,
            raw_text=taf.get('rawTAF', taf.get('raw_text', '')),
            forecast_periods=forecast_periods,
            retrieved_at=retrieved_at,
            raw_data=taf,
        )


def fetch_metar(icao: str, timeout: float = 10.0) -> Optional[MetarObservation]:
    """
    Convenience function to fetch METAR.

    Args:
        icao: ICAO airport code
        timeout: Request timeout

    Returns:
        MetarObservation if available
    """
    client = AviationWeatherClient(timeout=timeout)
    return client.fetch_metar(icao)


def fetch_taf(icao: str, timeout: float = 10.0) -> Optional[TafForecast]:
    """
    Convenience function to fetch TAF.

    Args:
        icao: ICAO airport code
        timeout: Request timeout

    Returns:
        TafForecast if available
    """
    client = AviationWeatherClient(timeout=timeout)
    return client.fetch_taf(icao)
