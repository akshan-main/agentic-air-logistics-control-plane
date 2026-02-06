# app/signals/weather_risk.py
"""
Weather data extraction from METAR/TAF.

NOTE: This module only EXTRACTS and STRUCTURES data.
It does NOT assign severity or make decisions - that's the LLM's job.
"""

from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from ..ingestion.aviationweather import MetarObservation, TafForecast


@dataclass
class WeatherSignal:
    """Extracted weather signal - raw facts only."""
    airport_icao: str
    flight_category: Optional[str]  # VFR, MVFR, IFR, LIFR
    wind_speed: Optional[int]  # knots
    wind_gust: Optional[int]  # knots
    wind_direction: Optional[int]  # degrees
    visibility_miles: Optional[float]
    ceiling_feet: Optional[int]
    ceiling_type: Optional[str]
    weather_phenomena: List[str]  # TS, RA, SN, FG, etc.
    temp_c: Optional[float]
    dewpoint_c: Optional[float]
    raw_metar: Optional[str]
    attrs: Dict[str, Any]


def derive_weather_signal(
    metar: MetarObservation,
    taf: Optional[TafForecast] = None,
) -> Optional[WeatherSignal]:
    """
    Extract weather signal from METAR observation.

    This is pure data extraction - no severity assignment.
    The LLM (RiskQuantAgent) reasons about what this means.

    Args:
        metar: Current METAR observation
        taf: Optional TAF forecast

    Returns:
        WeatherSignal with raw facts
    """
    if not metar:
        return None

    return WeatherSignal(
        airport_icao=metar.icao,
        flight_category=metar.flight_category,
        wind_speed=metar.wind_speed,
        wind_gust=metar.wind_gust,
        wind_direction=metar.wind_direction,
        visibility_miles=metar.visibility_miles,
        ceiling_feet=metar.ceiling_feet,
        ceiling_type=metar.ceiling_type,
        weather_phenomena=metar.weather or [],
        temp_c=metar.temp_c,
        dewpoint_c=metar.dewpoint_c,
        raw_metar=metar.raw_text,
        attrs={
            "observation_time": metar.observation_time.isoformat() if metar.observation_time else None,
            "raw_text": metar.raw_text,
            "flight_category": metar.flight_category,
            "wind_direction": metar.wind_direction,
            "wind_speed": metar.wind_speed,
            "wind_gust": metar.wind_gust,
            "visibility_miles": metar.visibility_miles,
            "ceiling_feet": metar.ceiling_feet,
            "ceiling_type": metar.ceiling_type,
            "weather": metar.weather,
            "temp_c": metar.temp_c,
            "dewpoint_c": metar.dewpoint_c,
        },
    )


def weather_to_edge_attrs(signal: WeatherSignal) -> Dict[str, Any]:
    """Convert weather signal to edge attributes."""
    return {
        "flight_category": signal.flight_category,
        "wind_speed": signal.wind_speed,
        "wind_gust": signal.wind_gust,
        "wind_direction": signal.wind_direction,
        "visibility_miles": signal.visibility_miles,
        "ceiling_feet": signal.ceiling_feet,
        "ceiling_type": signal.ceiling_type,
        "weather_phenomena": signal.weather_phenomena,
        **signal.attrs,
    }


# For backward compatibility - these are now just data labels, not severity assessments
# The LLM interprets what these mean
FLIGHT_CATEGORY_LABELS = {
    "VFR": "Visual Flight Rules - good visibility",
    "MVFR": "Marginal VFR - reduced visibility",
    "IFR": "Instrument Flight Rules - low visibility, clouds",
    "LIFR": "Low IFR - very low visibility and/or ceiling",
}
