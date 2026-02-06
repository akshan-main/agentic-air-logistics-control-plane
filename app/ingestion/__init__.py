# Ingestion module - real data sources for disruption signals
from .http import HttpClient, fetch_with_retry
from .faa_nasstatus import FAANASStatusClient, fetch_faa_status
from .aviationweather import AviationWeatherClient, fetch_metar, fetch_taf
from .nws_alerts import NWSAlertsClient, fetch_nws_alerts
from .opensky import OpenSkyClient, fetch_opensky
from .registry import IngestionRegistry, get_registry

__all__ = [
    "HttpClient",
    "fetch_with_retry",
    "FAANASStatusClient",
    "fetch_faa_status",
    "AviationWeatherClient",
    "fetch_metar",
    "fetch_taf",
    "NWSAlertsClient",
    "fetch_nws_alerts",
    "OpenSkyClient",
    "fetch_opensky",
    "IngestionRegistry",
    "get_registry",
]
