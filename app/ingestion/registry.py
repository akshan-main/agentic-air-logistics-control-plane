# app/ingestion/registry.py
"""
Ingestion registry for coordinating data sources.

Provides a unified interface for ingesting signals from all sources
for a given airport or case.

Coverage: US airports and territories
- K*  : Continental US
- P*  : Pacific (Alaska, Hawaii, Guam, Saipan)
- TJ* : Puerto Rico
- TI* : US Virgin Islands

Sources: FAA NAS, NWS Alerts, METAR, TAF, OpenSky
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, List, Optional
from dataclasses import dataclass, field

from .faa_nasstatus import FAANASStatusClient
from .aviationweather import AviationWeatherClient
from .nws_alerts import NWSAlertsClient
from .opensky import OpenSkyClient, MissingEvidenceRequest


@dataclass
class IngestionResult:
    """Result of ingestion for a single source."""
    source: str
    success: bool
    data: Any
    error: Optional[str] = None
    missing_evidence: Optional[MissingEvidenceRequest] = None
    retrieved_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class AirportIngestionResult:
    """Combined ingestion result for all sources for a US airport."""
    icao: str
    faa_status: Optional[IngestionResult] = None
    metar: Optional[IngestionResult] = None
    taf: Optional[IngestionResult] = None
    nws_alerts: Optional[IngestionResult] = None
    opensky: Optional[IngestionResult] = None
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def all_results(self) -> List[IngestionResult]:
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
    def missing_evidence_requests(self) -> List[MissingEvidenceRequest]:
        """Get all MissingEvidenceRequests from failed ingestions."""
        return [
            r.missing_evidence
            for r in self.all_results
            if r.missing_evidence is not None
        ]


class IngestionRegistry:
    """
    Coordinates ingestion from all data sources for US airports.

    Coverage: US airports (K*, P* ICAO prefixes)
    Sources: FAA NAS, NWS Alerts, METAR, TAF, OpenSky

    Usage:
        registry = IngestionRegistry()
        result = registry.ingest_airport("KJFK", case_id="...")
    """

    def __init__(self, timeout: float = 10.0):
        """
        Initialize registry with clients for all sources.

        Args:
            timeout: Default timeout for all sources
        """
        self.faa_client = FAANASStatusClient(timeout=timeout)
        self.weather_client = AviationWeatherClient(timeout=timeout)
        self.nws_client = NWSAlertsClient(timeout=timeout)
        self.opensky_client = OpenSkyClient(timeout=timeout)

    def _is_us_airport(self, icao: str) -> bool:
        """
        Check if airport is in US or US territories.

        Prefixes:
        - K*  : Continental US
        - P*  : Pacific (Alaska, Hawaii, Guam, Saipan)
        - TJ* : Puerto Rico
        - TI* : US Virgin Islands
        """
        icao_upper = icao.upper()
        prefix1 = icao_upper[:1]
        prefix2 = icao_upper[:2]

        # Single-letter prefixes: K (continental), P (Pacific)
        if prefix1 in ('K', 'P'):
            return True

        # Two-letter prefixes: TJ (Puerto Rico), TI (Virgin Islands)
        if prefix2 in ('TJ', 'TI'):
            return True

        return False

    def ingest_airport(
        self,
        icao: str,
        case_id: Optional[str] = None,
        include_opensky: bool = True,
    ) -> AirportIngestionResult:
        """
        Ingest all signals for a US airport.

        Sources: FAA NAS, NWS Alerts, METAR, TAF, OpenSky

        Args:
            icao: ICAO airport code (US: K*, P*, TJ*, TI*)
            case_id: Optional case ID for tracking
            include_opensky: Whether to include OpenSky (can be slow/rate-limited)

        Returns:
            AirportIngestionResult with all source results

        Raises:
            ValueError: If airport is not a US airport
        """
        icao = icao.upper()

        if not self._is_us_airport(icao):
            raise ValueError(f"Only US airports supported (K*, P*, TJ*, TI*). Got: {icao}")

        result = AirportIngestionResult(icao=icao)

        # Fetch all sources in parallel using ThreadPoolExecutor.
        # Each source uses its own httpx client, so this is thread-safe.
        # Typical speedup: 5 serial calls (~5-10s) -> parallel (~2-3s).
        futures = {}
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures["faa_status"] = executor.submit(self._ingest_faa, icao)
            futures["nws_alerts"] = executor.submit(self._ingest_nws, icao)
            futures["metar"] = executor.submit(self._ingest_metar, icao)
            futures["taf"] = executor.submit(self._ingest_taf, icao)
            if include_opensky:
                futures["opensky"] = executor.submit(self._ingest_opensky, icao, case_id)

            for key, future in futures.items():
                try:
                    setattr(result, key, future.result(timeout=30))
                except Exception as e:
                    # If a source throws unexpectedly, wrap it in a failed result
                    setattr(result, key, IngestionResult(
                        source=key.upper(),
                        success=False,
                        data=None,
                        error=f"Parallel fetch error: {str(e)}",
                    ))

        return result

    def _ingest_faa(self, icao: str) -> IngestionResult:
        """Ingest FAA NAS status. Only called for US airports."""
        try:
            status = self.faa_client.fetch_airport_status(icao)
            return IngestionResult(
                source="FAA_NAS",
                success=True,
                data=status,  # None means no disruptions reported
            )
        except Exception as e:
            return IngestionResult(
                source="FAA_NAS",
                success=False,
                data=None,
                error=str(e),
            )

    def _ingest_metar(self, icao: str) -> IngestionResult:
        """Ingest METAR observation."""
        try:
            metar = self.weather_client.fetch_metar(icao)
            return IngestionResult(
                source="METAR",
                success=True,
                data=metar,
            )
        except Exception as e:
            return IngestionResult(
                source="METAR",
                success=False,
                data=None,
                error=str(e),
            )

    def _ingest_taf(self, icao: str) -> IngestionResult:
        """Ingest TAF forecast."""
        try:
            taf = self.weather_client.fetch_taf(icao)
            return IngestionResult(
                source="TAF",
                success=True,
                data=taf,
            )
        except Exception as e:
            return IngestionResult(
                source="TAF",
                success=False,
                data=None,
                error=str(e),
            )

    def _ingest_nws(self, icao: str) -> IngestionResult:
        """Ingest NWS alerts. Only called for US airports."""
        try:
            alerts = self.nws_client.fetch_alerts_for_airport(icao)
            return IngestionResult(
                source="NWS_ALERTS",
                success=True,
                data=alerts,
            )
        except Exception as e:
            return IngestionResult(
                source="NWS_ALERTS",
                success=False,
                data=None,
                error=str(e),
            )

    def _ingest_opensky(self, icao: str, case_id: Optional[str]) -> IngestionResult:
        """Ingest OpenSky aircraft states."""
        response = self.opensky_client.fetch_states_for_airport(icao, case_id)
        missing = self.opensky_client.last_missing_evidence

        if response is not None:
            return IngestionResult(
                source="OPENSKY",
                success=True,
                data=response,
            )
        else:
            return IngestionResult(
                source="OPENSKY",
                success=False,
                data=None,
                error=missing.reason if missing else "Unknown error",
                missing_evidence=missing,
            )


# Singleton instance
_registry: Optional[IngestionRegistry] = None


def get_registry(timeout: float = 10.0) -> IngestionRegistry:
    """
    Get or create singleton IngestionRegistry.

    Args:
        timeout: Default timeout (only used on first call)

    Returns:
        IngestionRegistry instance
    """
    global _registry
    if _registry is None:
        _registry = IngestionRegistry(timeout=timeout)
    return _registry
