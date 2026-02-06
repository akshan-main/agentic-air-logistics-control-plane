# Data Sources Architecture

## Coverage

**US airports and territories only** - ICAO prefixes:
- **K*** : Continental US (e.g., KJFK, KLAX)
- **P*** : Pacific territories (Alaska, Hawaii, Guam, Saipan - e.g., PHNL, PANC)
- **TJ*** : Puerto Rico (e.g., TJSJ)
- **TI*** : US Virgin Islands (e.g., TIST)

---

## Implemented Sources

### 1. FAA NAS Status
- **Endpoint**: `https://nasstatus.faa.gov/api/airport-status-information`
- **Data**: Ground stops, ground delays, airport closures, reason codes
- **Format**: XML
- **Auth**: None (public)
- **Rate Limit**: None known

### 2. NWS Weather Alerts
- **Endpoint**: `https://api.weather.gov/alerts/active`
- **Data**: Severe weather warnings, watches, advisories
- **Format**: GeoJSON
- **Auth**: None (public)
- **Rate Limit**: Generous

### 3. Aviation Weather METAR
- **Endpoint**: `https://aviationweather.gov/api/data/metar`
- **Data**: Current weather observations
- **Format**: JSON
- **Auth**: None (public)
- **Rate Limit**: None known

### 4. Aviation Weather TAF
- **Endpoint**: `https://aviationweather.gov/api/data/taf`
- **Data**: Weather forecasts (24-30 hours)
- **Format**: JSON
- **Auth**: None (public)
- **Rate Limit**: None known

### 5. OpenSky ADS-B
- **Endpoint**: `https://opensky-network.org/api/states/all`
- **Data**: Aircraft positions, movement data
- **Format**: JSON
- **Auth**: Optional (anonymous has lower rate limits)
- **Rate Limit**: 10 req/s anonymous, higher with account

---

## Data Flow

```
External APIs           Ingestion Layer           Graph/Evidence
─────────────────────────────────────────────────────────────────

FAA NAS ─────┐
             │
NWS Alerts ──┤
             ├──→ IngestionRegistry ──→ Evidence Store
METAR/TAF ───┤                        (raw payloads)
             │                              │
OpenSky ─────┘                              ▼
                                      Signal Derivation
                                            │
                                           ▼
                                    Graph Edges/Claims
                                    (bi-temporal, evidence-bound)
```

---

## Example Usage

```bash
# Ingest JFK airport (continental US)
curl -X POST http://localhost:8000/ingest/airport/KJFK

# Ingest LAX airport (continental US)
curl -X POST http://localhost:8000/ingest/airport/KLAX

# Ingest Honolulu (Pacific - Hawaii)
curl -X POST http://localhost:8000/ingest/airport/PHNL

# Ingest San Juan (Puerto Rico)
curl -X POST http://localhost:8000/ingest/airport/TJSJ

# Ingest St. Thomas (US Virgin Islands)
curl -X POST http://localhost:8000/ingest/airport/TIST

# Non-US airports will return 400 error
curl -X POST http://localhost:8000/ingest/airport/EGLL  # London - rejected
```

---

## Major US Airports

| ICAO | Name | City |
|------|------|------|
| KJFK | John F. Kennedy | New York |
| KLAX | Los Angeles Intl | Los Angeles |
| KORD | O'Hare | Chicago |
| KATL | Hartsfield-Jackson | Atlanta |
| KDFW | Dallas/Fort Worth | Dallas |
| KDEN | Denver Intl | Denver |
| KSFO | San Francisco Intl | San Francisco |
| KSEA | Seattle-Tacoma | Seattle |
| KMIA | Miami Intl | Miami |
| KBOS | Logan | Boston |
| KEWR | Newark | Newark |
| KPHX | Phoenix Sky Harbor | Phoenix |
| KIAH | George Bush | Houston |
| KMSP | Minneapolis-St Paul | Minneapolis |
| KDTW | Detroit Metro | Detroit |
| PHNL | Daniel K. Inouye | Honolulu |
| PANC | Ted Stevens | Anchorage |
| TJSJ | Luis Muñoz Marín | San Juan, PR |
| TIST | Cyril E. King | Charlotte Amalie, USVI |
