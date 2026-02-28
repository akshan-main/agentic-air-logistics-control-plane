"""
Extract decision packets from Postgres → load to Snowflake audit warehouse.

Usage:
    python -m audit_warehouse.load extract   # Postgres → staging JSONL
    python -m audit_warehouse.load load      # staging JSONL → Snowflake
    python -m audit_warehouse.load run       # extract + load in one step
    python -m audit_warehouse.load verify    # run data quality checks on Snowflake
    python -m audit_warehouse.load preflight # check Postgres + Snowflake connectivity
"""

import json
import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from uuid import uuid4

import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
STAGING_DIR = Path(os.getenv("STAGING_DIR", str(REPO_ROOT / "audit_warehouse" / "staging")))
SQL_DIR = REPO_ROOT / "audit_warehouse" / "sql"

PACKETS_JSONL = STAGING_DIR / "packets.jsonl"
SHIPMENTS_JSONL = STAGING_DIR / "shipments.jsonl"
EVIDENCE_JSONL = STAGING_DIR / "evidence.jsonl"
DETAILS_JSONL = STAGING_DIR / "details.jsonl"

# Required fields for a valid decision packet row
REQUIRED_PACKET_FIELDS = {"case_id", "posture", "rationale_text"}

# Snowflake connection timeouts (seconds)
SF_LOGIN_TIMEOUT = 30
SF_NETWORK_TIMEOUT = 60

# Structured run log directory
LOGS_DIR = REPO_ROOT / "audit_warehouse" / "logs"


# ---------------------------------------------------------------------------
# Structured pipeline run log
# ---------------------------------------------------------------------------

class PipelineRunLog:
    """
    Structured JSON log for a single pipeline run.

    Writes one JSON line per stage to audit_warehouse/logs/run_<id>.jsonl.
    Provides observability without requiring external monitoring.
    """

    def __init__(self):
        self.run_id = str(uuid4())[:8]
        self.started_at = datetime.now(timezone.utc)
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        self.log_path = LOGS_DIR / f"run_{self.run_id}.jsonl"
        self._entries: List[Dict[str, Any]] = []

    def log_stage(self, stage: str, status: str, metrics: Dict[str, Any]):
        """Log a pipeline stage with structured metrics."""
        entry = {
            "run_id": self.run_id,
            "stage": stage,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **metrics,
        }
        self._entries.append(entry)
        with open(self.log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
        logger.info("Pipeline [%s] %s: %s %s", self.run_id, stage, status, metrics)

    def summary(self) -> Dict[str, Any]:
        """Return a summary of the run."""
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "stages": len(self._entries),
            "all_passed": all(e["status"] == "OK" for e in self._entries),
            "log_file": str(self.log_path),
        }


# ---------------------------------------------------------------------------
# Snowflake connection helper (with timeouts)
# ---------------------------------------------------------------------------

def get_snowflake_conn() -> snowflake.connector.SnowflakeConnection:
    """Connect to Snowflake using env vars with timeout protection."""
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(f"Missing required Snowflake env vars: {', '.join(missing)}")

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "AALCP_CORTEX_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "AALCP_DB"),
        login_timeout=SF_LOGIN_TIMEOUT,
        network_timeout=SF_NETWORK_TIMEOUT,
    )


def get_watermark(sf_conn: snowflake.connector.SnowflakeConnection, source: str) -> datetime:
    """Read the last-loaded watermark from RAW.LOAD_STATE."""
    cur = sf_conn.cursor()
    try:
        cur.execute(
            "SELECT watermark FROM RAW.LOAD_STATE WHERE source = %s",
            (source,),
        )
        row = cur.fetchone()
        if row:
            return row[0] if isinstance(row[0], datetime) else datetime.fromisoformat(str(row[0]))
        # No row → epoch (load everything)
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    finally:
        cur.close()


def update_watermark(sf_conn: snowflake.connector.SnowflakeConnection, source: str, watermark: datetime):
    """Update the watermark after a successful load."""
    cur = sf_conn.cursor()
    try:
        cur.execute(
            """
            MERGE INTO RAW.LOAD_STATE AS t
            USING (SELECT %s AS source, %s::TIMESTAMP_TZ AS watermark) AS s
            ON t.source = s.source
            WHEN MATCHED THEN UPDATE SET t.watermark = s.watermark, t.updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (source, watermark) VALUES (s.source, s.watermark)
            """,
            (source, watermark.isoformat()),
        )
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# PRE-FLIGHT: verify connectivity before pipeline runs
# ---------------------------------------------------------------------------

def preflight() -> Tuple[bool, List[str]]:
    """
    Verify Postgres and Snowflake are reachable before running the pipeline.

    Returns:
        (all_ok, list_of_issues)
    """
    issues = []

    # Check Postgres
    try:
        sys.path.insert(0, str(REPO_ROOT))
        from sqlalchemy import text
        from app.db.engine import SessionLocal

        session = SessionLocal()
        try:
            result = session.execute(text('SELECT COUNT(*) FROM "case" WHERE status = \'RESOLVED\''))
            count = result.scalar()
            logger.info("Postgres OK: %d resolved cases available", count)
            if count == 0:
                issues.append("Postgres: 0 resolved cases — run a simulation first")
        except Exception as e:
            issues.append(f"Postgres query failed: {e}")
        finally:
            session.close()
    except Exception as e:
        issues.append(f"Postgres connection failed: {e}")

    # Check Snowflake
    try:
        sf_conn = get_snowflake_conn()
        cur = sf_conn.cursor()
        try:
            cur.execute("SELECT CURRENT_TIMESTAMP()")
            ts = cur.fetchone()[0]
            logger.info("Snowflake OK: server time %s", ts)

            # Verify schema exists
            cur.execute("SHOW SCHEMAS IN DATABASE AALCP_DB")
            schemas = {row[1] for row in cur.fetchall()}
            for required_schema in ["RAW", "GOLD", "SERVICES"]:
                if required_schema not in schemas:
                    issues.append(f"Snowflake: schema {required_schema} missing — run 01_schema.sql")
        except Exception as e:
            issues.append(f"Snowflake query failed: {e}")
        finally:
            cur.close()
            sf_conn.close()
    except EnvironmentError as e:
        issues.append(f"Snowflake config: {e}")
    except Exception as e:
        issues.append(f"Snowflake connection failed: {e}")

    all_ok = len(issues) == 0
    if all_ok:
        logger.info("Pre-flight checks PASSED")
    else:
        for issue in issues:
            logger.error("Pre-flight FAIL: %s", issue)

    return all_ok, issues


# ---------------------------------------------------------------------------
# VALIDATION: check extracted data before writing to JSONL
# ---------------------------------------------------------------------------

def _validate_packet(flat: Dict[str, Any], case_id: str) -> List[str]:
    """
    Validate a flattened packet has required fields.

    Returns list of warnings (empty = valid).
    """
    warnings = []

    if not flat.get("case_id"):
        warnings.append(f"case {case_id}: missing case_id in flattened packet")

    if not flat.get("posture"):
        warnings.append(f"case {case_id}: missing posture")

    if not flat.get("rationale_text"):
        warnings.append(f"case {case_id}: empty rationale_text (will be NULL in Cortex Search)")

    if not flat.get("airport"):
        warnings.append(f"case {case_id}: missing airport")

    return warnings


# ---------------------------------------------------------------------------
# EXTRACT: Postgres → staging JSONL
# ---------------------------------------------------------------------------

def _flatten_packet(packet: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a decision packet dict into the RAW.DECISION_PACKETS shape."""
    posture_decision = packet.get("posture_decision") or {}
    scope = packet.get("scope") or {}
    metrics = packet.get("metrics") or {}

    # Rationale text
    rationale = posture_decision.get("reason", "")

    # Contradictions text
    contradictions = packet.get("contradictions") or []
    contradictions_text = "; ".join(
        f"{c.get('contradiction_type', 'UNKNOWN')}: {c.get('resolution_status', '')}"
        for c in contradictions
    ) if contradictions else ""

    # Policies text
    policies = packet.get("policies_applied") or []
    policies_text = "; ".join(
        f"{p.get('policy_text', '')}: {p.get('effect', '')}"
        for p in policies
    ) if policies else ""

    # Actions text
    actions = packet.get("actions_proposed") or []
    actions_text = "; ".join(
        f"{a.get('action_type', '')} ({a.get('state', '')})"
        for a in actions
    ) if actions else ""

    # Cascade text - flattened operational impact summary
    cascade = packet.get("cascade_impact") or {}
    cascade_text = _build_cascade_text(cascade)

    return {
        "case_id": packet.get("case_id"),
        "airport": scope.get("airport"),
        "scenario_id": scope.get("scenario_id"),
        "posture": posture_decision.get("posture"),
        "rationale_text": rationale,
        "contradictions_text": contradictions_text,
        "policies_text": policies_text,
        "actions_text": actions_text,
        "cascade_text": cascade_text,
        "metrics_variant": metrics,
        "created_at": packet.get("created_at"),
        "raw_packet_variant": packet,
    }


def _build_cascade_text(cascade: Dict[str, Any]) -> str:
    """Build a searchable text summary from cascade_impact."""
    if not cascade or cascade.get("error"):
        return ""

    parts = []
    summary = cascade.get("summary") or {}

    flights = summary.get("total_flights", 0)
    if flights:
        parts.append(f"{flights} flights affected")

    shipments_count = summary.get("total_shipments", 0)
    if shipments_count:
        parts.append(f"{shipments_count} shipments")

    bookings = summary.get("total_bookings", 0)
    if bookings:
        parts.append(f"{bookings} bookings")

    revenue = summary.get("total_revenue_usd", 0)
    if revenue:
        parts.append(f"${revenue:,.0f} revenue at risk")

    weight = summary.get("total_weight_kg", 0)
    if weight:
        parts.append(f"{weight:,.0f} kg cargo")

    breaches = summary.get("sla_breaches_imminent", 0)
    if breaches:
        parts.append(f"{breaches} SLA breaches imminent")

    # Commodities from shipments
    shipments = cascade.get("shipments") or []
    commodities = sorted({s.get("commodity", "") for s in shipments if s.get("commodity")})
    if commodities:
        parts.append(f"Commodities: {', '.join(commodities)}")

    # Service levels
    service_levels = sorted({s.get("service_level", "") for s in shipments if s.get("service_level")})
    if service_levels:
        parts.append(f"Services: {', '.join(service_levels)}")

    # Carriers
    carriers = cascade.get("carriers") or []
    carrier_names = [c.get("name", "") for c in carriers if c.get("name")]
    if carrier_names:
        parts.append(f"Carriers: {', '.join(carrier_names)}")

    return ". ".join(parts) + "." if parts else ""


def _extract_shipments(packet: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Explode cascade_impact.shipments into individual rows."""
    cascade = packet.get("cascade_impact") or {}
    shipments = cascade.get("shipments") or []
    scope = packet.get("scope") or {}
    case_id = packet.get("case_id")
    airport = scope.get("airport")
    created_at = packet.get("created_at")

    rows = []
    for s in shipments:
        rows.append({
            "case_id": case_id,
            "airport": airport,
            "tracking_number": s.get("tracking_number", "UNKNOWN"),
            "commodity": s.get("commodity"),
            "weight_kg": s.get("weight_kg"),
            "service_level": s.get("service_level"),
            "booking_charge": s.get("booking_charge"),
            "sla_deadline": s.get("sla_deadline"),
            "hours_remaining": s.get("hours_remaining"),
            "imminent_breach": s.get("imminent_breach", False),
            "created_at": created_at,
        })
    return rows


def _extract_details(packet: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Explode a decision packet into granular sub-document rows for PACKET_DETAILS.

    Produces one row per: policy evaluation, shipment, contradiction, claim, action.
    Each row has a searchable detail_text for Cortex Search indexing.
    """
    case_id = packet.get("case_id")
    scope = packet.get("scope") or {}
    airport = scope.get("airport")
    posture_decision = packet.get("posture_decision") or {}
    posture = posture_decision.get("posture")
    created_at = packet.get("created_at")
    rows: List[Dict[str, Any]] = []

    # --- Policy evaluations ---
    for i, p in enumerate(packet.get("policies_applied") or []):
        policy_text = p.get("policy_text", "")
        effect = p.get("effect", "")
        detail_text = f"Policy: {policy_text}. Effect: {effect}."
        rows.append({
            "detail_id": f"{case_id}::POLICY::{i}",
            "case_id": case_id,
            "airport": airport,
            "posture": posture,
            "detail_type": "POLICY",
            "detail_text": detail_text,
            "detail_variant": p,
            "created_at": created_at,
        })

    # --- Shipments (cascade impact) ---
    cascade = packet.get("cascade_impact") or {}
    for i, s in enumerate(cascade.get("shipments") or []):
        tracking = s.get("tracking_number", "UNKNOWN")
        commodity = s.get("commodity", "")
        weight = s.get("weight_kg", 0)
        service = s.get("service_level", "")
        charge = s.get("booking_charge", 0)
        hours = s.get("hours_remaining")
        breach = s.get("imminent_breach", False)
        detail_text = (
            f"Shipment {tracking}: {commodity}, {weight:.0f}kg, {service} service. "
            f"Booking charge ${charge:,.0f}."
        )
        if breach:
            detail_text += f" SLA breach imminent ({hours:.1f}h remaining)."
        rows.append({
            "detail_id": f"{case_id}::SHIPMENT::{i}",
            "case_id": case_id,
            "airport": airport,
            "posture": posture,
            "detail_type": "SHIPMENT",
            "detail_text": detail_text,
            "detail_variant": s,
            "created_at": created_at,
        })

    # --- Contradictions ---
    for i, c in enumerate(packet.get("contradictions") or []):
        c_type = c.get("contradiction_type", "UNKNOWN")
        status = c.get("resolution_status", "")
        detail_text = f"Contradiction ({c_type}): resolution {status}."
        rows.append({
            "detail_id": f"{case_id}::CONTRADICTION::{i}",
            "case_id": case_id,
            "airport": airport,
            "posture": posture,
            "detail_type": "CONTRADICTION",
            "detail_text": detail_text,
            "detail_variant": c,
            "created_at": created_at,
        })

    # --- Claims ---
    for i, cl in enumerate(packet.get("top_claims") or []):
        claim_text = cl.get("text", "")
        status = cl.get("status", "")
        confidence = cl.get("confidence", 0)
        detail_text = f"Claim: {claim_text}. Status: {status}, confidence: {confidence:.2f}."
        rows.append({
            "detail_id": f"{case_id}::CLAIM::{i}",
            "case_id": case_id,
            "airport": airport,
            "posture": posture,
            "detail_type": "CLAIM",
            "detail_text": detail_text,
            "detail_variant": cl,
            "created_at": created_at,
        })

    # --- Actions ---
    for i, a in enumerate(packet.get("actions_proposed") or []):
        action_type = a.get("action_type", "")
        state = a.get("state", "")
        risk = a.get("risk_level", "")
        detail_text = f"Action: {action_type}. State: {state}, risk: {risk}."
        rows.append({
            "detail_id": f"{case_id}::ACTION::{i}",
            "case_id": case_id,
            "airport": airport,
            "posture": posture,
            "detail_type": "ACTION",
            "detail_text": detail_text,
            "detail_variant": a,
            "created_at": created_at,
        })

    return rows


def extract(watermark: Optional[datetime] = None, run_log: Optional[PipelineRunLog] = None):
    """
    Extract decision packets + evidence from Postgres, write to staging JSONL.

    If watermark is None, tries to read from Snowflake LOAD_STATE.
    Falls back to epoch (load everything) if Snowflake is unreachable.
    """
    # Add repo root to path so we can import app modules
    sys.path.insert(0, str(REPO_ROOT))

    from sqlalchemy import text
    from app.db.engine import SessionLocal
    from app.packets.builder import build_decision_packet

    # Determine watermark
    packet_watermark = watermark or datetime(1970, 1, 1, tzinfo=timezone.utc)
    evidence_watermark = watermark or datetime(1970, 1, 1, tzinfo=timezone.utc)

    if watermark is None:
        try:
            sf_conn = get_snowflake_conn()
            packet_watermark = get_watermark(sf_conn, "decision_packets")
            evidence_watermark = get_watermark(sf_conn, "evidence")
            sf_conn.close()
            logger.info("Watermarks: packets=%s, evidence=%s", packet_watermark, evidence_watermark)
        except Exception as e:
            logger.warning("Could not read Snowflake watermarks (using epoch): %s", e)

    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    session = SessionLocal()
    try:
        # ---------------------------------------------------------------
        # Extract decision packets
        # ---------------------------------------------------------------
        result = session.execute(
            text("""
                SELECT id, created_at
                FROM "case"
                WHERE status = 'RESOLVED'
                  AND created_at > :watermark
                ORDER BY created_at
            """),
            {"watermark": packet_watermark},
        )
        case_rows = result.fetchall()
        logger.info("Found %d new resolved cases since %s", len(case_rows), packet_watermark)

        max_packet_ts = packet_watermark
        packet_count = 0
        shipment_count = 0
        detail_count = 0
        skipped_count = 0
        validation_warnings = []

        with open(PACKETS_JSONL, "w") as pf, \
             open(SHIPMENTS_JSONL, "w") as sf, \
             open(DETAILS_JSONL, "w") as df:
            for case_id, created_at in case_rows:
                try:
                    packet = build_decision_packet(case_id, session=session)
                    if not packet:
                        skipped_count += 1
                        continue

                    flat = _flatten_packet(packet)

                    # Validate required fields
                    warnings = _validate_packet(flat, str(case_id))
                    if warnings:
                        validation_warnings.extend(warnings)
                        for w in warnings:
                            logger.warning("Validation: %s", w)

                    # Skip packets missing critical fields (case_id is non-negotiable)
                    if not flat.get("case_id"):
                        skipped_count += 1
                        logger.error("Skipping case %s: no case_id in packet", case_id)
                        continue

                    pf.write(json.dumps(flat, default=str) + "\n")
                    packet_count += 1

                    # Explode shipments
                    for shipment_row in _extract_shipments(packet):
                        sf.write(json.dumps(shipment_row, default=str) + "\n")
                        shipment_count += 1

                    # Explode granular details (policies, claims, contradictions, actions, shipments)
                    for detail_row in _extract_details(packet):
                        df.write(json.dumps(detail_row, default=str) + "\n")
                        detail_count += 1

                    if created_at and created_at > max_packet_ts:
                        max_packet_ts = created_at

                except Exception as e:
                    logger.error("Failed to build packet for case %s: %s", case_id, e)
                    skipped_count += 1

        logger.info(
            "Extracted %d packets, %d shipments, %d details, %d skipped, %d validation warnings",
            packet_count, shipment_count, detail_count, skipped_count, len(validation_warnings),
        )

        # ---------------------------------------------------------------
        # Extract evidence
        # ---------------------------------------------------------------
        ev_result = session.execute(
            text("""
                SELECT
                    e.payload_sha256,
                    t.case_id::text,
                    e.source_system,
                    e.event_time_start,
                    e.retrieved_at,
                    e.excerpt,
                    e.raw_path
                FROM evidence e
                LEFT JOIN (
                    SELECT DISTINCT ref_id::text AS evidence_id, case_id
                    FROM trace_event
                    WHERE ref_type = 'evidence'
                ) t ON t.evidence_id = e.id::text
                WHERE e.retrieved_at > :watermark
                ORDER BY e.retrieved_at
            """),
            {"watermark": evidence_watermark},
        )
        ev_rows = ev_result.fetchall()
        logger.info("Found %d new evidence rows since %s", len(ev_rows), evidence_watermark)

        max_evidence_ts = evidence_watermark
        evidence_count = 0
        evidence_no_sha = 0

        with open(EVIDENCE_JSONL, "w") as ef:
            for row in ev_rows:
                sha256, case_id, source, event_time, ingested_at, excerpt, raw_path = row

                # Skip evidence without sha256 (primary key)
                if not sha256:
                    evidence_no_sha += 1
                    logger.warning("Skipping evidence row with NULL sha256 (case=%s)", case_id)
                    continue

                ef.write(json.dumps({
                    "sha256": sha256,
                    "case_id": case_id,
                    "source": source,
                    "event_time": event_time.isoformat() if event_time else None,
                    "ingested_at": ingested_at.isoformat() if ingested_at else None,
                    "payload_text": excerpt,
                    "raw_path": raw_path,
                }, default=str) + "\n")
                evidence_count += 1

                if ingested_at and ingested_at > max_evidence_ts:
                    max_evidence_ts = ingested_at

        if evidence_no_sha:
            logger.warning("Skipped %d evidence rows with NULL sha256", evidence_no_sha)
        logger.info("Extracted %d evidence rows", evidence_count)

        # Write watermarks + counts for the load step to reconcile against
        meta = {
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "packet_watermark": max_packet_ts.isoformat(),
            "evidence_watermark": max_evidence_ts.isoformat(),
            "packet_count": packet_count,
            "evidence_count": evidence_count,
            "shipment_count": shipment_count,
            "detail_count": detail_count,
            "skipped_count": skipped_count,
            "validation_warnings": len(validation_warnings),
        }
        (STAGING_DIR / "meta.json").write_text(json.dumps(meta, indent=2))
        logger.info("Extract complete. Meta: %s", json.dumps(meta))

        if run_log:
            run_log.log_stage("extract", "OK", {
                "packets_extracted": packet_count,
                "shipments_extracted": shipment_count,
                "details_extracted": detail_count,
                "evidence_extracted": evidence_count,
                "packets_skipped": skipped_count,
                "validation_warnings": len(validation_warnings),
            })

    finally:
        session.close()


# ---------------------------------------------------------------------------
# LOAD: staging JSONL → Snowflake (with reconciliation)
# ---------------------------------------------------------------------------

def _count_jsonl_lines(path: Path) -> int:
    """Count non-empty lines in a JSONL file."""
    if not path.exists():
        return 0
    with open(path) as f:
        return sum(1 for line in f if line.strip())


def load(run_log: Optional[PipelineRunLog] = None):
    """Load staged JSONL files into Snowflake RAW tables, refresh GOLD."""

    # Pre-check: staging files must exist
    meta_file = STAGING_DIR / "meta.json"
    if not meta_file.exists():
        raise FileNotFoundError(
            f"No meta.json in {STAGING_DIR}. Run 'extract' first."
        )

    meta = json.loads(meta_file.read_text())
    expected_packets = meta.get("packet_count", 0)
    expected_evidence = meta.get("evidence_count", 0)
    expected_shipments = meta.get("shipment_count", 0)
    expected_details = meta.get("detail_count", 0)

    # Verify JSONL line counts match meta (catch truncated writes)
    actual_packet_lines = _count_jsonl_lines(PACKETS_JSONL)
    actual_shipment_lines = _count_jsonl_lines(SHIPMENTS_JSONL)
    actual_evidence_lines = _count_jsonl_lines(EVIDENCE_JSONL)
    actual_detail_lines = _count_jsonl_lines(DETAILS_JSONL)

    if actual_packet_lines != expected_packets:
        logger.warning(
            "Reconciliation: packets.jsonl has %d lines but meta says %d",
            actual_packet_lines, expected_packets,
        )
    if actual_shipment_lines != expected_shipments:
        logger.warning(
            "Reconciliation: shipments.jsonl has %d lines but meta says %d",
            actual_shipment_lines, expected_shipments,
        )
    if actual_evidence_lines != expected_evidence:
        logger.warning(
            "Reconciliation: evidence.jsonl has %d lines but meta says %d",
            actual_evidence_lines, expected_evidence,
        )
    if actual_detail_lines != expected_details:
        logger.warning(
            "Reconciliation: details.jsonl has %d lines but meta says %d",
            actual_detail_lines, expected_details,
        )

    sf_conn = get_snowflake_conn()
    cur = sf_conn.cursor()

    try:
        cur.execute("USE DATABASE AALCP_DB")

        # Snapshot pre-load row counts for reconciliation
        pre_counts = _get_table_counts(cur)
        logger.info("Pre-load row counts: %s", pre_counts)

        # ---------------------------------------------------------------
        # Load decision packets
        # ---------------------------------------------------------------
        if PACKETS_JSONL.exists() and PACKETS_JSONL.stat().st_size > 0:
            logger.info("Loading %d packets to Snowflake...", actual_packet_lines)
            cur.execute(f"PUT 'file://{PACKETS_JSONL}' @RAW.INGEST_STAGE/packets AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

            cur.execute("""
                MERGE INTO RAW.DECISION_PACKETS AS t
                USING (
                    SELECT
                        $1:case_id::STRING           AS case_id,
                        $1:airport::STRING            AS airport,
                        $1:scenario_id::STRING        AS scenario_id,
                        $1:posture::STRING            AS posture,
                        $1:rationale_text::STRING     AS rationale_text,
                        $1:contradictions_text::STRING AS contradictions_text,
                        $1:policies_text::STRING      AS policies_text,
                        $1:actions_text::STRING       AS actions_text,
                        $1:cascade_text::STRING       AS cascade_text,
                        $1:metrics_variant            AS metrics_variant,
                        $1:created_at::TIMESTAMP_TZ   AS created_at,
                        $1:raw_packet_variant         AS raw_packet_variant
                    FROM @RAW.INGEST_STAGE/packets
                ) AS s
                ON t.case_id = s.case_id
                WHEN MATCHED THEN UPDATE SET
                    t.airport             = s.airport,
                    t.scenario_id         = s.scenario_id,
                    t.posture             = s.posture,
                    t.rationale_text      = s.rationale_text,
                    t.contradictions_text = s.contradictions_text,
                    t.policies_text       = s.policies_text,
                    t.actions_text        = s.actions_text,
                    t.cascade_text        = s.cascade_text,
                    t.metrics_variant     = s.metrics_variant,
                    t.created_at          = s.created_at,
                    t.raw_packet_variant  = s.raw_packet_variant
                WHEN NOT MATCHED THEN INSERT (
                    case_id, airport, scenario_id, posture,
                    rationale_text, contradictions_text, policies_text, actions_text,
                    cascade_text, metrics_variant, created_at, raw_packet_variant
                ) VALUES (
                    s.case_id, s.airport, s.scenario_id, s.posture,
                    s.rationale_text, s.contradictions_text, s.policies_text, s.actions_text,
                    s.cascade_text, s.metrics_variant, s.created_at, s.raw_packet_variant
                )
            """)
            logger.info("Packets MERGE complete")

        # ---------------------------------------------------------------
        # Load cascade shipments
        # ---------------------------------------------------------------
        if SHIPMENTS_JSONL.exists() and SHIPMENTS_JSONL.stat().st_size > 0:
            logger.info("Loading %d shipments to Snowflake...", actual_shipment_lines)
            cur.execute(f"PUT 'file://{SHIPMENTS_JSONL}' @RAW.INGEST_STAGE/shipments AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

            cur.execute("""
                MERGE INTO RAW.CASCADE_SHIPMENTS AS t
                USING (
                    SELECT
                        $1:case_id::STRING            AS case_id,
                        $1:airport::STRING             AS airport,
                        $1:tracking_number::STRING     AS tracking_number,
                        $1:commodity::STRING            AS commodity,
                        $1:weight_kg::FLOAT            AS weight_kg,
                        $1:service_level::STRING       AS service_level,
                        $1:booking_charge::FLOAT       AS booking_charge,
                        $1:sla_deadline::TIMESTAMP_TZ  AS sla_deadline,
                        $1:hours_remaining::FLOAT      AS hours_remaining,
                        $1:imminent_breach::BOOLEAN    AS imminent_breach,
                        $1:created_at::TIMESTAMP_TZ    AS created_at
                    FROM @RAW.INGEST_STAGE/shipments
                ) AS s
                ON t.case_id = s.case_id AND t.tracking_number = s.tracking_number
                WHEN MATCHED THEN UPDATE SET
                    t.airport         = s.airport,
                    t.commodity       = s.commodity,
                    t.weight_kg       = s.weight_kg,
                    t.service_level   = s.service_level,
                    t.booking_charge  = s.booking_charge,
                    t.sla_deadline    = s.sla_deadline,
                    t.hours_remaining = s.hours_remaining,
                    t.imminent_breach = s.imminent_breach,
                    t.created_at      = s.created_at
                WHEN NOT MATCHED THEN INSERT (
                    case_id, airport, tracking_number, commodity, weight_kg,
                    service_level, booking_charge, sla_deadline, hours_remaining,
                    imminent_breach, created_at
                ) VALUES (
                    s.case_id, s.airport, s.tracking_number, s.commodity, s.weight_kg,
                    s.service_level, s.booking_charge, s.sla_deadline, s.hours_remaining,
                    s.imminent_breach, s.created_at
                )
            """)
            logger.info("Shipments MERGE complete")

        # ---------------------------------------------------------------
        # Load evidence
        # ---------------------------------------------------------------
        if EVIDENCE_JSONL.exists() and EVIDENCE_JSONL.stat().st_size > 0:
            logger.info("Loading %d evidence rows to Snowflake...", actual_evidence_lines)
            cur.execute(f"PUT 'file://{EVIDENCE_JSONL}' @RAW.INGEST_STAGE/evidence AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

            cur.execute("""
                MERGE INTO RAW.EVIDENCE AS t
                USING (
                    SELECT
                        $1:sha256::STRING             AS sha256,
                        $1:case_id::STRING             AS case_id,
                        $1:source::STRING              AS source,
                        $1:event_time::TIMESTAMP_TZ    AS event_time,
                        $1:ingested_at::TIMESTAMP_TZ   AS ingested_at,
                        $1:payload_text::STRING        AS payload_text,
                        $1:raw_path::STRING            AS raw_path
                    FROM @RAW.INGEST_STAGE/evidence
                ) AS s
                ON t.sha256 = s.sha256
                WHEN MATCHED THEN UPDATE SET
                    t.case_id      = s.case_id,
                    t.source       = s.source,
                    t.event_time   = s.event_time,
                    t.ingested_at  = s.ingested_at,
                    t.payload_text = s.payload_text,
                    t.raw_path     = s.raw_path
                WHEN NOT MATCHED THEN INSERT (
                    sha256, case_id, source, event_time, ingested_at, payload_text, raw_path
                ) VALUES (
                    s.sha256, s.case_id, s.source, s.event_time, s.ingested_at, s.payload_text, s.raw_path
                )
            """)
            logger.info("Evidence MERGE complete")

        # ---------------------------------------------------------------
        # Load packet details (granular sub-document rows)
        # ---------------------------------------------------------------
        if DETAILS_JSONL.exists() and DETAILS_JSONL.stat().st_size > 0:
            logger.info("Loading %d detail rows to Snowflake...", actual_detail_lines)
            cur.execute(f"PUT 'file://{DETAILS_JSONL}' @RAW.INGEST_STAGE/details AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

            cur.execute("""
                MERGE INTO RAW.PACKET_DETAILS AS t
                USING (
                    SELECT
                        $1:detail_id::STRING         AS detail_id,
                        $1:case_id::STRING            AS case_id,
                        $1:airport::STRING             AS airport,
                        $1:posture::STRING             AS posture,
                        $1:detail_type::STRING         AS detail_type,
                        $1:detail_text::STRING         AS detail_text,
                        $1:detail_variant              AS detail_variant,
                        $1:created_at::TIMESTAMP_TZ    AS created_at
                    FROM @RAW.INGEST_STAGE/details
                ) AS s
                ON t.detail_id = s.detail_id
                WHEN MATCHED THEN UPDATE SET
                    t.case_id        = s.case_id,
                    t.airport        = s.airport,
                    t.posture        = s.posture,
                    t.detail_type    = s.detail_type,
                    t.detail_text    = s.detail_text,
                    t.detail_variant = s.detail_variant,
                    t.created_at     = s.created_at
                WHEN NOT MATCHED THEN INSERT (
                    detail_id, case_id, airport, posture, detail_type,
                    detail_text, detail_variant, created_at
                ) VALUES (
                    s.detail_id, s.case_id, s.airport, s.posture, s.detail_type,
                    s.detail_text, s.detail_variant, s.created_at
                )
            """)
            logger.info("Details MERGE complete")

        # ---------------------------------------------------------------
        # Refresh GOLD tables
        # ---------------------------------------------------------------
        logger.info("Refreshing GOLD tables...")
        gold_sql = (SQL_DIR / "02_gold_transforms.sql").read_text()
        for statement in gold_sql.split(";"):
            stmt = statement.strip()
            if stmt and not stmt.startswith("--"):
                cur.execute(stmt)
        logger.info("GOLD refresh complete")

        # ---------------------------------------------------------------
        # Post-load reconciliation
        # ---------------------------------------------------------------
        post_counts = _get_table_counts(cur)
        logger.info("Post-load row counts: %s", post_counts)

        delta_packets = post_counts["packets"] - pre_counts["packets"]
        delta_shipments = post_counts["shipments"] - pre_counts["shipments"]
        delta_evidence = post_counts["evidence"] - pre_counts["evidence"]
        delta_details = post_counts["details"] - pre_counts["details"]

        logger.info(
            "Reconciliation: +%d packets (expected %d), +%d shipments (expected %d), "
            "+%d details (expected %d), +%d evidence (expected %d)",
            delta_packets, expected_packets,
            delta_shipments, expected_shipments,
            delta_details, expected_details,
            delta_evidence, expected_evidence,
        )

        # Warn if counts diverge (updates don't increment, so delta <= expected is normal)
        if delta_packets > expected_packets:
            logger.warning("More packets inserted than expected — possible duplicate source data")
        if delta_evidence > expected_evidence:
            logger.warning("More evidence inserted than expected — possible duplicate source data")

        # ---------------------------------------------------------------
        # Update watermarks
        # ---------------------------------------------------------------
        update_watermark(sf_conn, "decision_packets", datetime.fromisoformat(meta["packet_watermark"]))
        update_watermark(sf_conn, "evidence", datetime.fromisoformat(meta["evidence_watermark"]))
        logger.info("Watermarks updated")

        # Clean up staged files
        cur.execute("REMOVE @RAW.INGEST_STAGE/packets")
        cur.execute("REMOVE @RAW.INGEST_STAGE/shipments")
        cur.execute("REMOVE @RAW.INGEST_STAGE/evidence")
        cur.execute("REMOVE @RAW.INGEST_STAGE/details")
        logger.info("Staged files cleaned up")

        if run_log:
            run_log.log_stage("load", "OK", {
                "rows_merged_packets": delta_packets,
                "rows_merged_shipments": delta_shipments,
                "rows_merged_details": delta_details,
                "rows_merged_evidence": delta_evidence,
                "post_total_packets": post_counts["packets"],
                "post_total_shipments": post_counts["shipments"],
                "post_total_details": post_counts["details"],
                "post_total_evidence": post_counts["evidence"],
                "gold_posture_daily": _safe_count(cur, "GOLD.POSTURE_DAILY"),
                "gold_contradictions_daily": _safe_count(cur, "GOLD.CONTRADICTIONS_DAILY"),
                "gold_evidence_coverage_daily": _safe_count(cur, "GOLD.EVIDENCE_COVERAGE_DAILY"),
            })

    finally:
        cur.close()
        sf_conn.close()

    logger.info("Load complete")


def _get_table_counts(cur) -> Dict[str, int]:
    """Get current row counts for reconciliation."""
    counts = {}
    for table, key in [
        ("RAW.DECISION_PACKETS", "packets"),
        ("RAW.CASCADE_SHIPMENTS", "shipments"),
        ("RAW.EVIDENCE", "evidence"),
        ("RAW.PACKET_DETAILS", "details"),
    ]:
        counts[key] = _safe_count(cur, table)
    return counts


def _safe_count(cur, table: str) -> int:
    """Get row count, returning -1 on error."""
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]
    except Exception:
        return -1


# ---------------------------------------------------------------------------
# VERIFY: standalone data quality checks (no Airflow needed)
# ---------------------------------------------------------------------------

def verify() -> Tuple[bool, List[str]]:
    """
    Run data quality checks on Snowflake tables.
    Same checks as the Airflow data_quality_gates task, runnable standalone.

    Returns:
        (all_passed, list_of_failures)
    """
    sf_conn = get_snowflake_conn()
    cur = sf_conn.cursor()
    failures = []

    try:
        cur.execute("USE DATABASE AALCP_DB")

        # Check 1: Row counts (at least 1 row per RAW table)
        for table in ["RAW.DECISION_PACKETS", "RAW.CASCADE_SHIPMENTS", "RAW.EVIDENCE", "RAW.PACKET_DETAILS"]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            if count < 1:
                failures.append(f"Row count: {table} has {count} rows (need >= 1)")
            else:
                logger.info("Row count OK: %s has %d rows", table, count)

        # Check 2: Freshness — latest packet created_at within 24 hours
        cur.execute("SELECT MAX(created_at) FROM RAW.DECISION_PACKETS")
        row = cur.fetchone()
        if row and row[0]:
            latest = row[0]
            if isinstance(latest, str):
                latest = datetime.fromisoformat(latest)
            age_hours = (datetime.now(latest.tzinfo or None) - latest).total_seconds() / 3600
            if age_hours > 24:
                failures.append(f"Freshness: latest packet is {age_hours:.1f}h old (threshold: 24h)")
            else:
                logger.info("Freshness OK: latest packet %.1fh old", age_hours)
        else:
            failures.append("Freshness: no packets found")

        # Check 3: NULL rationale threshold (< 50%)
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN rationale_text IS NULL OR rationale_text = '' THEN 1 ELSE 0 END) AS nulls
            FROM RAW.DECISION_PACKETS
        """)
        total, nulls = cur.fetchone()
        if total > 0:
            pct_null = (nulls / total) * 100
            if pct_null >= 50:
                failures.append(f"NULL rationale: {pct_null:.0f}% null (threshold: <50%)")
            else:
                logger.info("NULL rationale OK: %.0f%% null", pct_null)

        # Check 4: case_id uniqueness
        cur.execute("""
            SELECT case_id, COUNT(*) AS cnt
            FROM RAW.DECISION_PACKETS
            GROUP BY case_id
            HAVING COUNT(*) > 1
            LIMIT 5
        """)
        dupes = cur.fetchall()
        if dupes:
            failures.append(f"Uniqueness: {len(dupes)} duplicate case_ids found: {[d[0] for d in dupes]}")
        else:
            logger.info("Uniqueness OK: no duplicate case_ids")

        # Check 5: GOLD tables populated
        for table in ["GOLD.POSTURE_DAILY", "GOLD.CONTRADICTIONS_DAILY", "GOLD.EVIDENCE_COVERAGE_DAILY"]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            if count < 1:
                failures.append(f"GOLD: {table} is empty (run load to refresh)")
            else:
                logger.info("GOLD OK: %s has %d rows", table, count)

        # Check 6: Watermark sanity — watermarks should be after epoch
        cur.execute("SELECT source, watermark FROM RAW.LOAD_STATE")
        for source, wm in cur.fetchall():
            if isinstance(wm, str):
                wm = datetime.fromisoformat(wm)
            if wm.year <= 1970:
                failures.append(f"Watermark: {source} still at epoch — no data loaded yet")
            else:
                logger.info("Watermark OK: %s = %s", source, wm)

        # Check 7: Cortex Search reachability — verify both services return results
        for service_name, query, cols in [
            ("PACKET_SEARCH", "airport posture", '["case_id","posture"]'),
            ("DETAIL_SEARCH", "policy shipment", '["detail_id","detail_type"]'),
        ]:
            try:
                cur.execute(f"""
                    SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                        'AALCP_DB.SERVICES.{service_name}',
                        '{{"query":"{query}","columns":{cols},"limit":1}}'
                    )
                """)
                row = cur.fetchone()
                if row and row[0]:
                    parsed = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                    results = parsed.get("results", []) if isinstance(parsed, dict) else []
                    if results:
                        logger.info("Cortex Search OK: %s returned %d result(s)", service_name, len(results))
                    else:
                        failures.append(
                            f"Cortex Search: {service_name} returned 0 results — "
                            "run 03_cortex_search.sql and wait for indexing."
                        )
                else:
                    failures.append(f"Cortex Search: {service_name} returned NULL — service may not exist")
            except Exception as e:
                err_str = str(e)
                if "does not exist" in err_str.lower() or "not found" in err_str.lower():
                    failures.append(f"Cortex Search: {service_name} not found — run 03_cortex_search.sql")
                else:
                    failures.append(f"Cortex Search: {service_name} SEARCH_PREVIEW failed — {e}")

    finally:
        cur.close()
        sf_conn.close()

    all_passed = len(failures) == 0
    if all_passed:
        logger.info("All data quality checks PASSED")
    else:
        for f in failures:
            logger.error("FAIL: %s", f)

    return all_passed, failures


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def run():
    """Extract then load with structured run logging."""
    run_log = PipelineRunLog()
    logger.info("Pipeline run %s started", run_log.run_id)
    extract(run_log=run_log)
    load(run_log=run_log)
    summary = run_log.summary()
    logger.info("Pipeline run %s complete: %s", run_log.run_id, json.dumps(summary))


if __name__ == "__main__":
    commands = {
        "extract": extract,
        "load": load,
        "run": run,
        "verify": lambda: verify(),
        "preflight": lambda: preflight(),
    }

    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print(f"Usage: python -m audit_warehouse.load [{' | '.join(commands)}]")
        sys.exit(1)

    command = sys.argv[1]
    result = commands[command]()

    # verify and preflight return (ok, issues) — exit 1 on failure
    if isinstance(result, tuple):
        ok, issues = result
        if not ok:
            print(f"\n{len(issues)} issue(s) found:")
            for issue in issues:
                print(f"  - {issue}")
            sys.exit(1)
