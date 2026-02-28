-- =============================================================================
-- AALCP Audit Warehouse Schema
-- Snowflake is the ANALYTICS + AUDIT layer. Postgres remains operational.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS AALCP_DB;
USE DATABASE AALCP_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS SERVICES;

-- =============================================================================
-- Ingestion plumbing: file format + internal stage for JSONL loading
-- =============================================================================

CREATE OR REPLACE FILE FORMAT RAW.JSONL_FMT
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE;

CREATE OR REPLACE STAGE RAW.INGEST_STAGE
    FILE_FORMAT = RAW.JSONL_FMT;

-- =============================================================================
-- Incremental watermark: tracks max(created_at) loaded per source
-- =============================================================================

CREATE TABLE IF NOT EXISTS RAW.LOAD_STATE (
    source      STRING    NOT NULL PRIMARY KEY,
    watermark   TIMESTAMP_TZ NOT NULL,
    updated_at  TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- Seed epoch watermarks so first extract doesn't need special-case logic
INSERT INTO RAW.LOAD_STATE (source, watermark)
    SELECT 'decision_packets', '1970-01-01'::TIMESTAMP_TZ
    WHERE NOT EXISTS (SELECT 1 FROM RAW.LOAD_STATE WHERE source = 'decision_packets');

INSERT INTO RAW.LOAD_STATE (source, watermark)
    SELECT 'evidence', '1970-01-01'::TIMESTAMP_TZ
    WHERE NOT EXISTS (SELECT 1 FROM RAW.LOAD_STATE WHERE source = 'evidence');

-- =============================================================================
-- RAW.DECISION_PACKETS
-- One row per resolved case. Flattened text columns for Cortex Search.
-- raw_packet_variant holds the full JSON for ad-hoc exploration.
-- =============================================================================

CREATE TABLE IF NOT EXISTS RAW.DECISION_PACKETS (
    case_id             STRING       NOT NULL PRIMARY KEY,
    airport             STRING,
    scenario_id         STRING,
    posture             STRING,
    rationale_text      STRING,       -- posture_decision.reason
    contradictions_text STRING,       -- flattened contradiction descriptions
    policies_text       STRING,       -- flattened policy_text: effect
    actions_text        STRING,       -- flattened action_type (state)
    cascade_text        STRING,       -- flattened operational impact summary
    metrics_variant     VARIANT,      -- full metrics dict
    created_at          TIMESTAMP_TZ,
    raw_packet_variant  VARIANT       -- entire decision packet JSON
);

-- =============================================================================
-- RAW.CASCADE_SHIPMENTS
-- Exploded from decision packet cascade_impact.shipments.
-- One row per shipment per case. Forwarder revenue (booking_charge), NOT item value.
-- =============================================================================

CREATE TABLE IF NOT EXISTS RAW.CASCADE_SHIPMENTS (
    case_id          STRING    NOT NULL,
    airport          STRING,
    tracking_number  STRING    NOT NULL,
    commodity        STRING,
    weight_kg        FLOAT,
    service_level    STRING,
    booking_charge   FLOAT,
    sla_deadline     TIMESTAMP_TZ,
    hours_remaining  FLOAT,
    imminent_breach  BOOLEAN,
    created_at       TIMESTAMP_TZ,
    PRIMARY KEY (case_id, tracking_number)
);

-- =============================================================================
-- RAW.EVIDENCE
-- Audit index only. No raw blob storage in Snowflake.
-- sha256 + excerpt + raw_path pointer to on-disk immutable evidence store.
-- =============================================================================

CREATE TABLE IF NOT EXISTS RAW.EVIDENCE (
    sha256        STRING       NOT NULL PRIMARY KEY,
    case_id       STRING,
    source        STRING,
    event_time    TIMESTAMP_TZ,
    ingested_at   TIMESTAMP_TZ,
    payload_text  STRING,        -- excerpt (PII-redacted, 500 chars max)
    raw_path      STRING         -- pointer to ./var/evidence/{sha256}.bin
);

-- =============================================================================
-- RAW.PACKET_DETAILS
-- Granular sub-document rows exploded from decision packets.
-- One row per policy evaluation, shipment, contradiction, claim, or action.
-- Enables fine-grained Cortex Search queries (e.g. "which policy blocked JFK?")
-- =============================================================================

CREATE TABLE IF NOT EXISTS RAW.PACKET_DETAILS (
    detail_id       STRING       NOT NULL PRIMARY KEY,  -- case_id::detail_type::seq
    case_id         STRING       NOT NULL,
    airport         STRING,
    posture         STRING,
    detail_type     STRING       NOT NULL,  -- POLICY | SHIPMENT | CONTRADICTION | CLAIM | ACTION
    detail_text     STRING,                 -- searchable natural language summary
    detail_variant  VARIANT,                -- full structured JSON for this sub-component
    created_at      TIMESTAMP_TZ
);

-- =============================================================================
-- GOLD tables: daily aggregations for analytics dashboards
-- Refreshed by 02_gold_transforms.sql MERGE statements
-- =============================================================================

CREATE TABLE IF NOT EXISTS GOLD.POSTURE_DAILY (
    day          DATE    NOT NULL,
    airport      STRING  NOT NULL,
    posture      STRING  NOT NULL,
    packet_count INTEGER,
    PRIMARY KEY (day, airport, posture)
);

CREATE TABLE IF NOT EXISTS GOLD.CONTRADICTIONS_DAILY (
    day                 DATE    NOT NULL,
    airport             STRING  NOT NULL,
    contradiction_count INTEGER,
    packet_count        INTEGER,
    PRIMARY KEY (day, airport)
);

CREATE TABLE IF NOT EXISTS GOLD.EVIDENCE_COVERAGE_DAILY (
    day            DATE    NOT NULL,
    airport        STRING  NOT NULL,
    evidence_items INTEGER,
    packets        INTEGER,
    PRIMARY KEY (day, airport)
);
