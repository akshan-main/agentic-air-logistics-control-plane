-- =============================================================================
-- Cortex Search Service: semantic search over decision packet rationale
--
-- Indexes search_text (rationale + cascade impact) for retrieval.
-- Returns rationale_text and cascade_text as separate readable columns.
--
-- Prerequisites:
--   1. Snowflake account in a Cortex-supported region (US East, US West, EU)
--      OR cross-region inference enabled.
--   2. Warehouse AALCP_CORTEX_WH exists (X-SMALL is sufficient).
--      CREATE WAREHOUSE IF NOT EXISTS AALCP_CORTEX_WH
--          WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
-- =============================================================================

USE DATABASE AALCP_DB;

CREATE OR REPLACE CORTEX SEARCH SERVICE SERVICES.PACKET_SEARCH
    ON search_text
    PRIMARY KEY (case_id)
    ATTRIBUTES airport, posture, scenario_id, created_at
    WAREHOUSE = AALCP_CORTEX_WH
    TARGET_LAG = '1 hour'
    INITIALIZE = ON_CREATE
AS (
    SELECT
        case_id,
        rationale_text,
        cascade_text,
        (rationale_text || ' ' || COALESCE(cascade_text, '')) AS search_text,
        airport,
        posture,
        scenario_id,
        created_at
    FROM RAW.DECISION_PACKETS
    WHERE rationale_text IS NOT NULL
);

-- Fallback: if PRIMARY KEY in CREATE is not supported in your account,
-- uncomment the following:
-- ALTER CORTEX SEARCH SERVICE SERVICES.PACKET_SEARCH
--     SET PRIMARY KEY = (case_id);


-- =============================================================================
-- Cortex Search Service: granular detail-level search
--
-- Indexes individual policy evaluations, shipments, contradictions, claims,
-- and actions for fine-grained Q&A queries like:
--   "Which policy blocked the JFK case?"
--   "Which shipments have imminent SLA breaches?"
--   "What contradictions were found at ORD?"
--
-- detail_type attribute allows filtering by category.
-- =============================================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE SERVICES.DETAIL_SEARCH
    ON detail_text
    PRIMARY KEY (detail_id)
    ATTRIBUTES case_id, airport, posture, detail_type, created_at
    WAREHOUSE = AALCP_CORTEX_WH
    TARGET_LAG = '1 hour'
    INITIALIZE = ON_CREATE
AS (
    SELECT
        detail_id,
        case_id,
        detail_text,
        airport,
        posture,
        detail_type,
        created_at
    FROM RAW.PACKET_DETAILS
    WHERE detail_text IS NOT NULL
);
