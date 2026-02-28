-- =============================================================================
-- Validation queries for the AALCP audit warehouse
-- Run after loading data + creating Cortex Search service.
-- =============================================================================

USE DATABASE AALCP_DB;

-- =============================================================================
-- 1. RAW table row counts
-- =============================================================================

SELECT 'RAW.DECISION_PACKETS' AS table_name, COUNT(*) AS row_count
FROM RAW.DECISION_PACKETS
UNION ALL
SELECT 'RAW.CASCADE_SHIPMENTS', COUNT(*)
FROM RAW.CASCADE_SHIPMENTS
UNION ALL
SELECT 'RAW.EVIDENCE', COUNT(*)
FROM RAW.EVIDENCE
UNION ALL
SELECT 'RAW.PACKET_DETAILS', COUNT(*)
FROM RAW.PACKET_DETAILS;

-- =============================================================================
-- 2. GOLD table row counts
-- =============================================================================

SELECT 'GOLD.POSTURE_DAILY' AS table_name, COUNT(*) AS row_count
FROM GOLD.POSTURE_DAILY
UNION ALL
SELECT 'GOLD.CONTRADICTIONS_DAILY', COUNT(*)
FROM GOLD.CONTRADICTIONS_DAILY
UNION ALL
SELECT 'GOLD.EVIDENCE_COVERAGE_DAILY', COUNT(*)
FROM GOLD.EVIDENCE_COVERAGE_DAILY;

-- =============================================================================
-- 3. NULL checks on critical columns
-- =============================================================================

SELECT
    COUNT(*)                                                     AS total_packets,
    SUM(CASE WHEN rationale_text IS NULL THEN 1 ELSE 0 END)      AS null_rationale,
    SUM(CASE WHEN posture IS NULL THEN 1 ELSE 0 END)             AS null_posture,
    SUM(CASE WHEN airport IS NULL THEN 1 ELSE 0 END)             AS null_airport,
    ROUND(SUM(CASE WHEN rationale_text IS NULL THEN 1 ELSE 0 END)
          * 100.0 / NULLIF(COUNT(*), 0), 1)                      AS pct_null_rationale
FROM RAW.DECISION_PACKETS;

-- =============================================================================
-- 4. Watermark state
-- =============================================================================

SELECT * FROM RAW.LOAD_STATE;

-- =============================================================================
-- 5. Cortex Search validation via SEARCH_PREVIEW
-- =============================================================================

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'AALCP_DB.SERVICES.PACKET_SEARCH',
    '{
        "query": "HOLD posture",
        "columns": ["case_id", "rationale_text", "airport", "posture"],
        "limit": 3
    }'
);

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'AALCP_DB.SERVICES.PACKET_SEARCH',
    '{
        "query": "shipments affected SLA breach",
        "columns": ["case_id", "rationale_text", "cascade_text", "airport"],
        "limit": 3
    }'
);

-- =============================================================================
-- 5b. Detail-level Cortex Search validation via SEARCH_PREVIEW
-- =============================================================================

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'AALCP_DB.SERVICES.DETAIL_SEARCH',
    '{
        "query": "policy blocked",
        "columns": ["detail_id", "case_id", "detail_text", "detail_type", "airport"],
        "limit": 3
    }'
);

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'AALCP_DB.SERVICES.DETAIL_SEARCH',
    '{
        "query": "premium shipment SLA breach imminent",
        "columns": ["detail_id", "case_id", "detail_text", "detail_type", "airport"],
        "limit": 3
    }'
);

-- Detail type distribution
SELECT detail_type, COUNT(*) AS cnt
FROM RAW.PACKET_DETAILS
GROUP BY detail_type
ORDER BY cnt DESC;

-- =============================================================================
-- 6. Sample GOLD data
-- =============================================================================

SELECT * FROM GOLD.POSTURE_DAILY ORDER BY day DESC LIMIT 10;
SELECT * FROM GOLD.CONTRADICTIONS_DAILY ORDER BY day DESC LIMIT 10;
SELECT * FROM GOLD.EVIDENCE_COVERAGE_DAILY ORDER BY day DESC LIMIT 10;
