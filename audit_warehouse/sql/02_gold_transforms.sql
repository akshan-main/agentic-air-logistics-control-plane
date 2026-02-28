-- =============================================================================
-- GOLD table transforms: idempotent MERGE from RAW → GOLD
-- Run after every RAW load to refresh analytics aggregations.
-- =============================================================================

USE DATABASE AALCP_DB;

-- =============================================================================
-- GOLD.POSTURE_DAILY
-- How many packets per posture per airport per day?
-- =============================================================================

MERGE INTO GOLD.POSTURE_DAILY AS t
USING (
    SELECT
        DATE_TRUNC('DAY', created_at)::DATE AS day,
        airport,
        posture,
        COUNT(*)                            AS packet_count
    FROM RAW.DECISION_PACKETS
    WHERE airport IS NOT NULL AND posture IS NOT NULL
    GROUP BY 1, 2, 3
) AS s
ON  t.day     = s.day
AND t.airport  = s.airport
AND t.posture  = s.posture
WHEN MATCHED THEN
    UPDATE SET t.packet_count = s.packet_count
WHEN NOT MATCHED THEN
    INSERT (day, airport, posture, packet_count)
    VALUES (s.day, s.airport, s.posture, s.packet_count);

-- =============================================================================
-- GOLD.CONTRADICTIONS_DAILY
-- Daily contradiction counts per airport.
-- =============================================================================

MERGE INTO GOLD.CONTRADICTIONS_DAILY AS t
USING (
    SELECT
        DATE_TRUNC('DAY', created_at)::DATE                     AS day,
        airport,
        SUM(COALESCE(metrics_variant:contradiction_count::INT, 0)) AS contradiction_count,
        COUNT(*)                                                 AS packet_count
    FROM RAW.DECISION_PACKETS
    WHERE airport IS NOT NULL
    GROUP BY 1, 2
) AS s
ON  t.day     = s.day
AND t.airport  = s.airport
WHEN MATCHED THEN
    UPDATE SET
        t.contradiction_count = s.contradiction_count,
        t.packet_count        = s.packet_count
WHEN NOT MATCHED THEN
    INSERT (day, airport, contradiction_count, packet_count)
    VALUES (s.day, s.airport, s.contradiction_count, s.packet_count);

-- =============================================================================
-- GOLD.EVIDENCE_COVERAGE_DAILY
-- Daily evidence item counts per airport.
-- =============================================================================

MERGE INTO GOLD.EVIDENCE_COVERAGE_DAILY AS t
USING (
    SELECT
        DATE_TRUNC('DAY', created_at)::DATE                     AS day,
        airport,
        SUM(COALESCE(metrics_variant:evidence_count::INT, 0))   AS evidence_items,
        COUNT(*)                                                 AS packets
    FROM RAW.DECISION_PACKETS
    WHERE airport IS NOT NULL
    GROUP BY 1, 2
) AS s
ON  t.day     = s.day
AND t.airport  = s.airport
WHEN MATCHED THEN
    UPDATE SET
        t.evidence_items = s.evidence_items,
        t.packets        = s.packets
WHEN NOT MATCHED THEN
    INSERT (day, airport, evidence_items, packets)
    VALUES (s.day, s.airport, s.evidence_items, s.packets);
