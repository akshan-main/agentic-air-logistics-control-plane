-- 006_evidence_dedup.sql
-- Add uniqueness constraint on evidence to prevent duplicates from re-ingestion.
-- Same source + same reference + same content hash = same evidence.

-- If a DB existed before this migration, evidence rows may already contain duplicates.
-- We must deduplicate BEFORE creating the unique index.
--
-- Keep strategy:
--   - Keep the row with the most recent retrieved_at (tie-breaker: lowest UUID).
--   - Rewrite foreign-key references to point at the kept row.
--   - Delete the duplicates.

BEGIN;

-- Prevent concurrent inserts while we deduplicate and create the unique index.
LOCK TABLE evidence IN SHARE ROW EXCLUSIVE MODE;

DROP TABLE IF EXISTS pg_temp.evidence_dedup_map;
CREATE TEMP TABLE evidence_dedup_map (
    dup_id UUID PRIMARY KEY,
    keep_id UUID NOT NULL
) ON COMMIT DROP;

INSERT INTO evidence_dedup_map (dup_id, keep_id)
SELECT e.id AS dup_id, k.keep_id
FROM evidence e
JOIN (
    SELECT DISTINCT ON (source_system, source_ref, payload_sha256)
        id AS keep_id,
        source_system,
        source_ref,
        payload_sha256
    FROM evidence
    ORDER BY
        source_system,
        source_ref,
        payload_sha256,
        retrieved_at DESC,
        id ASC
) k
  ON e.source_system = k.source_system
 AND e.source_ref = k.source_ref
 AND e.payload_sha256 = k.payload_sha256
WHERE e.id <> k.keep_id;

-- claim_evidence: insert keep rows first to avoid PK conflicts, then delete dup rows
INSERT INTO claim_evidence (claim_id, evidence_id)
SELECT ce.claim_id, m.keep_id
FROM claim_evidence ce
JOIN evidence_dedup_map m ON ce.evidence_id = m.dup_id
ON CONFLICT DO NOTHING;

DELETE FROM claim_evidence ce
USING evidence_dedup_map m
WHERE ce.evidence_id = m.dup_id;

-- edge_evidence: same pattern
INSERT INTO edge_evidence (edge_id, evidence_id)
SELECT ee.edge_id, m.keep_id
FROM edge_evidence ee
JOIN evidence_dedup_map m ON ee.evidence_id = m.dup_id
ON CONFLICT DO NOTHING;

DELETE FROM edge_evidence ee
USING evidence_dedup_map m
WHERE ee.evidence_id = m.dup_id;

-- missing_evidence_request: rewrite FK to kept evidence row
UPDATE missing_evidence_request mer
SET resolved_by_evidence_id = m.keep_id
FROM evidence_dedup_map m
WHERE mer.resolved_by_evidence_id = m.dup_id;

-- trace_event: keep evidence linkage consistent for policy/packet queries
UPDATE trace_event t
SET ref_id = m.keep_id
FROM evidence_dedup_map m
WHERE t.ref_type = 'evidence'
  AND t.ref_id = m.dup_id;

-- Finally delete duplicate evidence rows
DELETE FROM evidence e
USING evidence_dedup_map m
WHERE e.id = m.dup_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_evidence_dedup
    ON evidence(source_system, source_ref, payload_sha256);

COMMIT;
