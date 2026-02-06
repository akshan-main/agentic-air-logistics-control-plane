-- 006_evidence_dedup.sql
-- Add uniqueness constraint on evidence to prevent duplicates from re-ingestion.
-- Same source + same reference + same content hash = same evidence.

CREATE UNIQUE INDEX IF NOT EXISTS idx_evidence_dedup
    ON evidence(source_system, source_ref, payload_sha256);
