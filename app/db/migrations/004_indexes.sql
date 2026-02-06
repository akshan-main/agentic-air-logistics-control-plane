-- 004_indexes.sql
-- Indexes for performance optimization

-- Edge traversal indexes
CREATE INDEX IF NOT EXISTS idx_edge_src_type_valid ON edge(src, type, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_edge_dst_type_valid ON edge(dst, type, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_edge_bitemporal ON edge(event_time_start, ingested_at);

-- Claim indexes
CREATE INDEX IF NOT EXISTS idx_claim_subject_status ON claim(subject_node_id, status);

-- Evidence indexes
CREATE INDEX IF NOT EXISTS idx_evidence_source_time ON evidence(source_system, retrieved_at);

-- Trace indexes
CREATE INDEX IF NOT EXISTS idx_trace_case_seq ON trace_event(case_id, seq);

-- Node version indexes
CREATE INDEX IF NOT EXISTS idx_node_version_node_valid ON node_version(node_id, valid_from, valid_to);

-- Missing evidence indexes
CREATE INDEX IF NOT EXISTS idx_missing_evidence_case ON missing_evidence_request(case_id, criticality);

-- ============================================================
-- PGVECTOR INDEX (with version check and fallback)
-- ============================================================
-- HNSW (>= 0.5.0) preferred for better recall, no training required
-- ivfflat as fallback for older versions

DO $$
DECLARE
    pgvector_version TEXT;
BEGIN
    SELECT extversion INTO pgvector_version FROM pg_extension WHERE extname = 'vector';

    IF pgvector_version IS NULL THEN
        RAISE NOTICE 'pgvector extension not found, skipping vector index creation';
    ELSIF pgvector_version >= '0.5.0' THEN
        -- Use HNSW (better recall, no training required)
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_embedding_case_hnsw ON embedding_case
            USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64)';
        RAISE NOTICE 'Created HNSW index (pgvector %)', pgvector_version;
    ELSE
        -- Fallback to ivfflat (requires ANALYZE after bulk inserts)
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_embedding_case_ivfflat ON embedding_case
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100)';
        RAISE NOTICE 'Created ivfflat index (pgvector % < 0.5.0)', pgvector_version;
    END IF;
END $$;
