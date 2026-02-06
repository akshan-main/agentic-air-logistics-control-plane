-- 002_core_schema.sql
-- Core schema for Agentic Air Logistics Control Plane

-- ============================================================
-- GRAPH NODES (IMMUTABLE - use node_version for changes)
-- ============================================================
CREATE TABLE IF NOT EXISTS node (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type TEXT NOT NULL,
    identifier TEXT NOT NULL,  -- Human-readable identifier (e.g., "KJFK")
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(type, identifier)
);

-- Node versions (append-only, attrs changes create new version)
CREATE TABLE IF NOT EXISTS node_version (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    node_id UUID NOT NULL REFERENCES node(id),
    attrs JSONB NOT NULL DEFAULT '{}',
    valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to TIMESTAMPTZ,  -- NULL = current version
    supersedes_id UUID REFERENCES node_version(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- GRAPH EDGES (bi-temporal + validity + evidence binding)
-- ============================================================
CREATE TABLE IF NOT EXISTS edge (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    src UUID NOT NULL REFERENCES node(id),
    dst UUID NOT NULL REFERENCES node(id),
    type TEXT NOT NULL,
    attrs JSONB NOT NULL DEFAULT '{}',
    -- Status for evidence binding (only FACT requires evidence)
    status TEXT NOT NULL DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'FACT', 'RETRACTED')),
    -- Audit trail: supersedes previous edge
    supersedes_edge_id UUID REFERENCES edge(id),
    -- Bi-temporal
    event_time_start TIMESTAMPTZ,
    event_time_end TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Validity window
    valid_from TIMESTAMPTZ,
    valid_to TIMESTAMPTZ,
    -- Provenance
    source_system TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5
);

-- ============================================================
-- IMMUTABLE EVIDENCE STORE
-- ============================================================
CREATE TABLE IF NOT EXISTS evidence (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_system TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_time_start TIMESTAMPTZ,
    event_time_end TIMESTAMPTZ,
    content_type TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    raw_path TEXT NOT NULL,
    excerpt TEXT,
    meta JSONB NOT NULL DEFAULT '{}'
);

-- ============================================================
-- CLAIMS (with evidence binding)
-- ============================================================
CREATE TABLE IF NOT EXISTS claim (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    text TEXT NOT NULL,
    subject_node_id UUID REFERENCES node(id),
    confidence DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('DRAFT', 'FACT', 'HYPOTHESIS', 'RETRACTED')),
    -- Audit trail: supersedes previous claim (for reconciliation)
    supersedes_claim_id UUID REFERENCES claim(id),
    -- Bi-temporal
    event_time_start TIMESTAMPTZ,
    event_time_end TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Evidence binding (required for FACT claims)
CREATE TABLE IF NOT EXISTS claim_evidence (
    claim_id UUID REFERENCES claim(id),
    evidence_id UUID REFERENCES evidence(id),
    PRIMARY KEY (claim_id, evidence_id)
);

-- Edge evidence binding (edges must trace to evidence when status=FACT)
CREATE TABLE IF NOT EXISTS edge_evidence (
    edge_id UUID REFERENCES edge(id),
    evidence_id UUID REFERENCES evidence(id),
    PRIMARY KEY (edge_id, evidence_id)
);

-- ============================================================
-- CONTRADICTION TRACKING
-- ============================================================
CREATE TABLE IF NOT EXISTS contradiction (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    claim_a UUID REFERENCES claim(id),
    claim_b UUID REFERENCES claim(id),
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolution_status TEXT NOT NULL CHECK (resolution_status IN ('OPEN', 'RESOLVED', 'IGNORED')),
    resolution_notes TEXT,
    resolved_by_decision_id UUID,
    -- Resolution creates new claim that supersedes conflicting claims
    resolution_claim_id UUID REFERENCES claim(id)
);

-- ============================================================
-- GOVERNANCE POLICIES
-- ============================================================
CREATE TABLE IF NOT EXISTS policy (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type TEXT NOT NULL,
    text TEXT NOT NULL,
    conditions JSONB NOT NULL,
    effects JSONB NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ
);

-- ============================================================
-- EXCEPTION CASES (MUST be defined before missing_evidence_request)
-- ============================================================
CREATE TABLE IF NOT EXISTS "case" (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    case_type TEXT NOT NULL CHECK (case_type IN ('AIRPORT_DISRUPTION', 'LANE_DISRUPTION')),
    scope JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'BLOCKED', 'RESOLVED'))
);

-- ============================================================
-- MISSING EVIDENCE REQUESTS (first-class tracking)
-- MUST come after "case" table definition
-- ============================================================
CREATE TABLE IF NOT EXISTS missing_evidence_request (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    case_id UUID REFERENCES "case"(id),
    source_system TEXT NOT NULL,  -- Which source is missing
    request_type TEXT NOT NULL,   -- What we tried to fetch
    request_params JSONB NOT NULL DEFAULT '{}',
    reason TEXT NOT NULL,         -- Why it's missing (timeout, rate limit, etc.)
    criticality TEXT NOT NULL CHECK (criticality IN ('BLOCKING', 'DEGRADED', 'INFORMATIONAL')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    resolved_by_evidence_id UUID REFERENCES evidence(id)
);

-- ============================================================
-- DECISIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS decision (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    case_id UUID REFERENCES "case"(id),
    chosen_action_id UUID,
    rationale_claim_id UUID REFERENCES claim(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- ACTIONS (with governance)
-- ============================================================
CREATE TABLE IF NOT EXISTS action (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    case_id UUID REFERENCES "case"(id),
    type TEXT NOT NULL CHECK (type IN (
        -- Shipment-level (require booking evidence)
        'HOLD_CARGO', 'RELEASE_CARGO', 'SWITCH_GATEWAY', 'REBOOK_FLIGHT',
        'UPGRADE_SERVICE', 'NOTIFY_CUSTOMER', 'FILE_CLAIM',
        -- Posture-level (no booking required)
        'SET_POSTURE',
        -- Operational actions (system-to-system, no booking required)
        'PUBLISH_GATEWAY_ADVISORY',
        'UPDATE_BOOKING_RULES',
        'TRIGGER_REEVALUATION',
        'ESCALATE_OPS'
    )),
    args JSONB NOT NULL DEFAULT '{}',
    state TEXT NOT NULL CHECK (state IN (
        'PROPOSED', 'PENDING_APPROVAL', 'APPROVED', 'EXECUTING', 'COMPLETED', 'FAILED', 'ROLLED_BACK'
    )),
    risk_level TEXT NOT NULL CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH')),
    requires_approval BOOLEAN NOT NULL,
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- ACTION OUTCOMES
-- ============================================================
CREATE TABLE IF NOT EXISTS outcome (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    action_id UUID REFERENCES action(id),
    success BOOLEAN NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- TRACE EVENTS (for replay learning)
-- ============================================================
CREATE TABLE IF NOT EXISTS trace_event (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    case_id UUID REFERENCES "case"(id),
    seq INT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'STATE_ENTER', 'STATE_EXIT', 'TOOL_CALL', 'TOOL_RESULT', 'HANDOFF', 'GUARDRAIL_FAIL', 'BLOCKED'
    )),
    ref_type TEXT,
    ref_id UUID,
    meta JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- PLAYBOOKS (for replay learning)
-- ============================================================
CREATE TABLE IF NOT EXISTS playbook (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    pattern JSONB NOT NULL,
    action_template JSONB NOT NULL,
    stats JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS playbook_case (
    playbook_id UUID REFERENCES playbook(id),
    case_id UUID REFERENCES "case"(id),
    PRIMARY KEY (playbook_id, case_id)
);

-- ============================================================
-- EMBEDDINGS (for semantic search)
-- ============================================================
CREATE TABLE IF NOT EXISTS embedding_case (
    case_id UUID PRIMARY KEY REFERENCES "case"(id),
    embedding vector(384),
    text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
