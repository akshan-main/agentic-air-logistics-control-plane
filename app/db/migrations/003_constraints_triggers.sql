-- 003_constraints_triggers.sql
-- Constraints and triggers for evidence binding and governance

-- ============================================================
-- CLAIM EVIDENCE BINDING GATE
-- ============================================================
-- Pattern: Insert claim as DRAFT/HYPOTHESIS, add claim_evidence,
--          then UPDATE to FACT. Trigger fires on promotion.
--
-- This works because:
--   1. INSERT with status='DRAFT' -> no trigger check needed
--   2. INSERT claim_evidence rows
--   3. UPDATE claim SET status='FACT' -> trigger fires, evidence exists

CREATE OR REPLACE FUNCTION enforce_claim_evidence_binding()
RETURNS TRIGGER AS $$
BEGIN
    -- Only check when status is being changed TO 'FACT'
    IF NEW.status = 'FACT' AND (TG_OP = 'INSERT' OR OLD.status != 'FACT') THEN
        IF NOT EXISTS (SELECT 1 FROM claim_evidence WHERE claim_id = NEW.id) THEN
            RAISE EXCEPTION 'Cannot promote claim to FACT without evidence binding. Add claim_evidence first.';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Fire on UPDATE only (INSERT as DRAFT, then promote)
DROP TRIGGER IF EXISTS claim_evidence_binding_gate ON claim;
CREATE TRIGGER claim_evidence_binding_gate
    BEFORE UPDATE ON claim
    FOR EACH ROW
    WHEN (NEW.status = 'FACT' AND OLD.status IS DISTINCT FROM 'FACT')
    EXECUTE FUNCTION enforce_claim_evidence_binding();

-- ============================================================
-- EDGE EVIDENCE BINDING GATE
-- ============================================================
-- Same pattern: Insert edge as DRAFT, add edge_evidence,
--               then UPDATE to FACT. Only FACT edges need evidence.
--
-- Structural/setup edges (AIRPORT->REGION) can stay as DRAFT.

CREATE OR REPLACE FUNCTION enforce_edge_evidence_binding()
RETURNS TRIGGER AS $$
BEGIN
    -- Only check when status is being changed TO 'FACT'
    IF NEW.status = 'FACT' AND (TG_OP = 'INSERT' OR OLD.status != 'FACT') THEN
        IF NOT EXISTS (SELECT 1 FROM edge_evidence WHERE edge_id = NEW.id) THEN
            RAISE EXCEPTION 'Cannot promote edge to FACT without evidence binding. Add edge_evidence first.';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Fire on UPDATE only when promoting to FACT
DROP TRIGGER IF EXISTS edge_evidence_binding_gate ON edge;
CREATE TRIGGER edge_evidence_binding_gate
    BEFORE UPDATE ON edge
    FOR EACH ROW
    WHEN (NEW.status = 'FACT' AND OLD.status IS DISTINCT FROM 'FACT')
    EXECUTE FUNCTION enforce_edge_evidence_binding();

-- ============================================================
-- NODE IMMUTABILITY GATE
-- ============================================================
-- Nodes cannot be updated. Use node_version for attribute changes.

CREATE OR REPLACE FUNCTION prevent_node_update()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Nodes are immutable. Create a new node_version instead.';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS node_immutability_gate ON node;
CREATE TRIGGER node_immutability_gate
    BEFORE UPDATE ON node
    FOR EACH ROW
    EXECUTE FUNCTION prevent_node_update();

-- ============================================================
-- ACTION GOVERNANCE GATE
-- ============================================================
-- HIGH risk actions must require approval
-- Cannot execute without approval if requires_approval is true

CREATE OR REPLACE FUNCTION enforce_action_governance()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.risk_level = 'HIGH' AND NOT NEW.requires_approval THEN
        RAISE EXCEPTION 'HIGH risk actions must require approval';
    END IF;
    IF NEW.state = 'EXECUTING' AND NEW.requires_approval AND NEW.approved_at IS NULL THEN
        RAISE EXCEPTION 'Cannot execute action requiring approval without approval';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS action_governance_gate ON action;
CREATE TRIGGER action_governance_gate
    BEFORE INSERT OR UPDATE ON action
    FOR EACH ROW
    EXECUTE FUNCTION enforce_action_governance();
