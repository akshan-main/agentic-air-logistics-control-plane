-- 005_seed_policies.sql
-- Seed operational policies for gateway posture decisions
-- NON-DESTRUCTIVE: uses ON CONFLICT to skip existing policies.
-- Custom policies added by operators are preserved.

-- Ensure unique index exists for ON CONFLICT
CREATE UNIQUE INDEX IF NOT EXISTS idx_policy_text_unique ON policy(text);

-- Policy 1: Open contradictions require evidence resolution
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'evidence_requirement',
    'Open contradictions require evidence resolution before posture decision',
    '{"has_contradictions": true}'::jsonb,
    '{"action": "needs_evidence", "description": "Resolve contradicting signals before proceeding"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 2: HIGH/CRITICAL risk actions require approval
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'approval_requirement',
    'HIGH or CRITICAL risk actions require human approval',
    '{"risk_level": ["HIGH", "CRITICAL"]}'::jsonb,
    '{"action": "requires_approval", "description": "Escalate to duty manager for approval"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 3: Shipment actions require booking evidence
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'evidence_requirement',
    'Shipment-level actions require booking evidence',
    '{"action_type": "shipment"}'::jsonb,
    '{"action": "block_without_booking", "description": "Cannot modify shipments without booking data"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 4: CRITICAL risk blocks ACCEPT posture
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'threshold',
    'CRITICAL risk level prohibits ACCEPT posture',
    '{"risk_level": "CRITICAL", "posture": "ACCEPT"}'::jsonb,
    '{"action": "block", "description": "Cannot accept new bookings during critical disruptions"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 5: LOW risk allows ACCEPT posture
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'operational',
    'LOW risk allows ACCEPT posture for normal operations',
    '{"risk_level": "LOW"}'::jsonb,
    '{"action": "allow", "description": "Normal operations, accept new bookings"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 6: MEDIUM risk allows RESTRICT posture
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'operational',
    'MEDIUM risk allows RESTRICT posture',
    '{"risk_level": "MEDIUM"}'::jsonb,
    '{"action": "allow", "description": "Restrict premium SLAs, allow standard bookings"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 7: Minimum evidence requirement
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'evidence_requirement',
    'Posture changes require at least 2 evidence sources',
    '{"min_evidence": 2}'::jsonb,
    '{"action": "allow", "description": "Sufficient evidence for posture decision"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 8: Weather data freshness
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'operational',
    'Weather data must be available for disruption assessment',
    '{"has_weather": true}'::jsonb,
    '{"action": "allow", "description": "Weather conditions verified"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 9: HIGH risk requires HOLD or ESCALATE
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'threshold',
    'HIGH risk recommends HOLD or ESCALATE posture',
    '{"risk_level": "HIGH"}'::jsonb,
    '{"action": "allow", "description": "Hold tendering until situation clarifies"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 10: IFR conditions trigger review
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'operational',
    'IFR/LIFR weather conditions trigger posture review',
    '{"flight_category": ["IFR", "LIFR"]}'::jsonb,
    '{"action": "allow", "description": "Weather impacts assessed in posture decision"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 11: Premium SLA posture changes within 48h require approval (from builtin_policies.py)
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'approval_requirement',
    'Premium SLA posture changes within 48h require approval',
    '{"service_tier": "PREMIUM", "hours_until_deadline": {"op": "<", "value": 48}, "action_type": "SET_POSTURE"}'::jsonb,
    '{"action": "require_approval"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 12: Cost threshold requires approval (from builtin_policies.py)
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'approval_requirement',
    'Actions with cost exposure above $10,000 require approval',
    '{"estimated_cost": {"op": ">", "value": 10000}}'::jsonb,
    '{"action": "require_approval"}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Policy 13: Open contradictions + stale evidence blocks ACCEPT (from builtin_policies.py)
INSERT INTO policy (id, type, text, conditions, effects, effective_from)
VALUES (
    uuid_generate_v4(),
    'posture_constraint',
    'Open contradictions with stale evidence require RESTRICT posture',
    '{"has_contradictions": true, "has_stale_evidence": true, "proposed_posture": "ACCEPT"}'::jsonb,
    '{"action": "block", "params": {"reason": "Cannot ACCEPT with open contradictions"}}'::jsonb,
    NOW()
) ON CONFLICT (text) DO NOTHING;

-- Verify policies were seeded
SELECT COUNT(*) as policy_count FROM policy;
