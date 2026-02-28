-- 007_playbook_aging.sql
-- Add precedent aging support to playbooks.
-- Non-destructive: ALTER TABLE ADD COLUMN, no data loss.

-- Track when a playbook was last applied to a case
ALTER TABLE playbook ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;

-- Snapshot which policy text hashes were active when this playbook was created
-- Stored as a sorted JSON array of 12-char hex hashes for drift detection
ALTER TABLE playbook ADD COLUMN IF NOT EXISTS policy_snapshot JSONB NOT NULL DEFAULT '[]';

-- Domain category determines half-life for decay calculation
-- Values: 'weather' (30d), 'operational' (90d), 'customs' (180d)
ALTER TABLE playbook ADD COLUMN IF NOT EXISTS domain TEXT NOT NULL DEFAULT 'operational';

-- Backfill last_used_at from created_at for existing playbooks
UPDATE playbook SET last_used_at = created_at WHERE last_used_at IS NULL;

-- Index for efficient decay-adjusted queries
CREATE INDEX IF NOT EXISTS idx_playbook_last_used ON playbook(last_used_at DESC NULLS LAST);
