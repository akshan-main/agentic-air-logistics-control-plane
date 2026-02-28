# app/replay/playbooks.py
"""
Playbook management for replay learning.

Playbooks are learned patterns that can be reused.
"""

import json
from typing import List, Dict, Any, Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import SessionLocal
from .miner import TraceMiner
from .aging import (
    compute_decay_factor,
    compute_policy_alignment,
    compute_aged_score,
    infer_domain_from_pattern,
    policy_text_hash,
)


class PlaybookManager:
    """
    Manages playbooks for replay learning.

    Playbooks are:
    - Created from successful case patterns
    - Stored for future matching
    - Retrieved when similar cases occur
    """

    def __init__(self, session: Optional[Session] = None):
        self._session = session
        self._owns_session = session is None

    @property
    def session(self) -> Session:
        if self._session is None:
            self._session = SessionLocal()
        return self._session

    def close(self):
        if self._owns_session and self._session is not None:
            self._session.close()
            self._session = None

    def create_playbook(
        self,
        name: str,
        pattern: Dict[str, Any],
        action_template: Dict[str, Any],
        domain: Optional[str] = None,
        policy_snapshot: Optional[List[str]] = None,
        initial_stats: Optional[Dict[str, Any]] = None,
    ) -> UUID:
        """
        Create a new playbook.

        Args:
            name: Playbook name
            pattern: Matching pattern
            action_template: Action template to apply
            domain: Domain category for decay half-life (inferred if not provided)
            policy_snapshot: Active policy text hashes at creation time
            initial_stats: Initial stats (defaults to zero counters)

        Returns:
            Playbook ID
        """
        playbook_id = uuid4()
        now = datetime.now(timezone.utc)

        if domain is None:
            domain = infer_domain_from_pattern(pattern)

        if policy_snapshot is None:
            policy_snapshot = self.get_current_policy_snapshot()

        stats = initial_stats or {"use_count": 0, "success_count": 0, "success_rate": 0.0}

        self.session.execute(
            text("""
                INSERT INTO playbook (id, name, pattern, action_template, stats,
                                      created_at, last_used_at, domain, policy_snapshot)
                VALUES (:id, :name, CAST(:pattern AS jsonb), CAST(:action_template AS jsonb),
                        CAST(:stats AS jsonb), :created_at, :last_used_at, :domain,
                        CAST(:policy_snapshot AS jsonb))
            """),
            {
                "id": playbook_id,
                "name": name,
                "pattern": json.dumps(pattern),
                "action_template": json.dumps(action_template),
                "stats": json.dumps(stats),
                "created_at": now,
                "last_used_at": now,
                "domain": domain,
                "policy_snapshot": json.dumps(policy_snapshot),
            }
        )
        self.session.commit()

        return playbook_id

    def create_from_case(self, case_id: UUID, name: Optional[str] = None) -> UUID:
        """
        Create playbook from a successful case.

        Args:
            case_id: Case ID to learn from
            name: Optional playbook name

        Returns:
            Playbook ID
        """
        # Mine the case
        miner = TraceMiner(self.session)
        mined = miner.mine_case(case_id)

        if not mined:
            raise ValueError(f"Could not mine case {case_id}")

        # Build pattern — store scope values for location-specific matching
        scope = mined.get("scope", {})
        pattern = {
            "case_type": mined.get("case_type"),
            "scope_keys": list(scope.keys()),
            "scope_values": scope,
            "state_pattern": mined.get("state_pattern"),
            "evidence_sources": mined.get("evidence_pattern"),
        }

        # Build action template — normalize to List[dict] for orchestrator compatibility
        raw_actions = mined.get("action_pattern") or []
        action_sequence = [
            {"type": a, "args": {}} if isinstance(a, str) else a
            for a in raw_actions
        ]
        action_template = {
            "action_sequence": action_sequence,
        }

        # Generate name if not provided
        if not name:
            case_type = mined.get("case_type", "CASE")
            name = f"{case_type}_playbook_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Infer domain and snapshot policies
        domain = infer_domain_from_pattern(pattern)
        policy_snapshot = self.get_current_policy_snapshot()

        # Create playbook with initial stats from the source case
        playbook_id = self.create_playbook(
            name, pattern, action_template,
            domain=domain, policy_snapshot=policy_snapshot,
            initial_stats={"use_count": 1, "success_count": 1, "success_rate": 1.0},
        )

        # Link to case
        self.session.execute(
            text("""
                INSERT INTO playbook_case (playbook_id, case_id)
                VALUES (:playbook_id, :case_id)
            """),
            {"playbook_id": playbook_id, "case_id": case_id}
        )
        self.session.commit()

        return playbook_id

    def find_matching(
        self,
        case_type: str,
        scope: Dict[str, Any],
        limit: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Find playbooks matching case criteria, ranked by aged relevance.

        Ranking: match_score * aged_score, where
        aged_score = success_rate * decay_factor * policy_alignment

        Args:
            case_type: Case type
            scope: Case scope
            limit: Maximum playbooks to return

        Returns:
            List of matching playbooks with aging metadata
        """
        result = self.session.execute(
            text("""
                SELECT id, name, pattern, action_template, stats,
                       created_at, last_used_at, domain, policy_snapshot
                FROM playbook
                WHERE pattern->>'case_type' = :case_type
                ORDER BY created_at DESC
            """),
            {"case_type": case_type}
        )

        current_snapshot = self.get_current_policy_snapshot()

        playbooks = []
        for row in result:
            pattern = row[2]
            stats = row[4] or {}
            created_at = row[5]
            last_used_at = row[6]
            domain = row[7] or "operational"
            pb_snapshot = row[8] or []

            match_score = self._compute_match_score(pattern, scope)

            success_rate = float(stats.get("success_rate", 0))
            use_count = int(stats.get("use_count", 0))
            decay_factor = compute_decay_factor(created_at, last_used_at, domain)
            policy_align = compute_policy_alignment(pb_snapshot, current_snapshot)
            aged_score = compute_aged_score(success_rate, decay_factor, policy_align, use_count)

            playbooks.append({
                "playbook_id": row[0],
                "name": row[1],
                "pattern": pattern,
                "action_template": row[3],
                "stats": stats,
                "match_score": match_score,
                "aged_score": aged_score,
                "decay_factor": decay_factor,
                "policy_alignment": policy_align,
            })

        # Sort by composite: match_score * aged_score
        playbooks.sort(key=lambda p: -(p["match_score"] * p["aged_score"]))

        return playbooks[:limit]

    def get_playbook(self, playbook_id: UUID) -> Optional[Dict[str, Any]]:
        """Get playbook by ID, including aging metadata."""
        result = self.session.execute(
            text("""
                SELECT id, name, pattern, action_template, stats,
                       created_at, last_used_at, domain, policy_snapshot
                FROM playbook
                WHERE id = :id
            """),
            {"id": playbook_id}
        )

        row = result.fetchone()
        if row:
            return {
                "playbook_id": row[0],
                "name": row[1],
                "pattern": row[2],
                "action_template": row[3],
                "stats": row[4],
                "created_at": row[5],
                "last_used_at": row[6],
                "domain": row[7] or "operational",
                "policy_snapshot": row[8] or [],
            }
        return None

    def record_usage(self, playbook_id: UUID, case_id: UUID, success: bool):
        """
        Record playbook usage and refresh last_used_at.

        Args:
            playbook_id: Playbook that was used
            case_id: Case it was used for
            success: Whether use was successful
        """
        now = datetime.now(timezone.utc)

        # Link case to playbook
        self.session.execute(
            text("""
                INSERT INTO playbook_case (playbook_id, case_id)
                VALUES (:playbook_id, :case_id)
                ON CONFLICT DO NOTHING
            """),
            {"playbook_id": playbook_id, "case_id": case_id}
        )

        # Update stats; only refresh last_used_at on success (failures shouldn't keep bad playbooks fresh)
        success_inc = 1 if success else 0
        if success:
            self.session.execute(
                text("""
                    UPDATE playbook
                    SET stats = jsonb_set(
                        jsonb_set(
                            stats,
                            '{use_count}',
                            ((COALESCE(stats->>'use_count', '0')::int + 1)::text)::jsonb
                        ),
                        '{success_count}',
                        ((COALESCE(stats->>'success_count', '0')::int + :success)::text)::jsonb
                    ),
                    last_used_at = :now
                    WHERE id = :id
                """),
                {"id": playbook_id, "success": success_inc, "now": now}
            )
        else:
            self.session.execute(
                text("""
                    UPDATE playbook
                    SET stats = jsonb_set(
                        jsonb_set(
                            stats,
                            '{use_count}',
                            ((COALESCE(stats->>'use_count', '0')::int + 1)::text)::jsonb
                        ),
                        '{success_count}',
                        ((COALESCE(stats->>'success_count', '0')::int + :success)::text)::jsonb
                    )
                    WHERE id = :id
                """),
                {"id": playbook_id, "success": success_inc}
            )

        # Update success rate
        self.session.execute(
            text("""
                UPDATE playbook
                SET stats = jsonb_set(
                    stats,
                    '{success_rate}',
                    (
                        COALESCE(stats->>'success_count', '0')::float /
                        GREATEST(COALESCE(stats->>'use_count', '1')::float, 1)
                    )::text::jsonb
                )
                WHERE id = :id
            """),
            {"id": playbook_id}
        )

        self.session.commit()

    def _compute_match_score(
        self,
        pattern: Dict[str, Any],
        scope: Dict[str, Any],
    ) -> float:
        """Compute match score between pattern and scope."""
        if not pattern:
            return 0.0

        # Prefer value-level matching when scope_values is available
        scope_values = pattern.get("scope_values")
        if scope_values:
            matches = sum(
                1 for k, v in scope_values.items()
                if scope.get(k) == v
            )
            return matches / len(scope_values)

        # Fallback to key-overlap for legacy playbooks without scope_values
        pattern_keys = set(pattern.get("scope_keys", []))
        scope_keys = set(scope.keys())

        if not pattern_keys:
            return 0.5  # No scope constraints

        overlap = len(pattern_keys & scope_keys)
        return overlap / len(pattern_keys)

    def get_current_policy_snapshot(self) -> List[str]:
        """Get current active policy text hashes for snapshot comparison."""
        result = self.session.execute(
            text("""
                SELECT text FROM policy
                WHERE effective_from <= NOW()
                  AND (effective_to IS NULL OR effective_to > NOW())
                ORDER BY text
            """)
        )
        return sorted([policy_text_hash(row[0]) for row in result])


def create_playbook_from_case(
    case_id: UUID,
    name: Optional[str] = None,
    session: Optional[Session] = None,
) -> UUID:
    """
    Convenience function to create playbook from case.

    Args:
        case_id: Case ID
        name: Optional playbook name
        session: Optional database session

    Returns:
        Playbook ID
    """
    manager = PlaybookManager(session)
    try:
        return manager.create_from_case(case_id, name)
    finally:
        if session is None:
            manager.close()
