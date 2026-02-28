# app/agents/memory/semantic.py
"""
Semantic memory - stores general knowledge and patterns.

Long-term storage for:
- Playbooks and patterns
- Domain knowledge
- Learned heuristics
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from ...db.engine import SessionLocal
from ...replay.aging import (
    compute_decay_factor,
    compute_policy_alignment,
    compute_aged_score,
    policy_text_hash,
)


class SemanticMemory:
    """
    Semantic memory for general knowledge.

    Stores and retrieves:
    - Playbooks (learned patterns)
    - Domain rules
    - Heuristics
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

    def find_similar_playbooks(
        self,
        case_type: str,
        conditions: Dict[str, Any],
        limit: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Find playbooks similar to current conditions, ranked by aged relevance.

        Args:
            case_type: Type of case
            conditions: Current conditions to match
            limit: Maximum playbooks to return

        Returns:
            List of matching playbooks with aging metadata
        """
        result = self.session.execute(
            text("""
                SELECT p.id, p.name, p.pattern, p.action_template, p.stats,
                       p.created_at, p.last_used_at, p.domain, p.policy_snapshot
                FROM playbook p
                WHERE p.pattern->>'case_type' = :case_type
                ORDER BY p.created_at DESC
            """),
            {"case_type": case_type}
        )

        current_snapshot = self._get_current_policy_snapshot()

        playbooks = []
        for row in result:
            pattern = row[2]
            stats = row[4] or {}
            created_at = row[5]
            last_used_at = row[6]
            domain = row[7] or "operational"
            pb_snapshot = row[8] or []

            # Check pattern match
            if not self._pattern_matches(pattern, conditions):
                continue

            match_score = self._compute_match_score(pattern, conditions)

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
        playbooks.sort(key=lambda p: -(p.get("match_score", 0) * p.get("aged_score", 0)))

        return playbooks[:limit]

    def get_playbook(self, playbook_id: UUID) -> Optional[Dict[str, Any]]:
        """
        Get playbook by ID, including aging metadata.

        Args:
            playbook_id: Playbook ID

        Returns:
            Playbook dict or None
        """
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

    def store_playbook(
        self,
        name: str,
        pattern: Dict[str, Any],
        action_template: Dict[str, Any],
        stats: Optional[Dict[str, Any]] = None,
    ) -> UUID:
        """
        Store a new playbook.

        Args:
            name: Playbook name
            pattern: Matching pattern
            action_template: Action template
            stats: Initial statistics

        Returns:
            Playbook ID
        """
        import json
        from uuid import uuid4

        playbook_id = uuid4()
        now = datetime.now(timezone.utc)

        from ...replay.aging import infer_domain_from_pattern

        domain = infer_domain_from_pattern(pattern)
        policy_snapshot = self._get_current_policy_snapshot()

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
                "stats": json.dumps(stats or {}),
                "created_at": now,
                "last_used_at": now,
                "domain": domain,
                "policy_snapshot": json.dumps(policy_snapshot),
            }
        )
        self.session.commit()

        return playbook_id

    def update_playbook_stats(
        self,
        playbook_id: UUID,
        success: bool,
    ):
        """
        Update playbook statistics. Only refresh last_used_at on success.

        Args:
            playbook_id: Playbook ID
            success: Whether playbook use was successful
        """
        now = datetime.now(timezone.utc)
        success_inc = 1 if success else 0

        # Increment counters; only refresh last_used_at on success
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
                        ((COALESCE(stats->>'success_count', '0')::int + :success_inc)::text)::jsonb
                    ),
                    last_used_at = :now
                    WHERE id = :id
                """),
                {"id": playbook_id, "success_inc": success_inc, "now": now}
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
                        ((COALESCE(stats->>'success_count', '0')::int + :success_inc)::text)::jsonb
                    )
                    WHERE id = :id
                """),
                {"id": playbook_id, "success_inc": success_inc}
            )

        # Update success rate
        self.session.execute(
            text("""
                UPDATE playbook
                SET stats = jsonb_set(
                    stats,
                    '{success_rate}',
                    (
                        (COALESCE(stats->>'success_count', '0')::float /
                         GREATEST(COALESCE(stats->>'use_count', '1')::float, 1))::text
                    )::jsonb
                )
                WHERE id = :id
            """),
            {"id": playbook_id}
        )

        self.session.commit()

    def _pattern_matches(
        self,
        pattern: Dict[str, Any],
        conditions: Dict[str, Any],
    ) -> bool:
        """Check if pattern matches conditions."""
        # Simple key-value matching
        for key, value in pattern.items():
            if key in conditions:
                if conditions[key] != value:
                    return False
        return True

    def _compute_match_score(
        self,
        pattern: Dict[str, Any],
        conditions: Dict[str, Any],
    ) -> float:
        """Compute match score between pattern and conditions."""
        if not pattern:
            return 0.0

        matches = sum(1 for k, v in pattern.items() if conditions.get(k) == v)
        return matches / len(pattern)

    def _get_current_policy_snapshot(self) -> List[str]:
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
