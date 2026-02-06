# app/agents/memory/semantic.py
"""
Semantic memory - stores general knowledge and patterns.

Long-term storage for:
- Playbooks and patterns
- Domain knowledge
- Learned heuristics
"""

from typing import List, Dict, Any, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from ...db.engine import SessionLocal


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
        Find playbooks similar to current conditions.

        Args:
            case_type: Type of case
            conditions: Current conditions to match
            limit: Maximum playbooks to return

        Returns:
            List of matching playbooks
        """
        result = self.session.execute(
            text("""
                SELECT p.id, p.name, p.pattern, p.action_template, p.stats
                FROM playbook p
                WHERE p.pattern->>'case_type' = :case_type
                ORDER BY (p.stats->>'success_rate')::float DESC NULLS LAST
                LIMIT :limit
            """),
            {"case_type": case_type, "limit": limit}
        )

        playbooks = []
        for row in result:
            playbook = {
                "playbook_id": row[0],
                "name": row[1],
                "pattern": row[2],
                "action_template": row[3],
                "stats": row[4],
            }

            # Check pattern match
            if self._pattern_matches(row[2], conditions):
                playbook["match_score"] = self._compute_match_score(row[2], conditions)
                playbooks.append(playbook)

        # Sort by match score
        playbooks.sort(key=lambda p: -p.get("match_score", 0))

        return playbooks[:limit]

    def get_playbook(self, playbook_id: UUID) -> Optional[Dict[str, Any]]:
        """
        Get playbook by ID.

        Args:
            playbook_id: Playbook ID

        Returns:
            Playbook dict or None
        """
        result = self.session.execute(
            text("""
                SELECT id, name, pattern, action_template, stats
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
        from uuid import uuid4

        playbook_id = uuid4()

        self.session.execute(
            text("""
                INSERT INTO playbook (id, name, pattern, action_template, stats, created_at)
                VALUES (:id, :name, :pattern, :action_template, :stats, :created_at)
            """),
            {
                "id": playbook_id,
                "name": name,
                "pattern": pattern,
                "action_template": action_template,
                "stats": stats or {},
                "created_at": datetime.now(timezone.utc),
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
        Update playbook statistics after use.

        Args:
            playbook_id: Playbook ID
            success: Whether playbook use was successful
        """
        # Increment counters
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
            {"id": playbook_id, "success_inc": 1 if success else 0}
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


# Import for type hints
from datetime import datetime, timezone
