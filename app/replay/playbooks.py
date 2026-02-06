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
    ) -> UUID:
        """
        Create a new playbook.

        Args:
            name: Playbook name
            pattern: Matching pattern
            action_template: Action template to apply

        Returns:
            Playbook ID
        """
        playbook_id = uuid4()

        # FIXED: JSONB columns require JSON strings with explicit CAST
        self.session.execute(
            text("""
                INSERT INTO playbook (id, name, pattern, action_template, stats, created_at)
                VALUES (:id, :name, CAST(:pattern AS jsonb), CAST(:action_template AS jsonb),
                        CAST(:stats AS jsonb), :created_at)
            """),
            {
                "id": playbook_id,
                "name": name,
                "pattern": json.dumps(pattern),
                "action_template": json.dumps(action_template),
                "stats": json.dumps({"use_count": 0, "success_count": 0}),
                "created_at": datetime.now(timezone.utc),
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

        # Build pattern
        pattern = {
            "case_type": mined.get("case_type"),
            "scope_keys": list(mined.get("scope", {}).keys()),
            "state_pattern": mined.get("state_pattern"),
            "evidence_sources": mined.get("evidence_pattern"),
        }

        # Build action template
        action_template = {
            "action_sequence": mined.get("action_pattern"),
        }

        # Generate name if not provided
        if not name:
            case_type = mined.get("case_type", "CASE")
            name = f"{case_type}_playbook_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Create playbook
        playbook_id = self.create_playbook(name, pattern, action_template)

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
        Find playbooks matching case criteria.

        Args:
            case_type: Case type
            scope: Case scope
            limit: Maximum playbooks to return

        Returns:
            List of matching playbooks
        """
        result = self.session.execute(
            text("""
                SELECT id, name, pattern, action_template, stats
                FROM playbook
                WHERE pattern->>'case_type' = :case_type
                ORDER BY (stats->>'success_rate')::float DESC NULLS LAST
                LIMIT :limit
            """),
            {"case_type": case_type, "limit": limit * 2}
        )

        playbooks = []
        for row in result:
            pattern = row[2]
            match_score = self._compute_match_score(pattern, scope)

            playbooks.append({
                "playbook_id": row[0],
                "name": row[1],
                "pattern": pattern,
                "action_template": row[3],
                "stats": row[4],
                "match_score": match_score,
            })

        # Sort by match score
        playbooks.sort(key=lambda p: -p["match_score"])

        return playbooks[:limit]

    def get_playbook(self, playbook_id: UUID) -> Optional[Dict[str, Any]]:
        """Get playbook by ID."""
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

    def record_usage(self, playbook_id: UUID, case_id: UUID, success: bool):
        """
        Record playbook usage.

        Args:
            playbook_id: Playbook that was used
            case_id: Case it was used for
            success: Whether use was successful
        """
        # Link case to playbook
        self.session.execute(
            text("""
                INSERT INTO playbook_case (playbook_id, case_id)
                VALUES (:playbook_id, :case_id)
                ON CONFLICT DO NOTHING
            """),
            {"playbook_id": playbook_id, "case_id": case_id}
        )

        # Update stats
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
            {"id": playbook_id, "success": 1 if success else 0}
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

        # Check scope key overlap
        pattern_keys = set(pattern.get("scope_keys", []))
        scope_keys = set(scope.keys())

        if not pattern_keys:
            return 0.5  # No scope constraints

        overlap = len(pattern_keys & scope_keys)
        return overlap / len(pattern_keys)


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
