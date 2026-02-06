# app/agents/memory/episodic.py
"""
Episodic memory - stores past case experiences.

Long-term storage for:
- Past case traces
- Successful resolution patterns
- Failure patterns to avoid
"""

from typing import List, Dict, Any, Optional
from uuid import UUID
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from ...db.engine import SessionLocal


class EpisodicMemory:
    """
    Episodic memory for past case experiences.

    Retrieves relevant past cases for pattern matching.
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

    def recall_similar_cases(
        self,
        case_type: str,
        scope: Dict[str, Any],
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Recall similar past cases.

        Args:
            case_type: Type of case
            scope: Case scope (airport, etc.)
            limit: Maximum cases to return

        Returns:
            List of similar case summaries
        """
        # Find cases with matching type and scope attributes
        airport = scope.get("airport")

        result = self.session.execute(
            text("""
                SELECT c.id, c.case_type, c.scope, c.status, c.created_at,
                       (SELECT COUNT(*) FROM trace_event WHERE case_id = c.id) as trace_count,
                       (SELECT COUNT(*) FROM action WHERE case_id = c.id AND state = 'COMPLETED') as action_count
                FROM "case" c
                WHERE c.case_type = :case_type
                  AND c.status = 'RESOLVED'
                  AND (:airport IS NULL OR c.scope->>'airport' = :airport)
                ORDER BY c.created_at DESC
                LIMIT :limit
            """),
            {"case_type": case_type, "airport": airport, "limit": limit}
        )

        cases = []
        for row in result:
            cases.append({
                "case_id": row[0],
                "case_type": row[1],
                "scope": row[2],
                "status": row[3],
                "created_at": row[4],
                "trace_count": row[5],
                "action_count": row[6],
            })

        return cases

    def recall_case_trace(self, case_id: UUID) -> List[Dict[str, Any]]:
        """
        Recall trace events for a specific case.

        Args:
            case_id: Case ID

        Returns:
            List of trace events
        """
        result = self.session.execute(
            text("""
                SELECT event_type, ref_type, ref_id, meta, created_at
                FROM trace_event
                WHERE case_id = :case_id
                ORDER BY seq
            """),
            {"case_id": case_id}
        )

        trace = []
        for row in result:
            trace.append({
                "event_type": row[0],
                "ref_type": row[1],
                "ref_id": row[2],
                "meta": row[3],
                "created_at": row[4],
            })

        return trace

    def recall_case_actions(self, case_id: UUID) -> List[Dict[str, Any]]:
        """
        Recall actions taken for a specific case.

        Args:
            case_id: Case ID

        Returns:
            List of actions with outcomes
        """
        result = self.session.execute(
            text("""
                SELECT a.type, a.args, a.state, a.risk_level,
                       o.success, o.payload
                FROM action a
                LEFT JOIN outcome o ON o.action_id = a.id
                WHERE a.case_id = :case_id
                ORDER BY a.created_at
            """),
            {"case_id": case_id}
        )

        actions = []
        for row in result:
            actions.append({
                "type": row[0],
                "args": row[1],
                "state": row[2],
                "risk_level": row[3],
                "success": row[4],
                "outcome_payload": row[5],
            })

        return actions

    def store_episode(
        self,
        case_id: UUID,
        summary: Dict[str, Any],
    ):
        """
        Store episode summary for future recall.

        Note: Individual events are stored by orchestrator.
        This stores aggregate summary for faster retrieval.
        """
        # Episodes are implicitly stored through trace_event and action tables
        # This method could store additional aggregated data if needed
        pass
