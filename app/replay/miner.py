# app/replay/miner.py
"""
Trace mining for replay learning.

Extracts patterns from successful case traces.
"""

from typing import List, Dict, Any, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import SessionLocal


class TraceMiner:
    """
    Mines traces from completed cases.

    Extracts:
    - State transition patterns
    - Action sequences
    - Evidence gathering patterns
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

    def mine_case(self, case_id: UUID) -> Dict[str, Any]:
        """
        Mine trace from a single case.

        Args:
            case_id: Case ID

        Returns:
            Mined pattern dict
        """
        # Get case info
        case_info = self._get_case_info(case_id)
        if not case_info:
            return {}

        # Get trace events
        trace = self._get_trace(case_id)

        # Get actions
        actions = self._get_actions(case_id)

        # Extract patterns
        state_pattern = self._extract_state_pattern(trace)
        action_pattern = self._extract_action_pattern(actions)
        evidence_pattern = self._extract_evidence_pattern(trace)

        return {
            "case_id": str(case_id),
            "case_type": case_info.get("case_type"),
            "scope": case_info.get("scope"),
            "status": case_info.get("status"),
            "state_pattern": state_pattern,
            "action_pattern": action_pattern,
            "evidence_pattern": evidence_pattern,
            "trace_length": len(trace),
            "action_count": len(actions),
        }

    def mine_successful_cases(
        self,
        case_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Mine patterns from successful cases.

        Args:
            case_type: Optional filter by case type
            limit: Maximum cases to mine

        Returns:
            List of mined patterns
        """
        # Find successful cases
        query = """
            SELECT id FROM "case"
            WHERE status = 'RESOLVED'
        """
        params = {"limit": limit}

        if case_type:
            query += " AND case_type = :case_type"
            params["case_type"] = case_type

        query += " ORDER BY created_at DESC LIMIT :limit"

        result = self.session.execute(text(query), params)
        case_ids = [row[0] for row in result]

        # Mine each case
        patterns = []
        for case_id in case_ids:
            pattern = self.mine_case(case_id)
            if pattern:
                patterns.append(pattern)

        return patterns

    def _get_case_info(self, case_id: UUID) -> Optional[Dict[str, Any]]:
        """Get case information."""
        result = self.session.execute(
            text('SELECT case_type, scope, status FROM "case" WHERE id = :id'),
            {"id": case_id}
        )
        row = result.fetchone()
        if row:
            return {
                "case_type": row[0],
                "scope": row[1],
                "status": row[2],
            }
        return None

    def _get_trace(self, case_id: UUID) -> List[Dict[str, Any]]:
        """Get trace events for case."""
        result = self.session.execute(
            text("""
                SELECT event_type, ref_type, ref_id, meta, created_at
                FROM trace_event
                WHERE case_id = :case_id
                ORDER BY seq
            """),
            {"case_id": case_id}
        )

        return [
            {
                "event_type": row[0],
                "ref_type": row[1],
                "ref_id": row[2],
                "meta": row[3],
                "created_at": row[4],
            }
            for row in result
        ]

    def _get_actions(self, case_id: UUID) -> List[Dict[str, Any]]:
        """Get actions for case."""
        result = self.session.execute(
            text("""
                SELECT type, args, state, risk_level
                FROM action
                WHERE case_id = :case_id
                ORDER BY created_at
            """),
            {"case_id": case_id}
        )

        return [
            {
                "type": row[0],
                "args": row[1],
                "state": row[2],
                "risk_level": row[3],
            }
            for row in result
        ]

    def _extract_state_pattern(self, trace: List[Dict[str, Any]]) -> List[str]:
        """Extract state transition pattern from trace.

        FIXED: State names are stored in meta["state"], not ref_id.
        The orchestrator stores ref_type='state' and the actual state
        name in the meta JSON field.
        """
        states = []
        for event in trace:
            if event["event_type"] == "STATE_ENTER":
                # State name is in meta["state"], not ref_id
                meta = event.get("meta") or {}
                state_name = meta.get("state")
                if state_name:
                    states.append(state_name)
                elif event.get("ref_id"):
                    # Fallback to ref_id for backwards compatibility
                    states.append(str(event["ref_id"]))
        return states

    def _extract_action_pattern(self, actions: List[Dict[str, Any]]) -> List[str]:
        """Extract action type pattern."""
        return [a["type"] for a in actions if a["state"] == "COMPLETED"]

    def _extract_evidence_pattern(self, trace: List[Dict[str, Any]]) -> List[str]:
        """Extract evidence source pattern."""
        sources = []
        for event in trace:
            if event["event_type"] == "TOOL_RESULT":
                source = event.get("meta", {}).get("source_system")
                if source and source not in sources:
                    sources.append(source)
        return sources


def mine_case_trace(case_id: UUID, session: Optional[Session] = None) -> Dict[str, Any]:
    """
    Convenience function to mine a case trace.

    Args:
        case_id: Case ID
        session: Optional database session

    Returns:
        Mined pattern dict
    """
    miner = TraceMiner(session)
    try:
        return miner.mine_case(case_id)
    finally:
        if session is None:
            miner.close()
