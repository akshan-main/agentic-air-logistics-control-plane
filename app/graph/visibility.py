# app/graph/visibility.py
"""
CANONICAL bi-temporal visibility predicates.

SINGLE SOURCE OF TRUTH for temporal filtering.
Use these predicates EVERYWHERE - no inline temporal logic.

An edge is visible at a point in time if:
1. Its event time window contains the query event time
2. It was ingested before the query ingest time
3. Its validity window contains the query event time
"""

from datetime import datetime
from typing import Dict, Any


def edge_visible_at(
    at_event_time: datetime,
    at_ingest_time: datetime,
    include_supersession: bool = True,
    table_alias: str = "edge"
) -> str:
    """
    Returns SQL WHERE clause for edge visibility.

    CANONICAL PREDICATE - Reuse this EXACT predicate in all queries.
    Do not copy-paste or create variations.

    Args:
        at_event_time: Point in event time to query
        at_ingest_time: Point in ingest time (what we knew at this time)
        include_supersession: If True, filter out superseded edges
        table_alias: Table alias to use (default: "edge")

    Returns:
        SQL WHERE clause string (uses :at_event_time, :at_ingest_time parameters)

    Usage:
        visibility = edge_visible_at(event_time, ingest_time)
        query = f"SELECT * FROM edge WHERE {visibility}"
        session.execute(query, {"at_event_time": event_time, "at_ingest_time": ingest_time})
    """
    a = table_alias
    base_visibility = f"""
        ({a}.event_time_start IS NULL OR {a}.event_time_start <= :at_event_time)
        AND ({a}.event_time_end IS NULL OR {a}.event_time_end > :at_event_time)
        AND {a}.ingested_at <= :at_ingest_time
        AND ({a}.valid_from IS NULL OR {a}.valid_from <= :at_event_time)
        AND ({a}.valid_to IS NULL OR {a}.valid_to > :at_event_time)
    """

    if include_supersession:
        # Exclude edges that have been superseded by a newer edge
        # An edge is superseded if another edge's supersedes_edge_id points to it
        return base_visibility + f"""
        AND NOT EXISTS (
            SELECT 1 FROM edge e_newer
            WHERE e_newer.supersedes_edge_id = {a}.id
              AND e_newer.ingested_at <= :at_ingest_time
        )
        """
    return base_visibility


def node_version_visible_at(at_event_time: datetime) -> str:
    """
    Returns SQL WHERE clause for node_version visibility.

    CANONICAL PREDICATE - Reuse this EXACT predicate in all queries.

    Args:
        at_event_time: Point in event time to query

    Returns:
        SQL WHERE clause string (uses :at_event_time parameter)

    Usage:
        visibility = node_version_visible_at(event_time)
        query = f"SELECT * FROM node_version WHERE node_id = :id AND {visibility}"
    """
    return """
        valid_from <= :at_event_time
        AND (valid_to IS NULL OR valid_to > :at_event_time)
    """


def claim_visible_at(at_event_time: datetime, at_ingest_time: datetime) -> str:
    """
    Returns SQL WHERE clause for claim visibility.

    CANONICAL PREDICATE - Reuse this EXACT predicate in all queries.

    Args:
        at_event_time: Point in event time to query
        at_ingest_time: Point in ingest time

    Returns:
        SQL WHERE clause string
    """
    return """
        (event_time_start IS NULL OR event_time_start <= :at_event_time)
        AND (event_time_end IS NULL OR event_time_end > :at_event_time)
        AND ingested_at <= :at_ingest_time
        AND status != 'RETRACTED'
    """


# Parameter names used by visibility predicates
VISIBILITY_PARAMS = {
    "at_event_time": "datetime - The event time point to query",
    "at_ingest_time": "datetime - The ingest time point (what we knew then)",
}


def get_visibility_params(
    at_event_time: datetime,
    at_ingest_time: datetime
) -> Dict[str, Any]:
    """
    Get parameter dict for visibility predicates.

    Args:
        at_event_time: Event time to query
        at_ingest_time: Ingest time to query

    Returns:
        Dict with properly named parameters
    """
    return {
        "at_event_time": at_event_time,
        "at_ingest_time": at_ingest_time,
    }
