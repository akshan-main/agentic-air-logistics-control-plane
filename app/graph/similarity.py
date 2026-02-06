# app/graph/similarity.py
"""
Graph similarity metrics.

Uses neighborhood-based similarity (Jaccard) for deterministic scoring.
"""

from typing import Set, List, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import SessionLocal


def jaccard_similarity(set_a: Set, set_b: Set) -> float:
    """
    Compute Jaccard similarity between two sets.

    J(A, B) = |A ∩ B| / |A ∪ B|

    Args:
        set_a: First set
        set_b: Second set

    Returns:
        Jaccard similarity in [0, 1]
    """
    if not set_a and not set_b:
        return 0.0

    intersection = len(set_a & set_b)
    union = len(set_a | set_b)

    if union == 0:
        return 0.0

    return intersection / union


def get_node_neighborhood(
    node_id: UUID,
    hops: int = 1,
    session: Optional[Session] = None,
) -> Set[UUID]:
    """
    Get IDs of nodes in neighborhood.

    Args:
        node_id: Center node
        hops: Number of hops (default 1)
        session: Optional session

    Returns:
        Set of node IDs in neighborhood
    """
    if session is None:
        session = SessionLocal()
        owns_session = True
    else:
        owns_session = False

    try:
        if hops == 1:
            # Simple 1-hop query
            result = session.execute(
                text("""
                    SELECT DISTINCT dst FROM edge WHERE src = :id
                    UNION
                    SELECT DISTINCT src FROM edge WHERE dst = :id
                """),
                {"id": node_id}
            )
        else:
            # Multi-hop using recursive CTE
            result = session.execute(
                text("""
                    WITH RECURSIVE neighbors AS (
                        SELECT dst as node_id, 1 as depth FROM edge WHERE src = :id
                        UNION
                        SELECT src, 1 FROM edge WHERE dst = :id

                        UNION ALL

                        SELECT CASE WHEN e.src = n.node_id THEN e.dst ELSE e.src END,
                               n.depth + 1
                        FROM edge e
                        JOIN neighbors n ON (e.src = n.node_id OR e.dst = n.node_id)
                        WHERE n.depth < :hops
                    )
                    SELECT DISTINCT node_id FROM neighbors
                """),
                {"id": node_id, "hops": hops}
            )

        return {row[0] for row in result}

    finally:
        if owns_session:
            session.close()


def compute_graph_similarity(
    node_a: UUID,
    node_b: UUID,
    hops: int = 1,
    session: Optional[Session] = None,
) -> float:
    """
    Compute graph similarity between two nodes.

    Uses Jaccard similarity of neighborhoods.

    Args:
        node_a: First node ID
        node_b: Second node ID
        hops: Neighborhood radius
        session: Optional session

    Returns:
        Similarity score in [0, 1]
    """
    if session is None:
        session = SessionLocal()
        owns_session = True
    else:
        owns_session = False

    try:
        neighborhood_a = get_node_neighborhood(node_a, hops, session)
        neighborhood_b = get_node_neighborhood(node_b, hops, session)

        return jaccard_similarity(neighborhood_a, neighborhood_b)

    finally:
        if owns_session:
            session.close()


def compute_case_similarity(
    case_a_id: UUID,
    case_b_id: UUID,
    session: Optional[Session] = None,
) -> float:
    """
    Compute similarity between two cases.

    Uses Jaccard similarity of nodes referenced by each case.

    Args:
        case_a_id: First case ID
        case_b_id: Second case ID
        session: Optional session

    Returns:
        Similarity score in [0, 1]
    """
    if session is None:
        session = SessionLocal()
        owns_session = True
    else:
        owns_session = False

    try:
        # Get nodes referenced by case A (via claims and edges)
        result_a = session.execute(
            text("""
                SELECT DISTINCT subject_node_id FROM claim
                WHERE subject_node_id IS NOT NULL
                  AND id IN (
                      SELECT ref_id FROM trace_event
                      WHERE case_id = :case_id AND ref_type = 'claim'
                  )
                UNION
                SELECT DISTINCT src FROM edge
                WHERE id IN (
                    SELECT ref_id FROM trace_event
                    WHERE case_id = :case_id AND ref_type = 'edge'
                )
                UNION
                SELECT DISTINCT dst FROM edge
                WHERE id IN (
                    SELECT ref_id FROM trace_event
                    WHERE case_id = :case_id AND ref_type = 'edge'
                )
            """),
            {"case_id": case_a_id}
        )
        nodes_a = {row[0] for row in result_a if row[0] is not None}

        # Get nodes referenced by case B
        result_b = session.execute(
            text("""
                SELECT DISTINCT subject_node_id FROM claim
                WHERE subject_node_id IS NOT NULL
                  AND id IN (
                      SELECT ref_id FROM trace_event
                      WHERE case_id = :case_id AND ref_type = 'claim'
                  )
                UNION
                SELECT DISTINCT src FROM edge
                WHERE id IN (
                    SELECT ref_id FROM trace_event
                    WHERE case_id = :case_id AND ref_type = 'edge'
                )
                UNION
                SELECT DISTINCT dst FROM edge
                WHERE id IN (
                    SELECT ref_id FROM trace_event
                    WHERE case_id = :case_id AND ref_type = 'edge'
                )
            """),
            {"case_id": case_b_id}
        )
        nodes_b = {row[0] for row in result_b if row[0] is not None}

        return jaccard_similarity(nodes_a, nodes_b)

    finally:
        if owns_session:
            session.close()
