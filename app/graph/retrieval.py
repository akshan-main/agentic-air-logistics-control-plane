# app/graph/retrieval.py
"""
Hybrid retrieval: semantic + keyword + graph.

DETERMINISTIC ranking formula:
    final_score = 0.5 * semantic_score + 0.3 * keyword_score + 0.2 * graph_score

All scores normalized to [0, 1].
Tie-breaking by ID for determinism.
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import SessionLocal

# DETERMINISTIC WEIGHTS - do not change without updating tests
WEIGHTS = {
    "semantic": 0.5,   # pgvector cosine similarity
    "keyword": 0.3,    # tsvector rank
    "graph": 0.2,      # neighborhood Jaccard similarity
}


@dataclass
class HybridSearchResult:
    """Single result from hybrid search."""
    id: UUID
    case_id: UUID
    text: str
    semantic_score: float
    keyword_score: float
    graph_score: float
    final_score: float

    @classmethod
    def compute_final_score(
        cls,
        semantic: float,
        keyword: float,
        graph: float
    ) -> float:
        """
        Compute deterministic final score.

        Formula: 0.5 * semantic + 0.3 * keyword + 0.2 * graph
        """
        return (
            WEIGHTS["semantic"] * semantic +
            WEIGHTS["keyword"] * keyword +
            WEIGHTS["graph"] * graph
        )


def hybrid_search(
    query_text: str,
    case_context: Optional[Dict[str, Any]] = None,
    limit: int = 20,
    session: Optional[Session] = None,
) -> List[HybridSearchResult]:
    """
    Hybrid search with deterministic ranking.

    Combines:
    1. Semantic: pgvector cosine similarity (normalized to [0, 1])
    2. Keyword: tsvector ts_rank (normalized to [0, 1])
    3. Graph: Jaccard similarity of neighborhood (already [0, 1])

    Final score = 0.5 * semantic + 0.3 * keyword + 0.2 * graph

    Args:
        query_text: Search query
        case_context: Optional context for graph similarity
        limit: Maximum results to return

    Returns:
        List of HybridSearchResult, sorted by final_score desc, id asc
    """
    if session is None:
        session = SessionLocal()
        owns_session = True
    else:
        owns_session = False

    try:
        # Get results from all three sources
        semantic_results = _semantic_search(query_text, limit * 2, session)
        keyword_results = _keyword_search(query_text, limit * 2, session)
        graph_results = _graph_similarity_search(case_context, limit * 2, session)

        # Normalize scores
        _normalize_semantic(semantic_results)
        _normalize_keyword(keyword_results)
        # Graph scores are already [0, 1] (Jaccard)

        # Merge by case_id
        merged = _merge_results(semantic_results, keyword_results, graph_results)

        # Compute final scores
        results = []
        for case_id, scores in merged.items():
            final_score = HybridSearchResult.compute_final_score(
                scores.get("semantic", 0.0),
                scores.get("keyword", 0.0),
                scores.get("graph", 0.0),
            )
            results.append(HybridSearchResult(
                id=scores.get("id", case_id),
                case_id=case_id,
                text=scores.get("text", ""),
                semantic_score=scores.get("semantic", 0.0),
                keyword_score=scores.get("keyword", 0.0),
                graph_score=scores.get("graph", 0.0),
                final_score=final_score,
            ))

        # Sort deterministically: final_score desc, then id asc for tie-breaking
        results.sort(key=lambda r: (-r.final_score, str(r.id)))

        return results[:limit]

    finally:
        if owns_session:
            session.close()


def _get_query_embedding(query_text: str) -> List[float]:
    """
    Generate embedding for query text using sentence-transformers.

    Uses the same model (all-MiniLM-L6-v2, 384 dims) as case embeddings.
    """
    try:
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer('all-MiniLM-L6-v2')
        embedding = model.encode(query_text, convert_to_numpy=True)
        return embedding.tolist()
    except ImportError:
        # Fallback: return None and use text-based search
        return None
    except Exception:
        return None


def _semantic_search(
    query_text: str,
    limit: int,
    session: Session
) -> List[Dict[str, Any]]:
    """
    Semantic search using pgvector.

    FIXED: Now properly embeds the query text instead of using an existing row's embedding.
    Returns cosine similarity (in range [-1, 1]).
    """
    # Generate embedding for the query
    query_embedding = _get_query_embedding(query_text)

    if query_embedding is None:
        # Fallback to text-based search if embedding fails
        result = session.execute(
            text("""
                SELECT case_id, text, 0.5 as cosine_sim
                FROM embedding_case
                WHERE text ILIKE :query
                LIMIT :limit
            """),
            {"query": f"%{query_text}%", "limit": limit}
        )
    else:
        # Use proper vector similarity with the embedded query
        # Convert embedding to string format for PostgreSQL
        embedding_str = "[" + ",".join(str(x) for x in query_embedding) + "]"
        result = session.execute(
            text("""
                SELECT case_id, text,
                       1 - (embedding <=> :query_embedding::vector) as cosine_sim
                FROM embedding_case
                WHERE embedding IS NOT NULL
                ORDER BY embedding <=> :query_embedding::vector
                LIMIT :limit
            """),
            {"query_embedding": embedding_str, "limit": limit}
        )

    results = []
    for row in result:
        results.append({
            "case_id": row[0],
            "text": row[1],
            "cosine_sim": row[2] if row[2] is not None else 0.0,
        })
    return results


def _keyword_search(
    query_text: str,
    limit: int,
    session: Session
) -> List[Dict[str, Any]]:
    """
    Keyword search using tsvector.

    Returns ts_rank score.
    """
    result = session.execute(
        text("""
            SELECT case_id, text,
                   ts_rank(to_tsvector('english', text), plainto_tsquery('english', :query)) as rank
            FROM embedding_case
            WHERE to_tsvector('english', text) @@ plainto_tsquery('english', :query)
            ORDER BY rank DESC
            LIMIT :limit
        """),
        {"query": query_text, "limit": limit}
    )

    results = []
    for row in result:
        results.append({
            "case_id": row[0],
            "text": row[1],
            "ts_rank": row[2],
        })
    return results


def _graph_similarity_search(
    case_context: Optional[Dict[str, Any]],
    limit: int,
    session: Session
) -> List[Dict[str, Any]]:
    """
    Graph similarity using Jaccard index on shared edge types.

    FIXED: Now properly computes similarity between cases based on:
    1. Shared edge types (AIRPORT_HAS_FAA_DISRUPTION, AIRPORT_WEATHER_RISK, etc.)
    2. Similar node connections

    If case_context contains a case_id, compares other cases to it.
    If it contains node_ids, finds cases connected to those nodes.
    """
    if not case_context:
        return []

    # Case 1: Compare to a specific case
    context_case_id = case_context.get("case_id")
    if context_case_id:
        # Get edge types for the context case
        result = session.execute(
            text("""
                WITH context_edges AS (
                    SELECT DISTINCT type FROM edge e
                    JOIN trace_event t ON t.ref_id::text = e.id::text
                    WHERE t.case_id = :context_case_id
                      AND t.ref_type = 'edge'
                ),
                other_case_edges AS (
                    SELECT t.case_id, array_agg(DISTINCT e.type) as edge_types
                    FROM trace_event t
                    JOIN edge e ON t.ref_id::text = e.id::text
                    WHERE t.ref_type = 'edge'
                      AND t.case_id != :context_case_id
                    GROUP BY t.case_id
                )
                SELECT o.case_id,
                       -- Jaccard: |A ∩ B| / |A ∪ B|
                       (SELECT count(*) FROM unnest(o.edge_types) et
                        WHERE et IN (SELECT type FROM context_edges))::float /
                       GREATEST(
                           (SELECT count(*) FROM context_edges) +
                           array_length(o.edge_types, 1) -
                           (SELECT count(*) FROM unnest(o.edge_types) et
                            WHERE et IN (SELECT type FROM context_edges)),
                           1
                       ) as jaccard_sim
                FROM other_case_edges o
                ORDER BY jaccard_sim DESC
                LIMIT :limit
            """),
            {"context_case_id": context_case_id, "limit": limit}
        )
        results = []
        for row in result:
            results.append({
                "case_id": row[0],
                "jaccard_sim": row[1] if row[1] is not None else 0.0,
            })
        return results

    # Case 2: Find cases connected to specific nodes
    context_node_ids = case_context.get("node_ids", [])
    if not context_node_ids:
        return []

    # Find cases that share edges to/from the context nodes
    result = session.execute(
        text("""
            SELECT DISTINCT t.case_id,
                   -- Jaccard based on how many context nodes are connected
                   count(DISTINCT e.src)::float / GREATEST(:context_count, 1) as jaccard_sim
            FROM trace_event t
            JOIN edge e ON t.ref_id::text = e.id::text
            WHERE t.ref_type = 'edge'
              AND (e.src = ANY(:context_ids) OR e.dst = ANY(:context_ids))
            GROUP BY t.case_id
            ORDER BY jaccard_sim DESC
            LIMIT :limit
        """),
        {"context_ids": context_node_ids, "context_count": len(context_node_ids), "limit": limit}
    )

    results = []
    for row in result:
        results.append({
            "case_id": row[0],
            "jaccard_sim": row[1] if row[1] is not None else 0.0,
        })
    return results


def _normalize_semantic(results: List[Dict[str, Any]]):
    """Normalize cosine similarity from [-1, 1] to [0, 1]."""
    for r in results:
        # cosine_sim in [-1, 1] -> [0, 1]
        r["semantic"] = (r.get("cosine_sim", 0.0) + 1) / 2


def _normalize_keyword(results: List[Dict[str, Any]]):
    """Normalize ts_rank to [0, 1] using min-max."""
    if not results:
        return

    max_rank = max(r.get("ts_rank", 0) for r in results) or 1
    for r in results:
        r["keyword"] = r.get("ts_rank", 0) / max_rank


def _merge_results(
    semantic: List[Dict[str, Any]],
    keyword: List[Dict[str, Any]],
    graph: List[Dict[str, Any]],
) -> Dict[UUID, Dict[str, Any]]:
    """Merge results by case_id."""
    merged: Dict[UUID, Dict[str, Any]] = {}

    for r in semantic:
        case_id = r["case_id"]
        if case_id not in merged:
            merged[case_id] = {"id": case_id, "text": r.get("text", "")}
        merged[case_id]["semantic"] = r.get("semantic", 0.0)

    for r in keyword:
        case_id = r["case_id"]
        if case_id not in merged:
            merged[case_id] = {"id": case_id, "text": r.get("text", "")}
        merged[case_id]["keyword"] = r.get("keyword", 0.0)

    for r in graph:
        case_id = r["case_id"]
        if case_id not in merged:
            merged[case_id] = {"id": case_id}
        merged[case_id]["graph"] = r.get("jaccard_sim", 0.0)

    return merged
