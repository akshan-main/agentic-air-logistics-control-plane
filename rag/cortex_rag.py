"""
Cortex RAG: grounded Q&A over decision packets using Snowflake Cortex.

Two-tier retrieval:
1. PACKET_SEARCH — packet-level rationale + cascade impact (broad context)
2. DETAIL_SEARCH — granular sub-documents: policies, shipments, contradictions,
   claims, actions (fine-grained answers)

Results from both tiers are merged, deduplicated by case_id, and fed into
a grounded AI_COMPLETE prompt. Citations are deterministic (from search results,
not parsed from LLM output).
"""

import os
import json
import logging
from typing import Dict, Any, List

import snowflake.connector

logger = logging.getLogger(__name__)

# Limits
MAX_QUESTION_LENGTH = 2000
MAX_TOP_K = 20
MIN_TOP_K = 1

# Snowflake connection timeouts
SF_LOGIN_TIMEOUT = 30
SF_NETWORK_TIMEOUT = 60


def _get_snowflake_conn() -> snowflake.connector.SnowflakeConnection:
    """Create a Snowflake connection from env vars with timeout protection."""
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(f"Missing Snowflake env vars: {', '.join(missing)}")

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "AALCP_CORTEX_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "AALCP_DB"),
        login_timeout=SF_LOGIN_TIMEOUT,
        network_timeout=SF_NETWORK_TIMEOUT,
    )


# ---------------------------------------------------------------------------
# Cortex Search helpers (shared by both services)
# ---------------------------------------------------------------------------

def _cortex_search_service(
    conn: snowflake.connector.SnowflakeConnection,
    service_name: str,
    question: str,
    columns: List[str],
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    """
    Query a Cortex Search service by name.

    Uses the snowflake.core Python API (Root -> cortex_search_services -> search).
    Falls back to SEARCH_PREVIEW SQL if the Python API is unavailable.
    """
    fqn = f"AALCP_DB.SERVICES.{service_name}"

    try:
        from snowflake.core import Root

        root = Root(conn)
        search_service = (
            root.databases["AALCP_DB"]
            .schemas["SERVICES"]
            .cortex_search_services[service_name]
        )

        response = search_service.search(
            query=question,
            columns=columns,
            limit=top_k,
        )

        results = response.results if hasattr(response, "results") else []
        return [dict(r) for r in results]

    except ImportError:
        logger.warning("snowflake.core not available, falling back to SEARCH_PREVIEW SQL")
    except Exception as e:
        logger.warning("Cortex Search Python API failed for %s (%s), falling back to SQL", service_name, e)

    # Fallback: SEARCH_PREVIEW SQL
    cur = conn.cursor()
    try:
        search_params = json.dumps({
            "query": question,
            "columns": columns,
            "limit": top_k,
        })
        cur.execute(
            "SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(%s, %s)",
            (fqn, search_params),
        )
        row = cur.fetchone()
        if row and row[0]:
            parsed = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            return parsed.get("results", []) if isinstance(parsed, dict) else []
        return []
    finally:
        cur.close()


def _packet_search(
    conn: snowflake.connector.SnowflakeConnection,
    question: str,
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    """Query PACKET_SEARCH for packet-level snippets."""
    return _cortex_search_service(
        conn, "PACKET_SEARCH", question,
        columns=["case_id", "rationale_text", "cascade_text", "airport", "posture"],
        top_k=top_k,
    )


def _detail_search(
    conn: snowflake.connector.SnowflakeConnection,
    question: str,
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    """Query DETAIL_SEARCH for granular sub-document snippets."""
    return _cortex_search_service(
        conn, "DETAIL_SEARCH", question,
        columns=["detail_id", "case_id", "detail_text", "detail_type", "airport", "posture"],
        top_k=top_k,
    )


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------

def _build_prompt(
    question: str,
    packet_snippets: List[Dict[str, Any]],
    detail_snippets: List[Dict[str, Any]],
) -> str:
    """Build a grounded prompt using ONLY retrieved snippets from both tiers."""
    context_parts = []

    # Packet-level context (broad rationale + cascade)
    for s in packet_snippets:
        case_id = s.get("case_id", "unknown")
        rationale = s.get("rationale_text", "")
        cascade = s.get("cascade_text", "")
        airport = s.get("airport", "")
        posture = s.get("posture", "")

        entry = f"[Case: {case_id}] Airport: {airport}, Posture: {posture}\n"
        entry += f"Rationale: {rationale}\n"
        if cascade:
            entry += f"Operational Impact: {cascade}\n"
        context_parts.append(entry)

    # Detail-level context (granular sub-documents)
    if detail_snippets:
        detail_parts = []
        for d in detail_snippets:
            case_id = d.get("case_id", "unknown")
            detail_type = d.get("detail_type", "DETAIL")
            detail_text = d.get("detail_text", "")
            detail_parts.append(f"[Case: {case_id}] [{detail_type}] {detail_text}")
        context_parts.append("GRANULAR DETAILS:\n" + "\n".join(detail_parts))

    context = "\n---\n".join(context_parts)

    return f"""You are an aviation operations auditor reviewing decision packets from the
Agentic Air Logistics Control Plane.

RULES:
1. Answer ONLY using the decision packet excerpts below. Do NOT use prior knowledge.
2. For every factual claim, cite the source case using [Case: <case_id>].
3. If the excerpts do not contain enough information to answer the question,
   respond with exactly: "Insufficient evidence in retrieved packets."
4. Do NOT speculate, infer, or add information beyond what the excerpts state.
5. When answering granular questions about specific policies, shipments,
   contradictions, or actions, prefer the GRANULAR DETAILS section for precision.

--- DECISION PACKET EXCERPTS ---
{context}
--- END EXCERPTS ---

Question: {question}

Answer:"""


def _ai_complete(
    conn: snowflake.connector.SnowflakeConnection,
    prompt: str,
    model: str = "mistral-large2",
) -> str:
    """Call Snowflake AI_COMPLETE for grounded generation."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT AI_COMPLETE(%s, %s)", (model, prompt))
        row = cur.fetchone()
        if row and row[0]:
            # AI_COMPLETE may return a JSON string or plain text
            result = row[0]
            if isinstance(result, str):
                try:
                    parsed = json.loads(result)
                    return parsed.get("choices", [{}])[0].get("message", {}).get("content", result)
                except (json.JSONDecodeError, IndexError, KeyError):
                    return result
            return str(result)
        return "No response from AI_COMPLETE."
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def query_audit(
    question: str,
    top_k: int = 5,
    model: str = "mistral-large2",
) -> Dict[str, Any]:
    """
    End-to-end RAG query over the audit warehouse (two-tier retrieval).

    Searches both packet-level (rationale, cascade) and detail-level
    (policies, shipments, contradictions, claims, actions) indexes.

    Args:
        question: Natural language question about decision history.
        top_k: Number of snippets to retrieve per tier (1-20).
        model: Snowflake AI_COMPLETE model name.

    Returns:
        {
            "answer": str,
            "citations": [case_id, ...],        # deterministic from search results
            "snippets": [{case_id, text}, ...],  # readable excerpts (packet-level)
            "details": [{case_id, detail_type, text}, ...],  # granular details
        }

    Raises:
        ValueError: If question is empty or exceeds length limit.
        EnvironmentError: If Snowflake env vars are missing.
    """
    # Input validation
    if not question or not question.strip():
        raise ValueError("Question cannot be empty")

    question = question.strip()
    if len(question) > MAX_QUESTION_LENGTH:
        raise ValueError(
            f"Question exceeds {MAX_QUESTION_LENGTH} characters ({len(question)} given)"
        )

    top_k = max(MIN_TOP_K, min(top_k, MAX_TOP_K))

    conn = _get_snowflake_conn()
    try:
        # 1. Retrieve from both tiers
        packet_results = _packet_search(conn, question, top_k)
        detail_results = _detail_search(conn, question, top_k)

        if not packet_results and not detail_results:
            return {
                "answer": "No relevant decision packets found for this query.",
                "citations": [],
                "snippets": [],
                "details": [],
            }

        # 2. Build grounded prompt with both tiers
        prompt = _build_prompt(question, packet_results, detail_results)

        # 3. Generate answer via AI_COMPLETE
        answer = _ai_complete(conn, prompt, model)

        # 4. Citations are deterministic: deduplicated case_ids from both tiers
        seen_case_ids = set()
        citations = []
        for s in packet_results + detail_results:
            cid = s.get("case_id")
            if cid and cid not in seen_case_ids:
                citations.append(cid)
                seen_case_ids.add(cid)

        # 5. Build readable snippet list (packet-level)
        snippets = [
            {
                "case_id": s.get("case_id"),
                "text": s.get("rationale_text", "")[:500],
                "airport": s.get("airport"),
                "posture": s.get("posture"),
            }
            for s in packet_results
        ]

        # 6. Build detail list (granular)
        details = [
            {
                "case_id": d.get("case_id"),
                "detail_type": d.get("detail_type"),
                "text": d.get("detail_text", "")[:500],
                "airport": d.get("airport"),
                "posture": d.get("posture"),
            }
            for d in detail_results
        ]

        return {
            "answer": answer,
            "citations": citations,
            "snippets": snippets,
            "details": details,
        }

    finally:
        conn.close()
