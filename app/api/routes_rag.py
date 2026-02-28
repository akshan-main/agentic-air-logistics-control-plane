"""
RAG query endpoint: Cortex Search retrieval + AI_COMPLETE grounded generation.

Queries the Snowflake audit warehouse for decision packet history
and returns grounded answers with case_id citations.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/rag", tags=["rag"])


class RAGRequest(BaseModel):
    """RAG query request."""
    question: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        description="Natural language question about decision history",
    )
    top_k: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Number of packet snippets to retrieve (1-20)",
    )


class SnippetInfo(BaseModel):
    """A retrieved packet snippet."""
    case_id: Optional[str] = None
    text: str
    airport: Optional[str] = None
    posture: Optional[str] = None


class DetailInfo(BaseModel):
    """A granular detail snippet (policy, shipment, contradiction, claim, action)."""
    case_id: Optional[str] = None
    detail_type: Optional[str] = None
    text: str
    airport: Optional[str] = None
    posture: Optional[str] = None


class RAGResponse(BaseModel):
    """RAG query response with grounded answer, citations, and granular details."""
    answer: str
    citations: List[str]
    snippets: List[SnippetInfo]
    details: List[DetailInfo] = []


@router.post("/query")
async def rag_query(request: RAGRequest) -> RAGResponse:
    """
    Query the audit warehouse using Cortex Search + AI_COMPLETE.

    Retrieves relevant decision packet snippets, builds a grounded prompt,
    and returns an answer with deterministic citations (case_ids from
    search results, not parsed from LLM output).

    Requires Snowflake env vars: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE.
    """
    try:
        from rag.cortex_rag import query_audit
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="RAG module not available. Install snowflake-connector-python.",
        )

    import os
    if not os.environ.get("SNOWFLAKE_ACCOUNT"):
        raise HTTPException(
            status_code=503,
            detail="Snowflake not configured. Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD env vars.",
        )

    try:
        result = query_audit(request.question, top_k=request.top_k)
        return RAGResponse(
            answer=result["answer"],
            citations=result["citations"],
            snippets=[SnippetInfo(**s) for s in result["snippets"]],
            details=[DetailInfo(**d) for d in result.get("details", [])],
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except EnvironmentError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception:
        logger.exception("RAG query failed")
        raise HTTPException(status_code=500, detail="Internal server error")
