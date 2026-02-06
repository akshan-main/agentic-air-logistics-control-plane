# app/api/routes_playbooks.py
"""
Playbook API routes.

Endpoints for replay learning playbooks.
"""

from typing import Dict, Any, Optional, List
from uuid import UUID

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import get_session
from ..replay.playbooks import PlaybookManager
from ..replay.evaluator import PlaybookEvaluator

router = APIRouter(prefix="/playbooks", tags=["playbooks"])


class PlaybookResponse(BaseModel):
    """Playbook details response."""
    playbook_id: str
    name: str
    pattern: Dict[str, Any]
    action_template: Dict[str, Any]
    stats: Dict[str, Any]
    created_at: Optional[str] = None


class SimilarPlaybooksResponse(BaseModel):
    """Similar playbooks response."""
    playbooks: List[Dict[str, Any]]
    case_context: Dict[str, Any]


class CreatePlaybookRequest(BaseModel):
    """Request to create playbook from case."""
    case_id: str
    name: Optional[str] = None


class EvaluateMatchRequest(BaseModel):
    """Request to evaluate playbook match."""
    playbook_id: str
    case_context: Dict[str, Any]


@router.get("/similar", response_model=SimilarPlaybooksResponse)
async def get_similar_playbooks(
    case_id: Optional[str] = None,
    case_type: Optional[str] = None,
    airport: Optional[str] = None,
    limit: int = Query(default=3, le=10),
    session: Session = Depends(get_session),
) -> SimilarPlaybooksResponse:
    """
    Find similar playbooks for a case.

    Args:
        case_id: Case ID to find similar playbooks for
        case_type: Case type filter
        airport: Airport filter (for scope)
        limit: Maximum results
        session: Database session

    Returns:
        Similar playbooks ranked by match score
    """
    scope: Dict[str, Any] = {}

    if case_id:
        # Get case context
        result = session.execute(
            text('SELECT case_type, scope FROM "case" WHERE id = :id'),
            {"id": case_id}
        )
        row = result.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Case not found")

        case_type = row[0]
        scope = row[1]

    elif airport:
        scope = {"airport": airport.upper()}

    if not case_type:
        raise HTTPException(
            status_code=400,
            detail="Either case_id or case_type must be provided"
        )

    manager = PlaybookManager(session)
    playbooks = manager.find_matching(case_type, scope, limit=limit)

    return SimilarPlaybooksResponse(
        playbooks=[
            {
                "playbook_id": str(p["playbook_id"]),
                "name": p["name"],
                "match_score": p.get("match_score", 0),
                "stats": p.get("stats", {}),
                "action_count": len(p.get("action_template", {}).get("action_sequence", [])),
            }
            for p in playbooks
        ],
        case_context={
            "case_type": case_type,
            "scope": scope,
        },
    )


@router.get("/{playbook_id}", response_model=PlaybookResponse)
async def get_playbook(
    playbook_id: str,
    session: Session = Depends(get_session),
) -> PlaybookResponse:
    """
    Get playbook details.

    Args:
        playbook_id: Playbook ID
        session: Database session

    Returns:
        Playbook details
    """
    manager = PlaybookManager(session)
    playbook = manager.get_playbook(UUID(playbook_id))

    if not playbook:
        raise HTTPException(status_code=404, detail="Playbook not found")

    # Get created_at
    result = session.execute(
        text("SELECT created_at FROM playbook WHERE id = :id"),
        {"id": playbook_id}
    )
    row = result.fetchone()
    created_at = row[0].isoformat() if row else None

    return PlaybookResponse(
        playbook_id=str(playbook["playbook_id"]),
        name=playbook["name"],
        pattern=playbook["pattern"],
        action_template=playbook["action_template"],
        stats=playbook.get("stats", {}),
        created_at=created_at,
    )


@router.post("/from-case")
async def create_playbook_from_case(
    request: CreatePlaybookRequest,
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    """
    Create a playbook from a successful case.

    Args:
        request: Creation request
        session: Database session

    Returns:
        Created playbook ID
    """
    # Verify case exists and is resolved
    result = session.execute(
        text('SELECT status FROM "case" WHERE id = :id'),
        {"id": request.case_id}
    )
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Case not found")

    if row[0] != "RESOLVED":
        raise HTTPException(
            status_code=400,
            detail="Can only create playbook from RESOLVED cases"
        )

    manager = PlaybookManager(session)
    playbook_id = manager.create_from_case(UUID(request.case_id), request.name)

    return {
        "playbook_id": str(playbook_id),
        "created_from_case": request.case_id,
    }


@router.post("/evaluate-match")
async def evaluate_playbook_match(
    request: EvaluateMatchRequest,
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    """
    Evaluate how well a playbook matches a case context.

    Args:
        request: Evaluation request
        session: Database session

    Returns:
        Match evaluation results
    """
    evaluator = PlaybookEvaluator(session)

    result = evaluator.evaluate_match(
        UUID(request.playbook_id),
        request.case_context,
    )

    return {
        "playbook_id": request.playbook_id,
        "match": result.get("match", False),
        "overall_score": result.get("overall_score", 0),
        "scope_match": result.get("scope_match", {}),
        "evidence_match": result.get("evidence_match", {}),
        "recommended_actions": result.get("recommended_actions", []),
    }


@router.get("/{playbook_id}/cases")
async def get_playbook_cases(
    playbook_id: str,
    limit: int = 20,
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    """
    Get cases that used this playbook.

    Args:
        playbook_id: Playbook ID
        limit: Maximum results
        session: Database session

    Returns:
        Cases that used the playbook
    """
    result = session.execute(
        text("""
            SELECT c.id, c.case_type, c.scope, c.status, c.created_at
            FROM "case" c
            JOIN playbook_case pc ON c.id = pc.case_id
            WHERE pc.playbook_id = :playbook_id
            ORDER BY c.created_at DESC
            LIMIT :limit
        """),
        {"playbook_id": playbook_id, "limit": limit}
    )

    cases = [
        {
            "case_id": str(r[0]),
            "case_type": r[1],
            "scope": r[2],
            "status": r[3],
            "created_at": r[4].isoformat(),
        }
        for r in result
    ]

    return {"playbook_id": playbook_id, "cases": cases, "count": len(cases)}


@router.get("")
async def list_playbooks(
    case_type: Optional[str] = None,
    limit: int = 20,
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    """
    List all playbooks.

    Args:
        case_type: Filter by case type
        limit: Maximum results
        session: Database session

    Returns:
        List of playbooks
    """
    query = "SELECT id, name, pattern, stats, created_at FROM playbook WHERE 1=1"
    params: Dict[str, Any] = {"limit": limit}

    if case_type:
        query += " AND pattern->>'case_type' = :case_type"
        params["case_type"] = case_type

    query += " ORDER BY created_at DESC LIMIT :limit"

    result = session.execute(text(query), params)

    playbooks = [
        {
            "playbook_id": str(r[0]),
            "name": r[1],
            "case_type": r[2].get("case_type") if r[2] else None,
            "stats": r[3],
            "created_at": r[4].isoformat(),
        }
        for r in result
    ]

    return {"playbooks": playbooks, "count": len(playbooks)}
