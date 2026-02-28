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
from ..replay.aging import compute_decay_factor, compute_policy_alignment, compute_aged_score

router = APIRouter(prefix="/playbooks", tags=["playbooks"])


def _parse_uuid(value: str, label: str = "ID") -> UUID:
    """Parse a UUID string safely, raising 400 on invalid input."""
    try:
        return UUID(value)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail=f"Invalid {label}: {value}")


class PlaybookResponse(BaseModel):
    """Playbook details response."""
    playbook_id: str
    name: str
    pattern: Dict[str, Any]
    action_template: Dict[str, Any]
    stats: Dict[str, Any]
    created_at: Optional[str] = None
    last_used_at: Optional[str] = None
    domain: str = "operational"
    decay_factor: Optional[float] = None
    policy_alignment: Optional[float] = None
    is_stale: bool = False


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
        Similar playbooks ranked by aged relevance
    """
    scope: Dict[str, Any] = {}

    if case_id:
        _parse_uuid(case_id, "case_id")
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
                "aged_score": p.get("aged_score", 0),
                "decay_factor": p.get("decay_factor", 1.0),
                "policy_alignment": p.get("policy_alignment", 1.0),
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
    Get playbook details with aging metadata.

    Args:
        playbook_id: Playbook ID
        session: Database session

    Returns:
        Playbook details including decay and staleness info
    """
    manager = PlaybookManager(session)
    playbook = manager.get_playbook(_parse_uuid(playbook_id, "playbook_id"))

    if not playbook:
        raise HTTPException(status_code=404, detail="Playbook not found")

    created_at = playbook.get("created_at")
    last_used_at = playbook.get("last_used_at")
    domain = playbook.get("domain", "operational")
    pb_snapshot = playbook.get("policy_snapshot", [])

    decay_factor = compute_decay_factor(created_at, last_used_at, domain) if created_at else 1.0
    current_snapshot = manager.get_current_policy_snapshot()
    policy_align = compute_policy_alignment(pb_snapshot, current_snapshot)

    return PlaybookResponse(
        playbook_id=str(playbook["playbook_id"]),
        name=playbook["name"],
        pattern=playbook["pattern"],
        action_template=playbook["action_template"],
        stats=playbook.get("stats", {}),
        created_at=created_at.isoformat() if created_at else None,
        last_used_at=last_used_at.isoformat() if last_used_at else None,
        domain=domain,
        decay_factor=decay_factor,
        policy_alignment=policy_align,
        is_stale=decay_factor < 0.25,
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
    playbook_id = manager.create_from_case(_parse_uuid(request.case_id, "case_id"), request.name)

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
        Match evaluation results including aging metadata
    """
    evaluator = PlaybookEvaluator(session)

    result = evaluator.evaluate_match(
        _parse_uuid(request.playbook_id, "playbook_id"),
        request.case_context,
    )

    return {
        "playbook_id": request.playbook_id,
        "match": result.get("match", False),
        "overall_score": result.get("overall_score", 0),
        "scope_match": result.get("scope_match", {}),
        "evidence_match": result.get("evidence_match", {}),
        "recommended_actions": result.get("recommended_actions", []),
        "decay_factor": result.get("decay_factor"),
        "policy_alignment": result.get("policy_alignment"),
        "is_stale": result.get("is_stale", False),
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
    List all playbooks with aging metadata.

    Args:
        case_type: Filter by case type
        limit: Maximum results
        session: Database session

    Returns:
        List of playbooks
    """
    # Fetch ALL matching playbooks without SQL pre-limit so that older playbooks
    # with high success rates aren't dropped before Python scores by aged relevance.
    query = """SELECT id, name, pattern, stats, created_at, last_used_at,
                      domain, policy_snapshot
               FROM playbook WHERE 1=1"""
    params: Dict[str, Any] = {}

    if case_type:
        query += " AND pattern->>'case_type' = :case_type"
        params["case_type"] = case_type

    result = session.execute(text(query), params)

    manager = PlaybookManager(session)
    current_snapshot = manager.get_current_policy_snapshot()

    playbooks = []
    for r in result:
        created_at = r[4]
        last_used_at = r[5]
        domain = r[6] or "operational"
        pb_snapshot = r[7] or []
        stats = r[3] or {}

        decay = compute_decay_factor(created_at, last_used_at, domain) if created_at else 1.0
        alignment = compute_policy_alignment(pb_snapshot, current_snapshot)
        use_count = int(stats.get("use_count", 0))
        success_rate = float(stats.get("success_rate", 0))
        aged = compute_aged_score(success_rate, decay, alignment, use_count)

        playbooks.append({
            "playbook_id": str(r[0]),
            "name": r[1],
            "case_type": r[2].get("case_type") if r[2] else None,
            "stats": stats,
            "created_at": created_at.isoformat(),
            "last_used_at": last_used_at.isoformat() if last_used_at else None,
            "domain": domain,
            "decay_factor": round(decay, 4),
            "policy_alignment": round(alignment, 4),
            "aged_score": round(aged, 4),
            "is_stale": decay < 0.25,
        })

    # Rank by aged relevance, then apply the requested limit
    playbooks.sort(key=lambda p: p["aged_score"], reverse=True)
    playbooks = playbooks[:limit]

    return {"playbooks": playbooks, "count": len(playbooks)}
