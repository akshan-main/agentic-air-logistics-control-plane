# app/api/routes_decisions.py
"""
Decision packet API routes.

Endpoints for retrieving decision packets.
"""

import logging
from typing import Dict, Any, Optional, List
from uuid import UUID

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from ..db.engine import get_session
from ..packets.builder import build_decision_packet

router = APIRouter(prefix="/packets", tags=["decisions"])


class PacketResponse(BaseModel):
    """Decision packet response."""
    case_id: str
    scope: Dict[str, Any]
    timestamps: Dict[str, str]
    posture: Optional[str] = None
    posture_confidence: Optional[float] = None
    claims: List[Dict[str, Any]]
    evidence: List[Dict[str, Any]]
    contradictions: List[Dict[str, Any]]
    policies_applied: List[Dict[str, Any]]
    actions_proposed: List[Dict[str, Any]]
    actions_executed: List[Dict[str, Any]]
    blocked_section: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, Any]] = None


@router.get("/{case_id}")
async def get_packet(
    case_id: str,
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    """
    Get the decision packet for a case.

    Args:
        case_id: Case ID

    Returns:
        Complete decision packet
    """
    try:
        case_uuid = UUID(case_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid case ID format")

    packet = build_decision_packet(case_uuid, session=session)

    if not packet:
        raise HTTPException(status_code=404, detail="Packet not found")

    # Transform to match UI expectations
    posture_decision = packet.get("posture_decision", {})
    metrics = packet.get("metrics", {})

    return {
        "case_id": packet.get("case_id"),
        "scope": packet.get("scope", {}),
        "posture": posture_decision.get("posture") if posture_decision else None,
        "posture_decision": posture_decision,
        "timestamps": {
            "created_at": packet.get("created_at"),
            "first_signal_at": metrics.get("first_signal_at") if metrics else None,
            "posture_emitted_at": metrics.get("posture_emitted_at") if metrics else None,
        },
        "claims": [
            {
                "claim_id": c.get("claim_id"),
                "text": c.get("text"),
                "status": c.get("status"),
                "confidence": c.get("confidence"),
            }
            for c in packet.get("top_claims", [])
        ],
        "evidence": [
            {
                "evidence_id": e.get("evidence_id"),
                "source_system": e.get("source_system"),
                "retrieved_at": e.get("retrieved_at"),
                "excerpt": e.get("excerpt"),
            }
            for e in packet.get("evidence_list", [])
        ],
        "contradictions": packet.get("contradictions", []),
        "policies_applied": packet.get("policies_applied", []),
        "actions_proposed": [
            {
                "action_id": a.get("action_id"),
                "type": a.get("action_type"),
                "args": a.get("args"),
                "state": a.get("state"),
            }
            for a in packet.get("actions_proposed", [])
        ],
        "actions_executed": packet.get("actions_executed", []),
        "blocked_section": {
            "is_blocked": packet.get("blocked_section") is not None,
            "reason": packet.get("blocked_section", {}).get("reason") if packet.get("blocked_section") else None,
            "missing_evidence_requests": packet.get("blocked_section", {}).get("missing_evidence_requests", []) if packet.get("blocked_section") else [],
        } if packet.get("blocked_section") else None,
        "metrics": metrics,
        "workflow_trace": packet.get("workflow_trace", []),
        "confidence_breakdown": packet.get("confidence_breakdown"),
        "cascade_impact": packet.get("cascade_impact"),
    }


@router.get("/{case_id}/summary")
async def get_packet_summary(
    case_id: str,
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    """
    Get a summary of the decision packet.

    Args:
        case_id: Case ID

    Returns:
        Packet summary (lighter weight than full packet)
    """
    from sqlalchemy import text

    try:
        case_uuid = UUID(case_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid case ID format")

    try:
        # Get case info
        case_result = session.execute(
            text("""
                SELECT case_type, scope, status, created_at
                FROM "case" WHERE id = :case_id
            """),
            {"case_id": case_uuid}
        )
        case_row = case_result.fetchone()
        if not case_row:
            raise HTTPException(status_code=404, detail="Case not found")

        # Get evidence count
        evidence_result = session.execute(
            text("""
                SELECT COUNT(DISTINCT e.id)
                FROM evidence e
                JOIN trace_event t ON t.ref_id::text = e.id::text
                WHERE t.case_id = :case_id AND t.ref_type = 'evidence'
            """),
            {"case_id": case_uuid}
        )
        evidence_count = evidence_result.scalar() or 0

        # Get posture from latest action
        action_result = session.execute(
            text("""
                SELECT type, args, state
                FROM action
                WHERE case_id = :case_id AND type = 'SET_POSTURE'
                ORDER BY created_at DESC LIMIT 1
            """),
            {"case_id": case_uuid}
        )
        action_row = action_result.fetchone()
        posture = None
        if action_row and action_row[1]:
            posture = action_row[1].get("posture")

        return {
            "case_id": str(case_uuid),
            "case_type": case_row[0],
            "scope": case_row[1],
            "status": case_row[2],
            "created_at": case_row[3].isoformat() if case_row[3] else None,
            "evidence_count": evidence_count,
            "posture": posture,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Packet query failed")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("")
async def list_packets(
    status: Optional[str] = None,
    posture: Optional[str] = None,
    limit: int = 20,
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    """
    List decision packets with filters.

    Args:
        status: Filter by case status (OPEN, BLOCKED, RESOLVED)
        posture: Filter by posture (ACCEPT, RESTRICT, HOLD, ESCALATE)
        limit: Maximum results

    Returns:
        List of packet summaries
    """
    from sqlalchemy import text

    try:
        # Build query with filters
        query = """
            SELECT c.id, c.case_type, c.scope, c.status, c.created_at,
                   (SELECT args->>'posture' FROM action
                    WHERE case_id = c.id AND type = 'SET_POSTURE'
                    ORDER BY created_at DESC LIMIT 1) as posture,
                   (SELECT COUNT(*) FROM trace_event t
                    JOIN evidence e ON t.ref_id::text = e.id::text
                    WHERE t.case_id = c.id AND t.ref_type = 'evidence') as evidence_count
            FROM "case" c
            WHERE 1=1
        """
        params = {"limit": limit}

        if status:
            query += " AND c.status = :status"
            params["status"] = status.upper()

        # Note: posture filter requires subquery since posture is in action table
        if posture:
            query += """ AND EXISTS (
                SELECT 1 FROM action
                WHERE case_id = c.id AND type = 'SET_POSTURE'
                  AND args->>'posture' = :posture
            )"""
            params["posture"] = posture.upper()

        query += " ORDER BY c.created_at DESC LIMIT :limit"

        result = session.execute(text(query), params)

        packets = []
        for row in result:
            packets.append({
                "case_id": str(row[0]),
                "case_type": row[1],
                "scope": row[2],
                "status": row[3],
                "created_at": row[4].isoformat() if row[4] else None,
                "posture": row[5],
                "evidence_count": row[6] or 0,
            })

        return {
            "packets": packets,
            "count": len(packets),
            "filters_applied": {
                "status": status,
                "posture": posture,
            },
        }

    except Exception as e:
        logger.exception("Packet query failed")
        raise HTTPException(status_code=500, detail="Internal server error")
