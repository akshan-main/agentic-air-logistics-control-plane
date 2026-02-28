# app/api/routes_cases.py
"""
Case management API routes.

Endpoints for creating and managing exception cases.
"""

import json
from typing import Dict, Any, Optional, List
from uuid import UUID
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db.engine import get_session, SessionLocal
from ..agents.orchestrator import Orchestrator
from ..governance.approvals import ApprovalManager
from ..replay.evaluator import PlaybookEvaluator

# UUID import for streaming endpoint
from uuid import UUID as PyUUID

router = APIRouter(prefix="/cases", tags=["cases"])


class CreateCaseRequest(BaseModel):
    """Request to create a new case."""
    case_type: str  # AIRPORT_DISRUPTION or LANE_DISRUPTION
    scope: Dict[str, Any]  # e.g., {"airport": "KJFK"} or {"origin": "KJFK", "destination": "EGLL"}


class CreateCaseResponse(BaseModel):
    """Response from case creation."""
    case_id: str
    case_type: str
    scope: Dict[str, Any]
    status: str
    created_at: datetime
    playbook_suggested: Optional[str] = None


class RunCaseRequest(BaseModel):
    """Request to run case resolution."""
    use_playbook: Optional[str] = None  # Optional playbook ID to use


class RunCaseResponse(BaseModel):
    """Response from case run."""
    case_id: str
    final_state: str
    status: str
    actions_proposed: int
    actions_executed: int
    actions_blocked: int
    posture: Optional[str] = None
    pdl_seconds: Optional[float] = None


class CaseStatusResponse(BaseModel):
    """Case status response."""
    case_id: str
    case_type: str
    scope: Dict[str, Any]
    status: str
    created_at: datetime
    current_state: Optional[str] = None
    actions: List[Dict[str, Any]]
    trace_events: int


@router.post("", response_model=CreateCaseResponse)
async def create_case(
    request: CreateCaseRequest,
    session: Session = Depends(get_session),
) -> CreateCaseResponse:
    """
    Create a new exception case.

    Args:
        request: Case creation request
        session: Database session

    Returns:
        Created case details
    """
    # Validate case type
    valid_types = ["AIRPORT_DISRUPTION", "LANE_DISRUPTION"]
    if request.case_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid case_type. Must be one of: {valid_types}"
        )

    # Create case
    result = session.execute(
        text("""
            INSERT INTO "case" (case_type, scope, status, created_at)
            VALUES (:case_type, CAST(:scope AS jsonb), 'OPEN', :created_at)
            RETURNING id, created_at
        """),
        {
            "case_type": request.case_type,
            "scope": json.dumps(request.scope),
            "created_at": datetime.now(timezone.utc),
        }
    )
    row = result.fetchone()
    case_id = row[0]
    created_at = row[1]
    session.commit()

    # Check for playbook suggestion (replay gate)
    playbook_suggested = None
    evaluator = PlaybookEvaluator(session)

    # Count previous cases of same type
    count_result = session.execute(
        text("""
            SELECT COUNT(*) FROM "case"
            WHERE case_type = :case_type AND status = 'RESOLVED'
        """),
        {"case_type": request.case_type}
    )
    case_count = count_result.scalar()

    if evaluator.should_use_playbook(request.case_type, case_count):
        playbooks = evaluator.playbook_manager.find_matching(
            request.case_type,
            request.scope,
            limit=1,
        )
        if playbooks:
            playbook_suggested = str(playbooks[0]["playbook_id"])

    return CreateCaseResponse(
        case_id=str(case_id),
        case_type=request.case_type,
        scope=request.scope,
        status="OPEN",
        created_at=created_at,
        playbook_suggested=playbook_suggested,
    )


@router.post("/{case_id}/run", response_model=RunCaseResponse)
async def run_case(
    case_id: str,
    request: Optional[RunCaseRequest] = None,
    background_tasks: BackgroundTasks = None,
    session: Session = Depends(get_session),
) -> RunCaseResponse:
    """
    Run the agent state machine for a case.

    Args:
        case_id: Case ID
        request: Optional run configuration
        background_tasks: Background task runner
        session: Database session

    Returns:
        Case resolution results
    """
    # Verify case exists
    result = session.execute(
        text('SELECT status FROM "case" WHERE id = :id'),
        {"id": case_id}
    )
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Case not found")

    if row[0] == "RESOLVED":
        raise HTTPException(status_code=400, detail="Case already resolved")

    # Create and run orchestrator
    orchestrator = Orchestrator(case_id, session)

    # Optionally set playbook context
    if request and request.use_playbook:
        orchestrator.set_playbook_context(request.use_playbook)

    # Run the state machine
    packet = orchestrator.run()

    # Get action counts
    actions_result = session.execute(
        text("""
            SELECT state, COUNT(*) FROM action
            WHERE case_id = :case_id
            GROUP BY state
        """),
        {"case_id": case_id}
    )
    action_counts = {row[0]: row[1] for row in actions_result}

    # Extract posture and pdl from packet (which is a dict)
    posture_decision = packet.get("posture_decision") if isinstance(packet, dict) else getattr(packet, "posture_decision", None)
    metrics = packet.get("metrics") if isinstance(packet, dict) else getattr(packet, "metrics", None)

    posture = None
    if posture_decision:
        posture = posture_decision.get("posture") if isinstance(posture_decision, dict) else getattr(posture_decision, "posture", None)

    pdl_seconds = None
    if metrics:
        pdl_seconds = metrics.get("pdl_seconds") if isinstance(metrics, dict) else getattr(metrics, "pdl_seconds", None)

    # Get actual case status from database (not derived from orchestrator state)
    status_result = session.execute(
        text('SELECT status FROM "case" WHERE id = :id'),
        {"id": case_id}
    )
    status_row = status_result.fetchone()
    case_status = status_row[0] if status_row else "BLOCKED"

    return RunCaseResponse(
        case_id=case_id,
        final_state=orchestrator.state.value,
        status=case_status,
        actions_proposed=action_counts.get("PROPOSED", 0),
        actions_executed=action_counts.get("COMPLETED", 0),
        actions_blocked=action_counts.get("FAILED", 0),
        posture=posture,
        pdl_seconds=pdl_seconds,
    )


@router.post("/{case_id}/approve/{action_id}")
async def approve_action(
    case_id: str,
    action_id: str,
    approved_by: str = "api_user",
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    """
    Approve a pending action.

    Args:
        case_id: Case ID
        action_id: Action ID to approve
        approved_by: Approver identifier
        session: Database session

    Returns:
        Approval result
    """
    # Verify action belongs to the specified case
    verify_result = session.execute(
        text("SELECT case_id FROM action WHERE id = :action_id"),
        {"action_id": action_id}
    )
    verify_row = verify_result.fetchone()
    if not verify_row:
        raise HTTPException(status_code=404, detail="Action not found")
    if str(verify_row[0]) != case_id:
        raise HTTPException(
            status_code=400,
            detail=f"Action {action_id} does not belong to case {case_id}"
        )

    manager = ApprovalManager(session)

    try:
        success, error = manager.approve(
            action_id=UUID(action_id),
            approved_by=approved_by,
            auto_execute=True,  # Execute after approval
        )
        if not success:
            raise HTTPException(status_code=400, detail=error or "Approval failed")

        # Get the final state after execution
        result = session.execute(
            text("SELECT state FROM action WHERE id = :id"),
            {"id": action_id}
        )
        row = result.fetchone()
        final_state = row[0] if row else "UNKNOWN"

        return {
            "action_id": action_id,
            "approved": True,
            "approved_by": approved_by,
            "executed": True,
            "final_state": final_state,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{case_id}", response_model=CaseStatusResponse)
async def get_case_status(
    case_id: str,
    session: Session = Depends(get_session),
) -> CaseStatusResponse:
    """
    Get case status and details.

    Args:
        case_id: Case ID
        session: Database session

    Returns:
        Case status and details
    """
    # Get case
    result = session.execute(
        text('SELECT case_type, scope, status, created_at FROM "case" WHERE id = :id'),
        {"id": case_id}
    )
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Case not found")

    # Get actions
    actions_result = session.execute(
        text("""
            SELECT id, type, args, state, risk_level, requires_approval, created_at
            FROM action WHERE case_id = :case_id
            ORDER BY created_at
        """),
        {"case_id": case_id}
    )
    actions = [
        {
            "action_id": str(r[0]),
            "type": r[1],
            "args": r[2],
            "state": r[3],
            "risk_level": r[4],
            "requires_approval": r[5],
            "created_at": r[6].isoformat(),
        }
        for r in actions_result
    ]

    # Get trace event count
    trace_result = session.execute(
        text("SELECT COUNT(*) FROM trace_event WHERE case_id = :case_id"),
        {"case_id": case_id}
    )
    trace_count = trace_result.scalar()

    # Get current state from latest trace
    # State name is stored in meta->>'state', not ref_id
    state_result = session.execute(
        text("""
            SELECT meta->>'state' FROM trace_event
            WHERE case_id = :case_id AND event_type = 'STATE_ENTER'
            ORDER BY seq DESC LIMIT 1
        """),
        {"case_id": case_id}
    )
    state_row = state_result.fetchone()
    current_state = state_row[0] if state_row else None

    return CaseStatusResponse(
        case_id=case_id,
        case_type=row[0],
        scope=row[1],
        status=row[2],
        created_at=row[3],
        current_state=current_state,
        actions=actions,
        trace_events=trace_count,
    )


@router.get("/{case_id}/run/stream")
async def run_case_stream(
    case_id: str,
    use_playbook: Optional[str] = None,
) -> StreamingResponse:
    """
    Run the agent state machine with streaming progress updates.

    Returns Server-Sent Events (SSE) for real-time progress visibility.

    Args:
        case_id: Case ID
        use_playbook: Optional playbook ID to use

    Returns:
        SSE stream with progress events
    """
    async def event_generator():
        session = SessionLocal()
        try:
            # Verify case exists
            result = session.execute(
                text('SELECT status FROM "case" WHERE id = :id'),
                {"id": case_id}
            )
            row = result.fetchone()
            if not row:
                yield f"data: {json.dumps({'error': 'Case not found'})}\n\n"
                return

            if row[0] == "RESOLVED":
                yield f"data: {json.dumps({'error': 'Case already resolved'})}\n\n"
                return

            # Send initial event
            yield f"data: {json.dumps({'event': 'started', 'case_id': case_id, 'state': 'INIT'})}\n\n"

            # Create orchestrator (convert string case_id to UUID)
            orchestrator = Orchestrator(PyUUID(case_id), session)

            if use_playbook:
                orchestrator.set_playbook_context(use_playbook)

            # Run with progress callback
            for progress in orchestrator.run_with_progress():
                yield f"data: {json.dumps(progress)}\n\n"

            # Get final action counts
            actions_result = session.execute(
                text("""
                    SELECT state, COUNT(*) FROM action
                    WHERE case_id = :case_id
                    GROUP BY state
                """),
                {"case_id": case_id}
            )
            action_counts = {row[0]: row[1] for row in actions_result}

            # Get actual case status from database
            status_result = session.execute(
                text('SELECT status FROM "case" WHERE id = :id'),
                {"id": case_id}
            )
            status_row = status_result.fetchone()
            case_status = status_row[0] if status_row else "BLOCKED"

            # Send completion event
            completion = {
                "event": "completed",
                "case_id": case_id,
                "final_state": orchestrator.state.value,
                "status": case_status,
                "actions_proposed": action_counts.get("PROPOSED", 0),
                "actions_executed": action_counts.get("COMPLETED", 0),
                "actions_blocked": action_counts.get("FAILED", 0),
            }

            # Add posture and metrics if available
            if hasattr(orchestrator, 'last_packet') and orchestrator.last_packet:
                packet = orchestrator.last_packet
                if isinstance(packet, dict):
                    posture_decision = packet.get("posture_decision")
                    metrics = packet.get("metrics")
                    if posture_decision:
                        completion["posture"] = posture_decision.get("posture") if isinstance(posture_decision, dict) else getattr(posture_decision, "posture", None)
                    if metrics:
                        completion["pdl_seconds"] = metrics.get("pdl_seconds") if isinstance(metrics, dict) else getattr(metrics, "pdl_seconds", None)

            yield f"data: {json.dumps(completion)}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        finally:
            session.close()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


@router.get("")
async def list_cases(
    status: Optional[str] = None,
    case_type: Optional[str] = None,
    limit: int = 20,
    session: Session = Depends(get_session),
) -> Dict[str, Any]:
    """
    List cases with optional filters.

    Args:
        status: Filter by status
        case_type: Filter by case type
        limit: Maximum results
        session: Database session

    Returns:
        List of cases
    """
    query = 'SELECT id, case_type, scope, status, created_at FROM "case" WHERE 1=1'
    params: Dict[str, Any] = {"limit": limit}

    if status:
        query += " AND status = :status"
        params["status"] = status

    if case_type:
        query += " AND case_type = :case_type"
        params["case_type"] = case_type

    query += " ORDER BY created_at DESC LIMIT :limit"

    result = session.execute(text(query), params)

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

    return {"cases": cases, "count": len(cases)}
