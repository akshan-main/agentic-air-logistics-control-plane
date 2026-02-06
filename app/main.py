# app/main.py
"""
Agentic Air Logistics Control Plane - Main Application

Air freight Gateway Posture Directive system that continuously ingests
real disruption signals and outputs governed operational decisions.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse

from .settings import settings
from .db.engine import check_connection, check_pgvector_version
from .api import (
    ingest_router,
    cases_router,
    graph_router,
    decisions_router,
    playbooks_router,
    webhooks_router,
    sandbox_router,
)

# Import simulation router
from simulation.api import router as simulation_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan handler.

    Runs startup checks and cleanup on shutdown.
    """
    # Startup
    print("Starting Agentic Air Logistics Control Plane...")

    # Check database connection
    if not check_connection():
        print("WARNING: Database connection failed!")
    else:
        print("Database connection OK")
        pgvector_version = check_pgvector_version()
        print(f"pgvector version: {pgvector_version}")

    yield

    # Shutdown
    print("Shutting down Agentic Air Logistics Control Plane...")


# Create FastAPI app
app = FastAPI(
    title="Agentic Air Logistics Control Plane",
    description="""
    Air freight Gateway Posture Directive system.

    Continuously ingests real disruption signals (FAA NAS, METAR/TAF, NWS Alerts, OpenSky)
    and outputs governed operational decisions (ACCEPT, RESTRICT, HOLD, ESCALATE) per airport.

    Key features:
    - Bi-temporal context graph with evidence binding
    - Deterministic state machine orchestration (not a ReAct loop)
    - Multi-agent roles: Investigator, RiskQuant, PolicyJudge, Critic, Comms, Executor
    - Governance with approval workflows
    - Decision Packets for audit trails
    - Replay learning with playbooks
    - Posture Decision Latency (PDL) tracking
    """,
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware - FIXED: More restrictive by default
# In production, set ALLOWED_ORIGINS env variable to specific domains
import os
allowed_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:8000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Request-ID"],
)


# Security headers middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add basic security headers to all responses."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        # Prevent XSS
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        # Don't expose server version
        response.headers["Server"] = "Agentic Air Logistics Control Plane"
        return response

app.add_middleware(SecurityHeadersMiddleware)

# Include API routers
app.include_router(ingest_router)
app.include_router(cases_router)
app.include_router(graph_router)
app.include_router(decisions_router)
app.include_router(playbooks_router)
app.include_router(webhooks_router)
app.include_router(sandbox_router)
app.include_router(simulation_router)


# Health check endpoints
@app.get("/health")
async def health_check():
    """Basic health check."""
    return {"status": "ok", "service": "agentic-air-logistics-control-plane"}


@app.get("/health/db")
async def db_health_check():
    """Database health check."""
    if check_connection():
        return {
            "status": "ok",
            "database": "connected",
            "pgvector": check_pgvector_version(),
        }
    else:
        raise HTTPException(status_code=503, detail="Database connection failed")


@app.get("/")
async def root():
    """Redirect to UI."""
    return RedirectResponse(url="/ui/")


# Mount static files for UI
import os
ui_path = os.path.join(os.path.dirname(__file__), "ui", "static")
if os.path.exists(ui_path):
    print(f"Mounting UI from: {ui_path}")
    app.mount("/ui", StaticFiles(directory=ui_path, html=True), name="ui")
else:
    print(f"WARNING: UI path not found: {ui_path}")


def run():
    """Run the server (entry point for CLI)."""
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,
    )


if __name__ == "__main__":
    run()
