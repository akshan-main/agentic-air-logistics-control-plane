# app/agents/memory/working.py
"""
Working memory - holds current context for the agent.

Short-term storage for:
- Current case context
- Active hypotheses
- Pending uncertainties
- Recent tool results
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from uuid import UUID
from datetime import datetime, timezone


@dataclass
class ToolResult:
    """Result from a tool call."""
    tool_name: str
    result: Any
    timestamp: datetime
    success: bool
    error: Optional[str] = None


@dataclass
class WorkingMemory:
    """
    Working memory for current case processing.

    Holds ephemeral context that doesn't persist beyond the case.
    """
    case_id: UUID
    case_scope: Dict[str, Any] = field(default_factory=dict)

    # Active context
    current_state: str = "INIT"
    current_airport: Optional[str] = None

    # Tool results (recent only)
    tool_results: List[ToolResult] = field(default_factory=list)
    max_tool_results: int = 20

    # Scratch space for agent reasoning
    notes: List[str] = field(default_factory=list)

    # Time tracking
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_activity: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def add_tool_result(
        self,
        tool_name: str,
        result: Any,
        success: bool = True,
        error: Optional[str] = None,
    ):
        """Add a tool result to working memory."""
        self.tool_results.append(ToolResult(
            tool_name=tool_name,
            result=result,
            timestamp=datetime.now(timezone.utc),
            success=success,
            error=error,
        ))

        # Keep only recent results
        if len(self.tool_results) > self.max_tool_results:
            self.tool_results = self.tool_results[-self.max_tool_results:]

        self.last_activity = datetime.now(timezone.utc)

    def add_note(self, note: str):
        """Add a note to working memory."""
        self.notes.append(note)
        self.last_activity = datetime.now(timezone.utc)

    def get_recent_results(self, tool_name: Optional[str] = None) -> List[ToolResult]:
        """Get recent tool results, optionally filtered by tool name."""
        if tool_name:
            return [r for r in self.tool_results if r.tool_name == tool_name]
        return self.tool_results

    def get_latest_result(self, tool_name: str) -> Optional[ToolResult]:
        """Get most recent result for a tool."""
        for result in reversed(self.tool_results):
            if result.tool_name == tool_name:
                return result
        return None

    def clear(self):
        """Clear working memory."""
        self.tool_results.clear()
        self.notes.clear()
        self.last_activity = datetime.now(timezone.utc)

    def to_context(self) -> Dict[str, Any]:
        """Convert to context dict for prompts."""
        return {
            "case_id": str(self.case_id),
            "case_scope": self.case_scope,
            "current_state": self.current_state,
            "current_airport": self.current_airport,
            "tool_result_count": len(self.tool_results),
            "elapsed_seconds": (
                datetime.now(timezone.utc) - self.started_at
            ).total_seconds(),
        }
