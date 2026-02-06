# app/agents/roles/critic.py
"""
Critic Agent - LLM-powered evidence quality assessment.

Uses LLM to reason about:
- Evidence completeness and consistency
- Source reliability
- Logical gaps in reasoning

Hard guardrails remain as safety checks.
"""

import json
from typing import Dict, Any, Optional, List
from uuid import UUID, uuid4
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..state_graph import BeliefState
from ...llm import get_llm_client
from ...db.engine import get_next_trace_seq


# Critic verdict types
VERDICT_ACCEPTABLE = "ACCEPTABLE"
VERDICT_INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"

# Hard guardrails (cannot be overridden by LLM)
MIN_REQUIRED_SOURCES = 3
MAX_REJECTIONS = 2


CRITIC_SYSTEM_PROMPT = """You are an ADVERSARIAL evidence critic for air freight gateway operations.

Your role is to CHALLENGE the reasoning and find weaknesses - like a skeptical auditor.
You protect against premature decisions that could cost millions in cargo delays or missed SLAs.

## YOUR ADVERSARIAL MINDSET

Ask yourself:
- "What if the opposite is true?" - If FAA says normal, could weather still be dangerous?
- "What's missing?" - Ground truth we don't have (actual flight delays, pilot reports)
- "Are we being fooled?" - Could stale data be hiding a developing situation?
- "What would make me change my mind?" - What evidence would flip this decision?

## SUPPLY CHAIN EXPERTISE (YOUR SPECIALIZED KNOWLEDGE)

Unlike generic fact-checkers, YOU understand:
1. **Cascade risk**: A 2-hour delay at a hub (JFK, ORD, LAX) cascades to 100+ connecting flights
2. **SLA cliffs**: Premium cargo has 4-hour SLAs - being 80% confident isn't good enough
3. **False calm**: FAA "normal" doesn't mean weather is clear for ground operations
4. **Seasonal patterns**: Winter storms develop fast, summer convection unpredictable
5. **Time-of-day effects**: Evening builds, morning fog, rush hour congestion

## EVIDENCE HIERARCHY (for airport disruption)

MUST HAVE (reject if missing):
- FAA_NAS: Official airport status
- METAR: Current actual conditions

SHOULD HAVE (warn if missing):
- TAF: Forecast for next 6 hours
- NWS_ALERTS: Severe weather warnings

NICE TO HAVE (note but don't reject for):
- OPENSKY: Movement validation

## CONTRADICTION DETECTION

You MUST flag these contradictions:
- FAA says "normal" but METAR shows IFR/LIFR conditions
- FAA says "delay" but OPENSKY shows normal traffic
- NWS has severe alert but no FAA action yet (developing situation!)
- METAR from 30+ minutes ago during rapidly changing weather

## VERDICT DECISION

INSUFFICIENT_EVIDENCE when:
- Missing FAA or METAR entirely
- Evidence is stale (>30 min) during active weather
- Clear contradiction between sources
- High-impact decision (HOLD/ESCALATE) with <3 sources

ACCEPTABLE when:
- Core sources present and consistent
- Minor gaps don't affect the decision direction
- We've already investigated twice (prevent loops)

BE SKEPTICAL. A wrong ACCEPT decision costs cargo; a wrong REJECT just adds one more investigation round.

Respond with JSON only."""


CRITIC_RESPONSE_FORMAT = """{
  "reasoning": "Step by step evaluation of evidence quality...",
  "source_evaluation": {
    "faa_nas": {"present": true/false, "quality": "good/stale/missing", "notes": "..."},
    "metar": {"present": true/false, "quality": "good/stale/missing", "notes": "..."},
    "taf": {"present": true/false, "quality": "good/stale/missing", "notes": "..."},
    "nws_alerts": {"present": true/false, "quality": "good/stale/missing", "notes": "..."},
    "opensky": {"present": true/false, "quality": "good/stale/missing", "notes": "..."}
  },
  "consistency_analysis": "How well do sources align...",
  "critical_gaps": ["list of gaps that matter"],
  "minor_gaps": ["list of gaps that don't matter much"],
  "confidence_in_evidence": 0.0 to 1.0,
  "verdict": "ACCEPTABLE or INSUFFICIENT",
  "verdict_rationale": "One sentence explaining the verdict"
}"""


class CriticAgent:
    """
    LLM-powered evidence quality critic.

    Uses LLM reasoning to evaluate evidence quality, with hard guardrails
    that cannot be overridden.
    """

    def __init__(self, case_id: UUID, session: Session):
        self.case_id = case_id
        self.session = session
        self.llm = get_llm_client()
        self._rejection_count = 0

    def critique(
        self,
        belief_state: BeliefState,
        risk_assessment: Optional[Dict[str, Any]],
    ) -> str:
        """
        Critique the current belief state and risk assessment.

        Uses LLM for reasoning, with hard guardrails as safety checks.

        Args:
            belief_state: Current belief state
            risk_assessment: Risk assessment from RiskQuantAgent

        Returns:
            Verdict: ACCEPTABLE or INSUFFICIENT_EVIDENCE
        """
        self._rejection_count = self._get_rejection_count()

        # ============================================================
        # HARD GUARDRAILS (cannot be overridden by LLM)
        # ============================================================
        hard_fail_reasons = []

        # Guardrail 1: Minimum VALID evidence threshold
        # Use valid_evidence_count, NOT evidence_count
        # API errors don't count as valid evidence for decision-making
        if belief_state.valid_evidence_count < 2:
            hard_fail_reasons.append(
                f"Hard guardrail: Only {belief_state.valid_evidence_count} valid evidence sources "
                f"(total: {belief_state.evidence_count}, errors: {belief_state.error_evidence_count})"
            )

        # Guardrail 2: Max rejections (prevent infinite loops)
        if self._rejection_count >= MAX_REJECTIONS:
            self._log_critique_result(
                verdict="ACCEPTABLE",
                reasoning=f"Accepting after {self._rejection_count} rejections (loop prevention)",
                llm_assessment=None,
            )
            return VERDICT_ACCEPTABLE

        # ============================================================
        # LLM REASONING
        # ============================================================
        llm_assessment = self._get_llm_critique(belief_state, risk_assessment)

        # ============================================================
        # COMBINE LLM REASONING WITH GUARDRAILS
        # ============================================================

        # If hard guardrails failed, reject regardless of LLM opinion
        if hard_fail_reasons and belief_state.budget_remaining:
            self._log_critique_result(
                verdict="INSUFFICIENT_EVIDENCE",
                reasoning="; ".join(hard_fail_reasons),
                llm_assessment=llm_assessment,
            )
            return VERDICT_INSUFFICIENT_EVIDENCE

        # If we have enough VALID evidence (3+ sources), trust LLM verdict
        # Use valid_evidence_count, NOT evidence_count
        if belief_state.valid_evidence_count >= MIN_REQUIRED_SOURCES:
            llm_verdict = llm_assessment.get("verdict", "ACCEPTABLE")
            final_verdict = VERDICT_ACCEPTABLE if llm_verdict == "ACCEPTABLE" else VERDICT_INSUFFICIENT_EVIDENCE

            # But don't reject if budget exhausted
            if final_verdict == VERDICT_INSUFFICIENT_EVIDENCE and not belief_state.budget_remaining:
                final_verdict = VERDICT_ACCEPTABLE

            self._log_critique_result(
                verdict=final_verdict,
                reasoning=llm_assessment.get("verdict_rationale", "LLM assessment"),
                llm_assessment=llm_assessment,
            )
            return final_verdict

        # Edge case: 2 sources - be more lenient
        self._log_critique_result(
            verdict="ACCEPTABLE",
            reasoning="Marginal evidence (2 sources) but proceeding",
            llm_assessment=llm_assessment,
        )
        return VERDICT_ACCEPTABLE

    def _get_llm_critique(
        self,
        belief_state: BeliefState,
        risk_assessment: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Get LLM-based evidence critique."""
        # Build context for LLM
        context = self._build_critique_context(belief_state, risk_assessment)

        try:
            assessment = self.llm.complete_json(
                system=CRITIC_SYSTEM_PROMPT,
                messages=[
                    {
                        "role": "user",
                        "content": f"""Evaluate the evidence quality for this airport disruption case.

EVIDENCE SUMMARY:
{json.dumps(context, indent=2)}

RESPONSE FORMAT:
{CRITIC_RESPONSE_FORMAT}

Provide your assessment:""",
                    }
                ],
                temperature=0.0,
            )
            return assessment

        except Exception as e:
            # FIXED: Fail-CLOSED, not fail-open
            # If LLM fails, the critic cannot verify evidence quality.
            # A critic that auto-accepts when it can't critique is useless.
            # Better to require reinvestigation than blindly accept.
            return {
                "verdict": "INSUFFICIENT_EVIDENCE",
                "verdict_rationale": f"LLM critique unavailable ({str(e)}). Cannot verify evidence quality - requiring reinvestigation.",
                "confidence_in_evidence": 0.0,
                "critical_gaps": ["LLM critique unavailable - evidence quality unverified"],
            }

    def _build_critique_context(
        self,
        belief_state: BeliefState,
        risk_assessment: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build context for LLM critique."""
        # Get evidence details
        evidence_details = self._get_evidence_details()

        # Get confidence breakdown from risk assessment
        confidence_breakdown = {}
        if risk_assessment:
            confidence_breakdown = risk_assessment.get("confidence_breakdown", {})

        return {
            "evidence_count": belief_state.evidence_count,
            "contradiction_count": belief_state.contradiction_count,
            "uncertainty_count": belief_state.uncertainty_count,
            "hypotheses_count": len(belief_state.hypotheses),
            "evidence_sources": evidence_details,
            "confidence_breakdown": confidence_breakdown,
            "proposed_posture": risk_assessment.get("recommended_posture") if risk_assessment else None,
            "risk_level": risk_assessment.get("risk_level") if risk_assessment else None,
            "stale_sources": self._check_evidence_staleness(),
        }

    def _get_evidence_details(self) -> List[Dict[str, Any]]:
        """Get detailed evidence information."""
        result = self.session.execute(
            text("""
                SELECT e.source_system, e.retrieved_at, e.excerpt
                FROM evidence e
                JOIN trace_event t ON t.ref_id::text = e.id::text
                WHERE t.case_id = :case_id
                  AND t.ref_type = 'evidence'
                ORDER BY e.retrieved_at DESC
            """),
            {"case_id": self.case_id}
        )

        details = []
        for row in result:
            excerpt = row[2] or ""
            # Extract status from excerpt JSON if present
            status = "success"
            if '"status": "api_error"' in excerpt or '"status":"api_error"' in excerpt:
                status = "api_error"
            elif '"status": "normal_operations"' in excerpt or '"status":"normal_operations"' in excerpt:
                status = "normal_operations"

            details.append({
                "source": row[0],
                "retrieved_at": row[1].isoformat() if row[1] else None,
                "excerpt_preview": excerpt[:200] if excerpt else None,
                "status": status,
            })
        return details

    def _get_rejection_count(self) -> int:
        """Count how many times critic has rejected for this case."""
        result = self.session.execute(
            text("""
                SELECT COUNT(*) FROM trace_event
                WHERE case_id = :case_id
                  AND event_type = 'GUARDRAIL_FAIL'
                  AND ref_type = 'critic'
            """),
            {"case_id": self.case_id}
        )
        return result.scalar() or 0

    def _check_evidence_staleness(self) -> List[str]:
        """Check for stale evidence (older than 30 minutes)."""
        from datetime import timedelta

        threshold = datetime.now(timezone.utc) - timedelta(minutes=30)

        result = self.session.execute(
            text("""
                SELECT DISTINCT source_system
                FROM evidence
                WHERE retrieved_at < :threshold
                  AND id IN (
                      SELECT ref_id::uuid FROM trace_event
                      WHERE case_id = :case_id AND ref_type = 'evidence'
                  )
            """),
            {"threshold": threshold, "case_id": self.case_id}
        )

        return [row[0] for row in result]

    def _log_critique_result(
        self,
        verdict: str,
        reasoning: str,
        llm_assessment: Optional[Dict[str, Any]],
    ):
        """Log critique result to trace."""
        event_type = "GUARDRAIL_FAIL" if verdict != "ACCEPTABLE" else "TOOL_RESULT"

        meta = {
            "verdict": verdict,
            "reasoning": reasoning,
            "rejection_count": self._rejection_count,
        }

        if llm_assessment:
            meta["llm_critique"] = {
                "confidence_in_evidence": llm_assessment.get("confidence_in_evidence"),
                "critical_gaps": llm_assessment.get("critical_gaps", []),
                "consistency_analysis": llm_assessment.get("consistency_analysis"),
            }

        seq = get_next_trace_seq(self.case_id, self.session)
        self.session.execute(
            text("""
                INSERT INTO trace_event
                (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                VALUES (:id, :case_id, :seq, :event_type, 'critic', :case_id_str, CAST(:meta AS jsonb), :created_at)
            """),
            {
                "id": uuid4(),
                "case_id": self.case_id,
                "seq": seq,
                "case_id_str": str(self.case_id),
                "event_type": event_type,
                "meta": json.dumps(meta),
                "created_at": datetime.now(timezone.utc),
            }
        )
        self.session.commit()
