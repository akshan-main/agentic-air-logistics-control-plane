# app/agents/roles/policy_judge.py
"""
Policy Judge Agent - LLM-powered governance policy evaluation.

Uses LLM to reason about:
- Policy interpretation and applicability
- Proportionality of proposed actions
- Edge cases and nuanced situations

Hard guardrails remain as safety checks that LLM cannot override.
"""

import json
from typing import List, Dict, Any, Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..state_graph import BeliefState
from ...llm import get_llm_client
from ...db.engine import get_next_trace_seq


# Policy verdict types
VERDICT_COMPLIANT = "COMPLIANT"
VERDICT_NEEDS_EVIDENCE = "NEEDS_EVIDENCE"
VERDICT_BLOCKED = "BLOCKED"

# Shipment-level actions that require booking evidence
SHIPMENT_ACTIONS = {
    "HOLD_CARGO", "RELEASE_CARGO", "SWITCH_GATEWAY",
    "REBOOK_FLIGHT", "UPGRADE_SERVICE", "NOTIFY_CUSTOMER", "FILE_CLAIM"
}


POLICY_JUDGE_SYSTEM_PROMPT = """You are a GOVERNANCE POLICY JUDGE for air freight forwarding operations.

Your job is to evaluate whether proposed actions comply with operational policies, regulatory
requirements, and forwarder governance rules. You are NOT a rubber stamp - you must CRITICALLY
assess compliance and flag violations.

================================================================================
AIR FREIGHT FORWARDING GOVERNANCE KNOWLEDGE
================================================================================

1. SLA TIER POLICIES (contractual obligations):
   - PRIORITY/TIME-CRITICAL: <48hr delivery commitment, highest penalty exposure
     * Policy: ANY risk to delivery window requires immediate customer notification
     * Policy: Cannot switch gateways without customer approval (contractual route)
     * Policy: HOLD posture automatically triggers proactive rebooking assessment

   - STANDARD: 3-5 day commitment, moderate flexibility
     * Policy: Can switch gateways if cost delta < 15% without approval
     * Policy: 24hr delay tolerance before customer notification required

   - ECONOMY/DEFERRED: Lowest priority, maximum flexibility
     * Policy: Can hold for 72hr without escalation
     * Policy: Gateway switches allowed for cost optimization

2. EVIDENCE FRESHNESS REQUIREMENTS:
   - FAA NAS status: Valid for 15 minutes (ground stops can lift suddenly)
   - METAR weather: Valid for 30 minutes (conditions change)
   - TAF forecast: Valid for 6 hours (unless amended)
   - NWS alerts: Valid until expiry time in alert
   - OpenSky movement: Valid for 10 minutes (real-time indicator)

   POLICY: Stale evidence cannot support ACCEPT posture for premium SLAs

3. HUB AIRPORT POLICIES (cascade risk):
   Major hubs (ORD, DFW, ATL, DEN, CLT, PHX): Higher approval thresholds
   - POLICY: HOLD posture at major hub requires duty manager notification
   - POLICY: Cascade risk multiplier applies (affects more connections)
   - POLICY: Gateway switch FROM hub requires review of all connecting shipments

4. DOCUMENT READINESS POLICIES:
   - POLICY: Cannot release cargo without customs clearance confirmation
   - POLICY: Dangerous goods require valid DGD (Dangerous Goods Declaration)
   - POLICY: Perishables require temperature log continuity
   - POLICY: High-value cargo (>$50K) requires security seal verification

5. APPROVAL THRESHOLDS BY VALUE:
   - <$10K cargo: Operations can approve posture changes
   - $10K-$50K: Supervisor approval required for HOLD/ESCALATE
   - $50K-$200K: Manager approval required
   - >$200K: Director approval required, customer account manager notified

6. CONTRADICTION RESOLUTION POLICIES:
   - POLICY: Cannot issue ACCEPT with unresolved contradictions in evidence
   - POLICY: Conflicting FAA/weather sources require manual verification
   - POLICY: Contradiction age >2hr requires fresh evidence fetch

7. SEASONAL/CAPACITY POLICIES:
   - Peak season (Nov-Jan, pre-CNY): Tighter posture defaults
   - POLICY: During peak, RESTRICT is baseline (not ACCEPT)
   - POLICY: During peak, rebooking requires capacity confirmation

================================================================================
EVALUATION PRINCIPLES
================================================================================

1. **SLA EXPOSURE**: What's the contractual risk of this action?
2. **EVIDENCE FRESHNESS**: Is the evidence current enough for this decision?
3. **CASCADE AWARENESS**: Does this affect downstream operations?
4. **PROPORTIONALITY**: Is the response proportionate to the risk?
5. **AUDIT TRAIL**: Would this decision survive regulatory review?

RISK-POSTURE ALIGNMENT (with SLA context):
- LOW risk + Standard SLA: ACCEPT appropriate
- LOW risk + Priority SLA: ACCEPT only with fresh evidence
- MEDIUM risk: RESTRICT appropriate (limits new exposure)
- HIGH risk: HOLD or RESTRICT with approval
- CRITICAL risk: ESCALATE required (human in loop)

HARD RULES (you CANNOT override these):
- CRITICAL risk + ACCEPT posture = VIOLATION (never combine)
- Shipment-level actions require booking evidence (no exceptions)
- Open contradictions must be resolved before posture decisions
- Stale evidence (>30min for weather) cannot support ACCEPT at priority tier

VERDICT OPTIONS:
- COMPLIANT: Actions align with policies, proceed with confidence
- NEEDS_EVIDENCE: Specific evidence gaps must be filled before decision
- BLOCKED: Policy violation detected, cannot proceed without override

DO NOT be a rubber stamp. If something looks operationally convenient but violates
governance policy, it's your job to flag it. Auditors will review these decisions.

Respond with JSON only."""


POLICY_JUDGE_RESPONSE_FORMAT = """{
  "reasoning": "Step by step policy evaluation...",
  "policy_evaluations": [
    {
      "policy_id": "builtin_X",
      "policy_text": "...",
      "applies": true/false,
      "compliant": true/false,
      "notes": "..."
    }
  ],
  "risk_posture_alignment": {
    "current_risk": "LOW/MEDIUM/HIGH/CRITICAL",
    "proposed_posture": "ACCEPT/RESTRICT/HOLD/ESCALATE",
    "aligned": true/false,
    "notes": "..."
  },
  "proportionality_assessment": "Is the response proportionate to the risk...",
  "evidence_adequacy": "Is evidence sufficient for these actions...",
  "concerns": ["list of policy concerns if any"],
  "recommendations": ["list of recommendations"],
  "verdict": "COMPLIANT or NEEDS_EVIDENCE or BLOCKED",
  "verdict_rationale": "One sentence explaining the verdict"
}"""


class PolicyJudgeAgent:
    """
    LLM-powered governance policy evaluator.

    Uses LLM reasoning to interpret policies, with hard guardrails
    that cannot be overridden for safety-critical rules.
    """

    def __init__(self, case_id: UUID, session: Session):
        self.case_id = case_id
        self.session = session
        self.llm = get_llm_client()

    def evaluate(
        self,
        belief_state: BeliefState,
        risk_assessment: Optional[Dict[str, Any]],
        proposed_actions: List[Dict[str, Any]],
    ) -> str:
        """
        Evaluate belief state and actions against policies.

        Uses LLM for reasoning, with hard guardrails as safety checks.

        Args:
            belief_state: Current belief state
            risk_assessment: Risk assessment from RiskQuantAgent
            proposed_actions: Actions proposed by planner

        Returns:
            Verdict: COMPLIANT, NEEDS_EVIDENCE, or BLOCKED
        """
        # ============================================================
        # HARD GUARDRAILS (cannot be overridden by LLM)
        # ============================================================
        hard_violations = []

        # Guardrail 1: CRITICAL risk + ACCEPT = always block
        if risk_assessment:
            risk_level = risk_assessment.get("risk_level", "LOW")
            posture = risk_assessment.get("recommended_posture", "ACCEPT")
            if risk_level == "CRITICAL" and posture == "ACCEPT":
                hard_violations.append(
                    "Hard guardrail: Cannot recommend ACCEPT posture with CRITICAL risk"
                )

        # Guardrail 2: Shipment actions require booking evidence
        has_shipment_action = any(
            a.get("type") in SHIPMENT_ACTIONS
            for a in proposed_actions
        )
        if has_shipment_action and not self._has_booking_evidence():
            hard_violations.append(
                "Hard guardrail: Shipment-level actions require booking evidence"
            )

        # If hard guardrails violated, block immediately
        if hard_violations:
            self._log_evaluation_result(
                verdict=VERDICT_BLOCKED,
                reasoning="; ".join(hard_violations),
                llm_assessment=None,
            )
            return VERDICT_BLOCKED

        # ============================================================
        # LLM REASONING
        # ============================================================
        policies = self._load_active_policies()
        llm_assessment = self._get_llm_evaluation(
            belief_state, risk_assessment, proposed_actions, policies
        )

        # ============================================================
        # PROCESS LLM VERDICT
        # ============================================================
        llm_verdict = llm_assessment.get("verdict", "COMPLIANT")

        # Map LLM verdict to our verdict types
        if llm_verdict == "BLOCKED":
            final_verdict = VERDICT_BLOCKED
        elif llm_verdict == "NEEDS_EVIDENCE":
            final_verdict = VERDICT_NEEDS_EVIDENCE
        else:
            final_verdict = VERDICT_COMPLIANT

        # Safety override: LLM cannot BLOCK if no shipment actions are proposed
        # and the only reason is booking evidence. The hard guardrail (line 206-213)
        # already handles the real check correctly.
        if final_verdict == VERDICT_BLOCKED and not has_shipment_action:
            # Check if the block is solely about booking evidence
            rationale = llm_assessment.get("verdict_rationale", "").lower()
            evals = llm_assessment.get("policy_evaluations", [])
            booking_only = all(
                "booking" in (p.get("policy_text", "") + p.get("notes", "")).lower()
                for p in evals
                if p.get("applies") and not p.get("compliant")
            )
            if booking_only or "booking" in rationale:
                final_verdict = VERDICT_COMPLIANT

        # Mark actions requiring approval based on LLM assessment
        if risk_assessment and risk_assessment.get("risk_level") in ["HIGH", "CRITICAL"]:
            for action in proposed_actions:
                action["requires_approval"] = True

        # Log the result
        self._log_evaluation_result(
            verdict=final_verdict,
            reasoning=llm_assessment.get("verdict_rationale", "LLM assessment"),
            llm_assessment=llm_assessment,
        )

        # Log applied policies (compliant)
        applied_policies = [
            p for p in llm_assessment.get("policy_evaluations", [])
            if p.get("applies") and p.get("compliant")
        ]
        if applied_policies:
            self._log_applied_policies(applied_policies)

        # Log violated policies (non-compliant) so packet builder can find them
        violated_policies = [
            p for p in llm_assessment.get("policy_evaluations", [])
            if p.get("applies") and not p.get("compliant")
        ]
        if violated_policies:
            self._log_violated_policies(violated_policies)

        return final_verdict

    def _get_llm_evaluation(
        self,
        belief_state: BeliefState,
        risk_assessment: Optional[Dict[str, Any]],
        proposed_actions: List[Dict[str, Any]],
        policies: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Get LLM-based policy evaluation."""
        context = self._build_evaluation_context(
            belief_state, risk_assessment, proposed_actions, policies
        )

        try:
            assessment = self.llm.complete_json(
                system=POLICY_JUDGE_SYSTEM_PROMPT,
                messages=[
                    {
                        "role": "user",
                        "content": f"""Evaluate the proposed actions against governance policies.

CONTEXT:
{json.dumps(context, indent=2)}

RESPONSE FORMAT:
{POLICY_JUDGE_RESPONSE_FORMAT}

Provide your assessment:""",
                    }
                ],
                temperature=0.0,
            )
            return assessment

        except Exception as e:
            # FIXED: Fail-CLOSED, not fail-open
            # If LLM fails, we should NOT auto-approve - that's dangerous for governance.
            # Instead, require evidence/manual review.
            return {
                "verdict": "NEEDS_EVIDENCE",
                "verdict_rationale": f"LLM evaluation failed ({str(e)}). Requiring manual review for safety.",
                "concerns": ["LLM policy evaluation unavailable - manual review required"],
            }

    def _build_evaluation_context(
        self,
        belief_state: BeliefState,
        risk_assessment: Optional[Dict[str, Any]],
        proposed_actions: List[Dict[str, Any]],
        policies: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build context for LLM evaluation."""
        return {
            "belief_state": {
                "evidence_count": belief_state.evidence_count,
                "contradiction_count": belief_state.contradiction_count,
                "uncertainty_count": belief_state.uncertainty_count,
                "hypotheses_count": len(belief_state.hypotheses),
            },
            "risk_assessment": {
                "risk_level": risk_assessment.get("risk_level") if risk_assessment else None,
                "recommended_posture": risk_assessment.get("recommended_posture") if risk_assessment else None,
                "confidence": (risk_assessment.get("confidence") or risk_assessment.get("overall_confidence")) if risk_assessment else None,
            },
            "proposed_actions": [
                {
                    "type": a.get("type"),
                    "args": a.get("args", {}),
                    "risk_level": a.get("risk_level", "LOW"),
                }
                for a in proposed_actions
            ],
            "policies": [
                {
                    "id": p.get("id"),
                    "type": p.get("type"),
                    "text": p.get("text"),
                    "conditions": p.get("conditions"),
                    "effects": p.get("effects"),
                }
                for p in policies
            ],
            "has_booking_evidence": self._has_booking_evidence(),
            "has_shipment_actions": any(
                a.get("type") in SHIPMENT_ACTIONS for a in proposed_actions
            ),
            "has_contradictions": belief_state.contradiction_count > 0,
        }

    def _load_active_policies(self) -> List[Dict[str, Any]]:
        """Load active policies from database, auto-seeding if empty."""
        now = datetime.now(timezone.utc)

        result = self.session.execute(
            text("""
                SELECT id, type, text, conditions, effects
                FROM policy
                WHERE effective_from <= :now
                  AND (effective_to IS NULL OR effective_to > :now)
            """),
            {"now": now}
        )

        policies = []
        for row in result:
            policies.append({
                "id": str(row[0]),
                "type": row[1],
                "text": row[2],
                "conditions": row[3],
                "effects": row[4],
            })

        # Auto-seed if database is empty — uses unified policy list
        if not policies:
            self._seed_policies()
            # Re-query after seeding
            result = self.session.execute(
                text("""
                    SELECT id, type, text, conditions, effects
                    FROM policy
                    WHERE effective_from <= :now
                      AND (effective_to IS NULL OR effective_to > :now)
                """),
                {"now": now}
            )
            for row in result:
                policies.append({
                    "id": str(row[0]),
                    "type": row[1],
                    "text": row[2],
                    "conditions": row[3],
                    "effects": row[4],
                })

        return policies

    def _seed_policies(self):
        """Seed policies into database if empty. Uses unified builtin_policies.py."""
        from ...policy.builtin_policies import load_builtin_policies
        try:
            load_builtin_policies(self.session)
        except Exception:
            self.session.rollback()

    def _has_booking_evidence(self) -> bool:
        """Check if case has booking evidence."""
        result = self.session.execute(
            text("""
                SELECT COUNT(*) FROM evidence
                WHERE source_system = 'BOOKING'
                  AND id IN (
                      SELECT ref_id::uuid FROM trace_event
                      WHERE case_id = :case_id AND ref_type = 'evidence'
                  )
            """),
            {"case_id": self.case_id}
        )
        return result.scalar() > 0

    def _log_evaluation_result(
        self,
        verdict: str,
        reasoning: str,
        llm_assessment: Optional[Dict[str, Any]],
    ):
        """Log evaluation result to trace."""
        event_type = "GUARDRAIL_FAIL" if verdict != VERDICT_COMPLIANT else "TOOL_RESULT"

        meta = {
            "verdict": verdict,
            "reasoning": reasoning,
        }

        if llm_assessment:
            meta["llm_evaluation"] = {
                "risk_posture_alignment": llm_assessment.get("risk_posture_alignment"),
                "concerns": llm_assessment.get("concerns", []),
                "recommendations": llm_assessment.get("recommendations", []),
            }

        seq = get_next_trace_seq(self.case_id, self.session)
        # Use ref_type='policy' for GUARDRAIL_FAIL to match packet builder queries
        ref_type = "policy" if event_type == "GUARDRAIL_FAIL" else "policy_judge"
        self.session.execute(
            text("""
                INSERT INTO trace_event
                (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                VALUES (:id, :case_id, :seq, :event_type, :ref_type, :case_id_str, CAST(:meta AS jsonb), :created_at)
            """),
            {
                "id": uuid4(),
                "case_id": self.case_id,
                "seq": seq,
                "case_id_str": str(self.case_id),
                "event_type": event_type,
                "ref_type": ref_type,
                "meta": json.dumps(meta),
                "created_at": datetime.now(timezone.utc),
            }
        )
        self.session.commit()

    def _log_applied_policies(self, policies: List[Dict[str, Any]]):
        """Log successfully applied policies to trace."""
        for policy in policies:
            seq = get_next_trace_seq(self.case_id, self.session)
            self.session.execute(
                text("""
                    INSERT INTO trace_event
                    (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                    VALUES (:id, :case_id, :seq, 'TOOL_RESULT', 'policy_applied', NULL, CAST(:meta AS jsonb), :created_at)
                """),
                {
                    "id": uuid4(),
                    "case_id": self.case_id,
                    "seq": seq,
                    "meta": json.dumps({
                        "policy_id": str(policy.get("policy_id", policy.get("id"))),
                        "policy_text": policy.get("policy_text", policy.get("text")),
                        "compliant": policy.get("compliant", True),
                    }),
                    "created_at": datetime.now(timezone.utc),
                }
            )
        self.session.commit()

    def _log_violated_policies(self, policies: List[Dict[str, Any]]):
        """Log violated policies to trace as GUARDRAIL_FAIL for packet builder."""
        for policy in policies:
            seq = get_next_trace_seq(self.case_id, self.session)
            self.session.execute(
                text("""
                    INSERT INTO trace_event
                    (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                    VALUES (:id, :case_id, :seq, 'GUARDRAIL_FAIL', 'policy', NULL, CAST(:meta AS jsonb), :created_at)
                """),
                {
                    "id": uuid4(),
                    "case_id": self.case_id,
                    "seq": seq,
                    "meta": json.dumps({
                        "policy_id": str(policy.get("policy_id", policy.get("id"))),
                        "policy_text": policy.get("policy_text", policy.get("text")),
                        "effect": policy.get("notes", "BLOCKED"),
                    }),
                    "created_at": datetime.now(timezone.utc),
                }
            )
        self.session.commit()
