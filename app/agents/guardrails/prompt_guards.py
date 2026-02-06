# app/agents/guardrails/prompt_guards.py
"""
Prompt guardrails for LLM calls.

Ensures LLM outputs are evidence-bound and auditable.
"""

# ============================================================
# EVIDENCE BINDING PROMPT
# ============================================================
# Added to system prompt for ALL LLM calls.
# This is a CRITICAL guardrail against unevidenced claims.

EVIDENCE_BINDING_PROMPT = """
CRITICAL CONSTRAINT - EVIDENCE BINDING:
You MUST NOT make any factual claim (status=FACT) without citing
specific evidence IDs from tool results.

WRONG: "JFK has a ground stop" (no evidence cited)
RIGHT: "JFK has a ground stop [evidence_id: ev_abc123 from FAA NAS]"

If you cannot cite evidence, the claim MUST be status=HYPOTHESIS.
The system will reject FACT claims without evidence bindings.

Before proposing any action, verify you have cited evidence for
the claims that justify that action.

CLAIM STATUS GUIDE:
- DRAFT: Initial claim, not yet verified
- HYPOTHESIS: Plausible but unverified claim
- FACT: Verified claim WITH evidence binding
- RETRACTED: Previously believed but now known false

EVIDENCE CITATION FORMAT:
When making a FACT claim, include: [evidence_id: {id} from {source}]
Example: "Airport is experiencing ground delays [evidence_id: ev_123 from FAA_NAS]"
"""


# ============================================================
# POSTURE DIRECTIVE PROMPT
# ============================================================
# Explains the posture options to the LLM.

POSTURE_DIRECTIVE_PROMPT = """
GATEWAY POSTURE DIRECTIVES:
You must recommend one of these postures:

- ACCEPT: Normal operations, accept new bookings
- RESTRICT: Limit specific service tiers/SLAs
- HOLD: Pause new tendering until evidence clears
- ESCALATE: Escalate to duty manager for human review

Choose ACCEPT only when evidence shows normal operations.
Choose RESTRICT when minor disruptions affect specific services.
Choose HOLD when significant disruptions create uncertainty.
Choose ESCALATE when contradictions or critical issues require human judgment.
"""


# ============================================================
# ACTION CONSTRAINT PROMPT
# ============================================================
# Explains action constraints to the LLM.

ACTION_CONSTRAINT_PROMPT = """
ACTION CONSTRAINTS:

SHIPMENT ACTIONS (require booking evidence):
- HOLD_CARGO, RELEASE_CARGO, SWITCH_GATEWAY
- REBOOK_FLIGHT, UPGRADE_SERVICE
- NOTIFY_CUSTOMER, FILE_CLAIM

You CANNOT propose shipment actions without booking evidence.
If no booking evidence exists, stay at posture level only.

POSTURE/OPERATIONAL ACTIONS (no booking required):
- SET_POSTURE: Set gateway posture directive
- PUBLISH_GATEWAY_ADVISORY: Notify downstream systems
- UPDATE_BOOKING_RULES: Update rules for new bookings
- TRIGGER_REEVALUATION: Force re-evaluation of pending decisions
- ESCALATE_OPS: Escalate to duty manager

HIGH RISK ACTIONS require approval:
- SWITCH_GATEWAY, REBOOK_FLIGHT, UPGRADE_SERVICE, FILE_CLAIM
"""


def build_agent_system_prompt(base_prompt: str) -> str:
    """
    Build complete system prompt with guardrails.

    Prepends evidence binding and other guardrails to agent prompt.

    Args:
        base_prompt: Base agent-specific prompt

    Returns:
        Complete system prompt with guardrails
    """
    return "\n\n".join([
        EVIDENCE_BINDING_PROMPT,
        POSTURE_DIRECTIVE_PROMPT,
        ACTION_CONSTRAINT_PROMPT,
        "---",
        base_prompt,
    ])


def build_investigation_prompt(context: dict) -> str:
    """
    Build prompt for investigation phase.

    Args:
        context: Current context (case scope, available tools, etc.)

    Returns:
        Investigation prompt
    """
    base = f"""
INVESTIGATION TASK:
Investigate the current situation for {context.get('airport', 'the airport')}.

Available evidence sources:
1. FAA NAS Status - Official airport status
2. METAR/TAF - Current and forecast weather
3. NWS Alerts - Weather alerts
4. OpenSky - Aircraft traffic

Gather evidence from available sources and identify:
1. Current disruptions and their severity
2. Uncertainties that need resolution
3. Contradictions between sources

Report findings with evidence citations.
"""
    return build_agent_system_prompt(base)


def build_risk_assessment_prompt(context: dict) -> str:
    """
    Build prompt for risk assessment phase.

    Args:
        context: Current context (evidence, signals, etc.)

    Returns:
        Risk assessment prompt
    """
    base = f"""
RISK ASSESSMENT TASK:
Assess the operational risk based on gathered evidence.

Evidence summary:
{context.get('evidence_summary', 'No evidence yet')}

Quantify:
1. Severity of each disruption signal
2. Overall risk level (LOW, MEDIUM, HIGH, CRITICAL)
3. Recommended posture directive

Support all assessments with evidence citations.
"""
    return build_agent_system_prompt(base)
