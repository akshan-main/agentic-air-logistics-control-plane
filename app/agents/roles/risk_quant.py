# app/agents/roles/risk_quant.py
"""
Risk Quantification Agent - LLM-based risk assessment.

Uses LLM to reason about operational exposure from disruptions.
NO RULE-BASED LOGIC - all reasoning is done by the LLM.
"""

import json
from typing import Dict, Any, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..state_graph import BeliefState, Posture
from ...llm import get_llm_client


RISK_ASSESSMENT_SYSTEM_PROMPT = """You are a SUPPLY CHAIN RISK QUANTIFICATION specialist for air freight forwarders.

You have DOMAIN EXPERTISE that generic analysts lack:

## YOUR SPECIALIZED KNOWLEDGE

### 1. FORWARDER ECONOMICS
- Forwarders don't own planes - they BUY space from carriers and SELL to shippers
- Margin is typically 15-25% - a missed SLA can wipe out profit on that shipment
- PREMIUM cargo (pharma, perishables) has 4-12 hour SLAs at 2-3x margin
- A single ground stop at a hub can create $500K-2M in rebooking costs

### 2. CASCADE MATHEMATICS
- Hub airports (JFK, ORD, LAX, ATL) have 5-10x cascade multiplier
- Point-to-point routes (secondary airports) have 1-2x multiplier
- Every 1-hour delay at origin = 3-4 hours total journey delay (connections)
- Perishables have 48-72 hour total viability - delays can mean total loss

### 3. SLA RISK TIERS
- EXPRESS (4-12h): Ground stop = almost certain SLA breach, HOLD immediately
- PREMIUM (12-24h): Ground stop = 60% breach risk, RESTRICT premium
- STANDARD (48-72h): Can absorb 4-6 hour delays, RESTRICT only if >8h expected

### 4. SIGNAL INTERPRETATION
- "Ground Delay Program" = 30-90 min average, not critical unless hub
- "Ground Stop" = All departures halted, HOLD for that airport
- "Closure" = Rare, usually weather (hurricane), ESCALATE
- Wind 25+ kts OR visibility <3mi OR ceiling <500ft = de facto IFR, treat as delay even if FAA says normal

## POSTURE DECISION MATRIX

| Condition | Posture | Why |
|-----------|---------|-----|
| Ground stop at hub, SLA exposure >$100K | HOLD | High financial risk, can't rebook easily |
| Ground stop at secondary, SLA exposure <$50K | RESTRICT | Limit new premium, watch existing |
| GDP only, <60 min avg delay | RESTRICT | Marginally extend SLAs, limit new EXPRESS |
| IFR weather, no FAA action yet | RESTRICT | Precautionary - FAA may be delayed in reporting |
| Normal ops, all sources agree | ACCEPT | Green light |
| Sources contradict, can't determine | ESCALATE | Human judgment needed |

## CONFIDENCE CALIBRATION

Your confidence should reflect:
- 0.85-0.95: All 4+ sources agree, clear signal
- 0.70-0.84: 3 sources agree, minor gaps
- 0.50-0.69: 2 sources, or some contradiction
- <0.50: Major data gaps, should probably ESCALATE

Respond with JSON only."""


RISK_ASSESSMENT_RESPONSE_FORMAT = """{
  "reasoning": "Step by step analysis of the situation...",
  "signal_analysis": {
    "faa": "Analysis of FAA status...",
    "weather": "Analysis of weather conditions...",
    "alerts": "Analysis of NWS alerts...",
    "movement": "Analysis of aircraft movement..."
  },
  "risk_factors": ["list", "of", "key", "risk", "factors"],
  "mitigating_factors": ["list", "of", "mitigating", "factors"],
  "uncertainty_impact": "How missing data affects assessment...",
  "overall_severity": 0.0 to 1.0,
  "risk_level": "LOW|MEDIUM|HIGH|CRITICAL",
  "recommended_posture": "ACCEPT|RESTRICT|HOLD|ESCALATE",
  "confidence": 0.0 to 1.0,
  "rationale": "One sentence summary of why this posture..."
}"""


class RiskQuantAgent:
    """
    LLM-based risk quantification agent.

    Uses LLM reasoning to assess disruption risk and recommend posture.
    No rule-based fallbacks - LLM makes all decisions.
    """

    def __init__(self, case_id: UUID, session: Session):
        self.case_id = case_id
        self.session = session
        self.llm = get_llm_client()

    def assess_risk(self, belief_state: BeliefState) -> Dict[str, Any]:
        """
        Assess overall risk using LLM reasoning.

        Args:
            belief_state: Current belief state with evidence

        Returns:
            Risk assessment dict with posture recommendation
        """
        # Gather all signals for LLM
        signals = self._get_signals(belief_state)

        # Build context for LLM
        context = self._build_assessment_context(signals, belief_state)

        # Get LLM assessment
        assessment = self._get_llm_assessment(context)

        # Calculate dynamic confidence based on data quality with explanation
        # (overwrites LLM's static confidence)
        confidence, confidence_breakdown = self._calculate_confidence_with_explanation(signals, belief_state)
        assessment["confidence"] = confidence
        assessment["confidence_breakdown"] = confidence_breakdown

        # Create claims from LLM assessment
        self._create_claims_from_assessment(assessment, belief_state)

        # Add metadata
        assessment["evidence_count"] = belief_state.evidence_count
        assessment["uncertainty_count"] = belief_state.uncertainty_count
        assessment["contradiction_count"] = belief_state.contradiction_count

        # Store risk assessment in trace_event for later retrieval
        self._store_risk_assessment(assessment)

        return assessment

    def _store_risk_assessment(self, assessment: Dict[str, Any]) -> None:
        """Store risk assessment in trace_event for packet retrieval."""
        from uuid import uuid4
        from datetime import datetime, timezone
        import json
        from ...db.engine import get_next_trace_seq

        seq = get_next_trace_seq(self.case_id, self.session)
        self.session.execute(
            text("""
                INSERT INTO trace_event
                (id, case_id, seq, event_type, ref_type, meta, created_at)
                VALUES
                (:id, :case_id, :seq, :event_type, :ref_type,
                 CAST(:meta AS jsonb), :created_at)
            """),
            {
                "id": uuid4(),
                "case_id": self.case_id,
                "seq": seq,
                "event_type": "TOOL_RESULT",
                "ref_type": "risk_assessment",
                "meta": json.dumps({
                    "risk_level": assessment.get("risk_level"),
                    "recommended_posture": assessment.get("recommended_posture"),
                    "confidence": assessment.get("confidence"),
                    "confidence_breakdown": assessment.get("confidence_breakdown"),
                }),
                "created_at": datetime.now(timezone.utc),
            }
        )
        self.session.commit()

    def _create_claims_from_assessment(
        self,
        assessment: Dict[str, Any],
        belief_state: BeliefState,
    ) -> None:
        """Create claims from the LLM risk assessment."""
        from uuid import uuid4
        from datetime import datetime, timezone
        import json

        # Clear existing risk assessment claims to prevent duplicates on re-assessment
        # This happens when critic forces re-investigation and risk_quant runs again
        self._clear_existing_risk_claims(belief_state)

        # Create main risk assessment claim
        risk_level = assessment.get("risk_level", "UNKNOWN")
        posture = assessment.get("recommended_posture", "HOLD")
        confidence = assessment.get("confidence", 0.5)
        rationale = assessment.get("rationale", "")

        claim_id = uuid4()
        self.session.execute(
            text("""
                INSERT INTO claim
                (id, text, subject_node_id, confidence, status, ingested_at)
                VALUES
                (:id, :text, NULL, :confidence, :status, :created_at)
            """),
            {
                "id": claim_id,
                "text": f"Risk level is {risk_level}. Recommended posture: {posture}. {rationale}",
                "confidence": confidence,
                "status": "HYPOTHESIS",  # LLM claims start as HYPOTHESIS
                "created_at": datetime.now(timezone.utc),
            }
        )

        # Link claim to evidence (if we have any)
        if belief_state.evidence_ids:
            for evidence_id in belief_state.evidence_ids[:3]:  # Link to first 3
                self.session.execute(
                    text("""
                        INSERT INTO claim_evidence (claim_id, evidence_id)
                        VALUES (:claim_id, :evidence_id)
                        ON CONFLICT DO NOTHING
                    """),
                    {"claim_id": claim_id, "evidence_id": evidence_id}
                )

        # Create trace_event linking claim to case
        from ...db.engine import get_next_trace_seq
        seq = get_next_trace_seq(self.case_id, self.session)
        self.session.execute(
            text("""
                INSERT INTO trace_event
                (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                VALUES
                (:id, :case_id, :seq, :event_type, :ref_type, :ref_id,
                 CAST(:meta AS jsonb), :created_at)
            """),
            {
                "id": uuid4(),
                "case_id": self.case_id,
                "seq": seq,
                "event_type": "TOOL_RESULT",
                "ref_type": "claim",
                "ref_id": claim_id,
                "meta": json.dumps({
                    "risk_level": risk_level,
                    "posture": posture,
                    "confidence": confidence,
                }),
                "created_at": datetime.now(timezone.utc),
            }
        )

        belief_state.claim_ids.append(claim_id)

        # Create claims for each risk factor
        for factor in assessment.get("risk_factors", [])[:5]:
            factor_claim_id = uuid4()
            self.session.execute(
                text("""
                    INSERT INTO claim
                    (id, text, subject_node_id, confidence, status, ingested_at)
                    VALUES
                    (:id, :text, NULL, :confidence, :status, :created_at)
                """),
                {
                    "id": factor_claim_id,
                    "text": f"Risk factor identified: {factor}",
                    "confidence": confidence * 0.8,  # Slightly lower confidence for factors
                    "status": "HYPOTHESIS",
                    "created_at": datetime.now(timezone.utc),
                }
            )

            # Link to case
            seq = get_next_trace_seq(self.case_id, self.session)
            self.session.execute(
                text("""
                    INSERT INTO trace_event
                    (id, case_id, seq, event_type, ref_type, ref_id, meta, created_at)
                    VALUES
                    (:id, :case_id, :seq, :event_type, :ref_type, :ref_id,
                     CAST(:meta AS jsonb), :created_at)
                """),
                {
                    "id": uuid4(),
                    "case_id": self.case_id,
                    "seq": seq,
                    "event_type": "TOOL_RESULT",
                    "ref_type": "claim",
                    "ref_id": factor_claim_id,
                    "meta": json.dumps({"risk_factor": factor}),
                    "created_at": datetime.now(timezone.utc),
                }
            )

            belief_state.claim_ids.append(factor_claim_id)

        self.session.commit()

    def _clear_existing_risk_claims(self, belief_state: BeliefState) -> None:
        """
        Clear existing risk assessment claims to prevent duplicates.

        This is called when re-assessing risk (e.g., after critic forces re-investigation).
        We remove the trace_event links so they won't show up in the packet,
        and clear from belief_state.claim_ids.
        """
        # Get existing risk claims for this case (claims with "Risk level" or "Risk factor")
        result = self.session.execute(
            text("""
                SELECT c.id FROM claim c
                JOIN trace_event t ON t.ref_id::text = c.id::text
                WHERE t.case_id = :case_id
                  AND t.ref_type = 'claim'
                  AND (c.text LIKE 'Risk level is%' OR c.text LIKE 'Risk factor identified:%')
            """),
            {"case_id": self.case_id}
        )

        existing_claim_ids = [row[0] for row in result]

        if existing_claim_ids:
            # Remove trace_event links (soft delete - claims still exist for audit)
            self.session.execute(
                text("""
                    DELETE FROM trace_event
                    WHERE case_id = :case_id
                      AND ref_type = 'claim'
                      AND ref_id::text = ANY(:claim_ids)
                """),
                {
                    "case_id": self.case_id,
                    "claim_ids": [str(cid) for cid in existing_claim_ids]
                }
            )

            # Remove from belief_state
            belief_state.claim_ids = [
                cid for cid in belief_state.claim_ids
                if cid not in existing_claim_ids
            ]

            self.session.commit()

    def _get_cascade_impact(self, airport_icao: str) -> Optional[Dict[str, Any]]:
        """
        Get cascade impact from context graph.

        This is the key integration between aviation signals and supply chain.
        Returns affected flights, shipments, bookings, and forwarder revenue exposure.
        """
        if not airport_icao or airport_icao == 'Unknown':
            return None

        try:
            from ...graph.traversal import cascade_from_airport

            cascade = cascade_from_airport(
                airport_icao=airport_icao,
                session=self.session
            )

            return {
                "affected_flights": cascade.affected_flights,
                "affected_shipments": cascade.affected_shipments,
                "affected_bookings": cascade.affected_bookings,
                "total_shipments": cascade.total_shipments,
                "total_bookings": cascade.total_bookings,
                # Realistic forwarder metrics
                "total_revenue_at_risk": cascade.total_revenue_at_risk,
                "total_weight_kg": cascade.total_weight_kg,
                "sla_at_risk_count": cascade.sla_at_risk_count,
                "premium_sla_at_risk": cascade.premium_sla_at_risk,
                "express_sla_at_risk": cascade.express_sla_at_risk,
            }
        except Exception as e:
            # Log but don't fail - cascade is enhancement, not required
            return None

    def _get_signals(self, belief_state: BeliefState) -> Dict[str, Any]:
        """Get all signals from edges."""
        signals = {
            "faa": None,
            "weather": None,
            "movement": None,
            "alerts": [],
        }

        if not belief_state.edge_ids:
            return signals

        result = self.session.execute(
            text("""
                SELECT type, attrs FROM edge
                WHERE id = ANY(:edge_ids)
            """),
            {"edge_ids": belief_state.edge_ids}
        )

        for row in result:
            edge_type, attrs = row
            if "FAA_DISRUPTION" in edge_type:
                signals["faa"] = attrs
            elif "WEATHER_RISK" in edge_type:
                signals["weather"] = attrs
            elif "MOVEMENT_COLLAPSE" in edge_type:
                signals["movement"] = attrs
            elif "NWS_ALERT" in edge_type:
                signals["alerts"].append(attrs)

        return signals

    def _build_assessment_context(
        self,
        signals: Dict[str, Any],
        belief_state: BeliefState
    ) -> str:
        """Build context string for LLM assessment."""
        parts = []

        # Airport info
        airport_icao = belief_state.airport_icao or 'Unknown'
        parts.append(f"## Airport: {airport_icao}")

        # Get cascade impact from context graph
        cascade = self._get_cascade_impact(airport_icao)
        parts.append("")

        # FAA Status
        parts.append("## FAA NAS Status")
        if signals["faa"]:
            faa = signals["faa"]
            parts.append(f"- Delay: {faa.get('delay', False)}")
            parts.append(f"- Delay Type: {faa.get('delay_type', 'None')}")
            parts.append(f"- Reason: {faa.get('reason', 'Not specified')}")
            parts.append(f"- Average Delay: {faa.get('avg_delay_minutes', 0)} minutes")
            parts.append(f"- Closure: {faa.get('closure', False)}")
        else:
            parts.append("- No FAA disruption data available")
        parts.append("")

        # Weather
        parts.append("## Current Weather (METAR)")
        if signals["weather"]:
            wx = signals["weather"]
            parts.append(f"- Wind: {wx.get('wind_speed', 'N/A')} kts, gusts {wx.get('wind_gust', 'None')}")
            parts.append(f"- Visibility: {wx.get('visibility_miles', 'N/A')} miles")
            parts.append(f"- Ceiling: {wx.get('ceiling_feet', 'N/A')} feet")
            parts.append(f"- Conditions: {wx.get('conditions', 'N/A')}")
            if wx.get("severity"):
                parts.append(f"- Derived Severity: {wx.get('severity')}")
        else:
            parts.append("- No weather data available")
        parts.append("")

        # NWS Alerts
        parts.append("## NWS Weather Alerts")
        if signals["alerts"]:
            for i, alert in enumerate(signals["alerts"], 1):
                parts.append(f"Alert {i}:")
                parts.append(f"  - Event: {alert.get('event', 'Unknown')}")
                parts.append(f"  - Severity: {alert.get('severity', 'Unknown')}")
                parts.append(f"  - Urgency: {alert.get('urgency', 'Unknown')}")
        else:
            parts.append("- No active weather alerts")
        parts.append("")

        # Movement Data
        parts.append("## Aircraft Movement (OpenSky)")
        if signals["movement"]:
            mv = signals["movement"]
            parts.append(f"- Aircraft Count: {mv.get('aircraft_count', 'N/A')}")
            parts.append(f"- Movement Change: {mv.get('delta_percent', 'N/A')}%")
            if mv.get("severity"):
                parts.append(f"- Derived Severity: {mv.get('severity')}")
        else:
            parts.append("- No movement data available (OpenSky may have timed out)")
        parts.append("")

        # Uncertainties
        parts.append("## Uncertainties (Missing Information)")
        if belief_state.uncertainties:
            for u in belief_state.uncertainties:
                parts.append(f"- {u.question}")
        else:
            parts.append("- No significant uncertainties")
        parts.append("")

        # Contradictions
        parts.append("## Contradictions (Conflicting Information)")
        if belief_state.contradictions:
            for c in belief_state.contradictions:
                parts.append(f"- {c.contradiction_type}: {c.why_it_matters}")
        else:
            parts.append("- No contradictions detected")
        parts.append("")

        parts.append(f"## Evidence Count: {belief_state.evidence_count}")
        parts.append("")

        # Cascade Impact (forwarder operational exposure)
        parts.append("## Supply Chain Cascade Impact (Forwarder Exposure)")
        if cascade:
            parts.append(f"- Affected Flights: {len(cascade.get('affected_flights', []))}")
            parts.append(f"- Affected Shipments: {cascade.get('total_shipments', 0)}")
            parts.append(f"- Affected Bookings: {cascade.get('total_bookings', 0)}")
            # Realistic forwarder metrics
            parts.append(f"- Total Revenue at Risk: ${cascade.get('total_revenue_at_risk', 0):,.2f}")
            parts.append(f"- Total Weight: {cascade.get('total_weight_kg', 0):,.1f} kg")
            parts.append(f"- SLAs at Risk (48h window): {cascade.get('sla_at_risk_count', 0)}")
            parts.append(f"  - PREMIUM SLAs: {cascade.get('premium_sla_at_risk', 0)}")
            parts.append(f"  - EXPRESS SLAs: {cascade.get('express_sla_at_risk', 0)}")
            if cascade.get('affected_flights'):
                parts.append("- Top Affected Flights:")
                for flight in cascade['affected_flights'][:5]:
                    parts.append(f"  - {flight.get('flight_number', 'Unknown')}: {flight.get('origin', '?')} → {flight.get('destination', '?')}")
        else:
            parts.append("- No supply chain data available (enterprise data not seeded)")
        parts.append("")

        parts.append("## Required Response Format")
        parts.append(RISK_ASSESSMENT_RESPONSE_FORMAT)

        return "\n".join(parts)

    def _get_llm_assessment(self, context: str) -> Dict[str, Any]:
        """Get risk assessment from LLM."""
        messages = [
            {"role": "user", "content": context}
        ]

        try:
            response = self.llm.complete_json(
                system=RISK_ASSESSMENT_SYSTEM_PROMPT,
                messages=messages,
                temperature=0.0,  # Deterministic
            )
        except Exception as e:
            # Fail-closed: if the LLM is unavailable or returns invalid JSON,
            # default to human-in-the-loop posture rather than crashing the run.
            response = {
                "reasoning": f"LLM risk assessment unavailable ({str(e)}). Failing closed.",
                "signal_analysis": {},
                "risk_factors": ["LLM_UNAVAILABLE"],
                "mitigating_factors": [],
                "uncertainty_impact": "LLM assessment unavailable; escalating to manual review.",
                "overall_severity": 0.8,
                "risk_level": "HIGH",
                "recommended_posture": "ESCALATE",
                "confidence": 0.25,
                "rationale": "LLM unavailable; escalate to duty manager for review.",
            }

        # Validate and normalize response
        return self._normalize_assessment(response)

    def _normalize_assessment(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize LLM response to expected format."""
        # Ensure required fields exist
        normalized = {
            "reasoning": response.get("reasoning", ""),
            "signal_analysis": response.get("signal_analysis", {}),
            "risk_factors": response.get("risk_factors", []),
            "mitigating_factors": response.get("mitigating_factors", []),
            "uncertainty_impact": response.get("uncertainty_impact", ""),
            "overall_severity": float(response.get("overall_severity", 0.5)),
            "risk_level": response.get("risk_level", "MEDIUM"),
            "recommended_posture": response.get("recommended_posture", "HOLD"),
            "confidence": float(response.get("confidence", 0.5)),
            "rationale": response.get("rationale", ""),
            "component_scores": response.get("signal_analysis", {}),  # For backward compat
        }

        # Validate posture
        valid_postures = {"ACCEPT", "RESTRICT", "HOLD", "ESCALATE"}
        if normalized["recommended_posture"] not in valid_postures:
            normalized["recommended_posture"] = "HOLD"

        # Validate risk level
        valid_levels = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        if normalized["risk_level"] not in valid_levels:
            normalized["risk_level"] = "MEDIUM"

        # Clamp severity and confidence
        normalized["overall_severity"] = max(0.0, min(1.0, normalized["overall_severity"]))
        normalized["confidence"] = max(0.0, min(1.0, normalized["confidence"]))

        return normalized

    def _calculate_confidence_with_explanation(
        self,
        signals: Dict[str, Any],
        belief_state: BeliefState,
    ) -> tuple:
        """
        Calculate confidence dynamically with detailed breakdown.

        Returns:
            tuple: (confidence_score, breakdown_dict)
        """
        breakdown = {
            "base": 0.30,
            "sources": {},
            "penalties": {},
            "boosts": {},
            "final": 0.0,
        }
        confidence = 0.30  # Base confidence

        # Check evidence sources and their status (success vs api_error)
        evidence_sources = self._get_evidence_sources()
        failed_sources = self._get_failed_evidence_sources()
        breakdown["available_sources"] = list(evidence_sources)
        if failed_sources:
            breakdown["failed_sources"] = list(failed_sources)

        # METAR - critical source
        if signals.get("weather"):
            confidence += 0.18
            breakdown["sources"]["METAR"] = "+18% (weather conditions)"
        elif "METAR" in evidence_sources and "METAR" not in failed_sources:
            confidence += 0.18
            breakdown["sources"]["METAR"] = "+18% (normal conditions)"
        elif "METAR" in failed_sources:
            confidence += 0.04  # Small credit for trying
            breakdown["sources"]["METAR"] = "+4% (API error - data unavailable)"
        else:
            breakdown["sources"]["METAR"] = "missing"

        # FAA_NAS - critical source
        faa_signal = signals.get("faa")
        if faa_signal:
            has_disruption = bool(
                faa_signal.get("has_disruption")
                or faa_signal.get("delay")
                or faa_signal.get("closure")
            )
            confidence += 0.18
            breakdown["sources"]["FAA_NAS"] = "+18% (disruption detected)" if has_disruption else "+18% (normal operations)"
        elif "FAA_NAS" in evidence_sources and "FAA_NAS" not in failed_sources:
            confidence += 0.18
            breakdown["sources"]["FAA_NAS"] = "+18% (normal operations)"
        elif "FAA_NAS" in failed_sources:
            confidence += 0.04
            breakdown["sources"]["FAA_NAS"] = "+4% (API error - data unavailable)"
        else:
            breakdown["sources"]["FAA_NAS"] = "missing"

        # OPENSKY - supplementary source
        if signals.get("movement"):
            confidence += 0.12
            breakdown["sources"]["OPENSKY"] = "+12% (aircraft movement)"
        elif "OPENSKY" in evidence_sources and "OPENSKY" not in failed_sources:
            confidence += 0.12
            breakdown["sources"]["OPENSKY"] = "+12% (movement data normal)"
        elif "OPENSKY" in failed_sources:
            confidence += 0.02  # Minimal credit - supplementary source
            breakdown["sources"]["OPENSKY"] = "+2% (API error - data unavailable)"
        else:
            breakdown["sources"]["OPENSKY"] = "missing"

        # NWS_ALERTS - supplementary source
        if signals.get("alerts") and len(signals.get("alerts", [])) > 0:
            confidence += 0.08
            breakdown["sources"]["NWS_ALERTS"] = "+8% (alerts active)"
        elif "NWS_ALERTS" in evidence_sources and "NWS_ALERTS" not in failed_sources:
            confidence += 0.08
            breakdown["sources"]["NWS_ALERTS"] = "+8% (no active alerts)"
        elif "NWS_ALERTS" in failed_sources:
            confidence += 0.02
            breakdown["sources"]["NWS_ALERTS"] = "+2% (API error - data unavailable)"
        else:
            breakdown["sources"]["NWS_ALERTS"] = "missing"

        # TAF - supplementary source
        if "TAF" in evidence_sources and "TAF" not in failed_sources:
            confidence += 0.06
            breakdown["sources"]["TAF"] = "+6% (forecast available)"
        elif "TAF" in failed_sources:
            confidence += 0.01
            breakdown["sources"]["TAF"] = "+1% (API error - data unavailable)"
        else:
            breakdown["sources"]["TAF"] = "missing"

        # Reduce for uncertainties
        if belief_state.uncertainty_count > 0:
            penalty = min(0.20, belief_state.uncertainty_count * 0.04)
            confidence -= penalty
            breakdown["penalties"]["uncertainties"] = f"-{int(penalty*100)}% ({belief_state.uncertainty_count} unresolved)"

        # Reduce for contradictions
        if belief_state.contradiction_count > 0:
            penalty = min(0.20, belief_state.contradiction_count * 0.10)
            confidence -= penalty
            breakdown["penalties"]["contradictions"] = f"-{int(penalty*100)}% ({belief_state.contradiction_count} detected)"

        # Small boost for more evidence
        if belief_state.evidence_count > 0:
            boost = min(0.05, belief_state.evidence_count * 0.01)
            confidence += boost
            breakdown["boosts"]["evidence_count"] = f"+{int(boost*100)}% ({belief_state.evidence_count} pieces)"

        final = max(0.25, min(0.95, confidence))
        breakdown["final"] = final
        breakdown["explanation"] = self._generate_confidence_explanation(breakdown)

        return final, breakdown

    def _generate_confidence_explanation(self, breakdown: Dict[str, Any]) -> str:
        """Generate human-readable confidence explanation."""
        parts = []

        # Count available sources
        available = [k for k, v in breakdown["sources"].items() if v != "missing"]
        missing = [k for k, v in breakdown["sources"].items() if v == "missing"]

        if available:
            parts.append(f"Data from {len(available)} sources: {', '.join(available)}")
        if missing:
            parts.append(f"Missing: {', '.join(missing)}")
        if breakdown.get("penalties"):
            penalty_parts = [f"{k}: {v}" for k, v in breakdown["penalties"].items()]
            parts.append(f"Penalties: {'; '.join(penalty_parts)}")

        return ". ".join(parts) if parts else "No explanation available"

    def _calculate_confidence(
        self,
        signals: Dict[str, Any],
        belief_state: BeliefState,
    ) -> float:
        """Calculate confidence (backward compatible wrapper)."""
        confidence, _ = self._calculate_confidence_with_explanation(signals, belief_state)
        return confidence

    def _get_evidence_sources(self) -> set:
        """Get all evidence source systems for this case."""
        result = self.session.execute(
            text("""
                SELECT DISTINCT e.source_system
                FROM evidence e
                JOIN trace_event t ON t.ref_id::text = e.id::text
                WHERE t.case_id = :case_id AND t.ref_type = 'evidence'
            """),
            {"case_id": self.case_id}
        )
        return {row[0] for row in result}

    def _get_failed_evidence_sources(self) -> set:
        """
        Get evidence sources that failed (api_error status).

        Checks the evidence excerpt for 'api_error' status indicator.
        """
        result = self.session.execute(
            text("""
                SELECT DISTINCT e.source_system
                FROM evidence e
                JOIN trace_event t ON t.ref_id::text = e.id::text
                WHERE t.case_id = :case_id
                  AND t.ref_type = 'evidence'
                  AND (e.excerpt LIKE '%"status": "api_error"%'
                       OR e.excerpt LIKE '%"status":"api_error"%')
            """),
            {"case_id": self.case_id}
        )
        return {row[0] for row in result}
