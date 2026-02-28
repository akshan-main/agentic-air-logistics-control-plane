# app/replay/aging.py
"""
Precedent aging for playbook relevance decay.

Playbook precedents lose relevance over time via exponential decay.
Different domains decay at different rates:
- Weather: 30-day half-life (weather patterns change rapidly)
- Operational: 90-day half-life (standard operational playbooks)
- Customs/compliance: 180-day half-life (regulatory changes are slow)

Policy changes can also invalidate old precedents. When policies
are added, removed, or sunset, playbooks created under the old
policy set get a reduced alignment score.

All functions in this module are pure (no DB access) for testability.
"""

import hashlib
import math
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional


# Domain-specific half-lives in days
HALF_LIVES: Dict[str, float] = {
    "weather": 30.0,
    "operational": 90.0,
    "customs": 180.0,
}

DEFAULT_HALF_LIFE = 90.0  # days


def compute_decay_factor(
    created_at: datetime,
    last_used_at: Optional[datetime],
    domain: str = "operational",
    now: Optional[datetime] = None,
) -> float:
    """
    Compute exponential decay factor for a playbook.

    Uses the MORE RECENT of created_at and last_used_at as the
    reference point. A playbook that was used recently is still
    relevant even if it was created long ago.

    Formula: decay = 0.5 ^ (age_days / half_life_days)

    Returns a value in (0, 1] where 1.0 means "brand new".
    """
    if now is None:
        now = datetime.now(timezone.utc)

    reference = last_used_at or created_at
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    age = now - reference
    age_days = max(age.total_seconds() / 86400.0, 0.0)

    half_life = HALF_LIVES.get(domain, DEFAULT_HALF_LIFE)

    return math.pow(0.5, age_days / half_life)


def compute_policy_alignment(
    playbook_snapshot: List[str],
    current_snapshot: List[str],
) -> float:
    """
    Compute policy alignment score between playbook creation-time
    snapshot and current active policy set.

    Uses Jaccard similarity of policy text hash sets.

    Returns:
        1.0 = policies identical (full alignment)
        0.5 = legacy playbook with no snapshot (benefit of the doubt)
        0.0 = no overlap (completely stale)
    """
    if not playbook_snapshot and not current_snapshot:
        return 1.0  # Both empty = aligned

    if not playbook_snapshot:
        return 0.5  # Legacy playbook with no snapshot

    pb_set = set(playbook_snapshot)
    cur_set = set(current_snapshot)

    intersection = len(pb_set & cur_set)
    union = len(pb_set | cur_set)

    return intersection / union if union > 0 else 0.0


def sample_confidence(use_count: int, min_samples: int = 5) -> float:
    """
    Compute confidence factor based on sample size.

    Uses a simple ramp: confidence = min(use_count / min_samples, 1.0).
    A playbook with 1 use gets 0.2 confidence; 5+ uses gets 1.0.
    This prevents a 1/1 playbook from ranking equally to a 200/200 one.
    """
    if use_count <= 0:
        return 0.0
    return min(use_count / min_samples, 1.0)


def compute_aged_score(
    success_rate: float,
    decay_factor: float,
    policy_alignment: float,
    use_count: int = 5,
) -> float:
    """
    Compute the final aged relevance score.

    Formula: success_rate * decay_factor * policy_alignment * confidence

    All components are in [0, 1], so the result is in [0, 1].
    The confidence factor prevents low-sample playbooks from ranking
    equally to well-tested ones.
    """
    confidence = sample_confidence(use_count)
    return success_rate * decay_factor * policy_alignment * confidence


def policy_text_hash(text: str) -> str:
    """
    Compute a short hash of a policy text for snapshot comparison.

    Uses first 12 hex chars of SHA-256. Collision risk is negligible
    for < 100 policies.
    """
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def build_policy_snapshot(policies: List[Dict[str, Any]]) -> List[str]:
    """
    Build a sorted list of policy text hashes from active policies.

    Args:
        policies: List of policy dicts with "text" key

    Returns:
        Sorted list of 12-char hex hashes
    """
    hashes = [policy_text_hash(p["text"]) for p in policies if p.get("text")]
    return sorted(hashes)


def infer_domain_from_pattern(pattern: Dict[str, Any]) -> str:
    """
    Infer the domain category from a playbook's pattern.

    Heuristic based on case_type and evidence sources:
    - customs/import/export case types -> 'customs' (180-day half-life)
    - weather-only evidence sources -> 'weather' (30-day half-life)
    - Default -> 'operational' (90-day half-life)
    """
    case_type = (pattern.get("case_type") or "").upper()
    if "CUSTOMS" in case_type or "IMPORT" in case_type or "EXPORT" in case_type:
        return "customs"

    evidence_sources = pattern.get("evidence_sources") or []
    if not evidence_sources:
        return "operational"

    weather_sources = {"METAR", "TAF", "NWS_ALERTS"}
    if set(evidence_sources).issubset(weather_sources):
        return "weather"

    return "operational"
