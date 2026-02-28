# tests/test_playbook_aging.py
"""
Test precedent aging for playbook relevance decay.

All tests are non-DB (pure function tests) and test the aging module directly.
"""

import pytest
from datetime import datetime, timezone, timedelta

from app.replay.aging import (
    compute_decay_factor,
    compute_policy_alignment,
    compute_aged_score,
    sample_confidence,
    policy_text_hash,
    build_policy_snapshot,
    infer_domain_from_pattern,
    HALF_LIVES,
    DEFAULT_HALF_LIFE,
)


# ---------------------------------------------------------------------------
# Decay Factor Tests
# ---------------------------------------------------------------------------

class TestDecayFactor:
    """Tests for exponential decay computation."""

    def test_brand_new_playbook_has_no_decay(self):
        now = datetime.now(timezone.utc)
        factor = compute_decay_factor(now, now, "operational", now=now)
        assert abs(factor - 1.0) < 0.001

    def test_half_life_gives_50_percent(self):
        now = datetime.now(timezone.utc)
        created = now - timedelta(days=90)  # operational half-life
        factor = compute_decay_factor(created, None, "operational", now=now)
        assert abs(factor - 0.5) < 0.01

    def test_weather_decays_faster_than_operational(self):
        now = datetime.now(timezone.utc)
        created = now - timedelta(days=45)

        weather_factor = compute_decay_factor(created, None, "weather", now=now)
        ops_factor = compute_decay_factor(created, None, "operational", now=now)

        assert weather_factor < ops_factor

    def test_customs_decays_slower_than_operational(self):
        now = datetime.now(timezone.utc)
        created = now - timedelta(days=120)

        customs_factor = compute_decay_factor(created, None, "customs", now=now)
        ops_factor = compute_decay_factor(created, None, "operational", now=now)

        assert customs_factor > ops_factor

    def test_last_used_refreshes_decay(self):
        now = datetime.now(timezone.utc)
        created = now - timedelta(days=180)  # Old creation
        last_used = now - timedelta(days=5)  # Recently used

        factor = compute_decay_factor(created, last_used, "operational", now=now)
        assert factor > 0.9  # Should be nearly fresh

    def test_never_used_decays_from_creation(self):
        now = datetime.now(timezone.utc)
        created = now - timedelta(days=180)

        factor = compute_decay_factor(created, None, "operational", now=now)
        # Two half-lives: 0.5^2 = 0.25
        assert abs(factor - 0.25) < 0.05

    def test_decay_is_deterministic(self):
        now = datetime(2025, 6, 1, tzinfo=timezone.utc)
        created = datetime(2025, 3, 1, tzinfo=timezone.utc)

        factors = [
            compute_decay_factor(created, None, "operational", now=now)
            for _ in range(10)
        ]
        assert len(set(factors)) == 1

    def test_unknown_domain_uses_default(self):
        now = datetime.now(timezone.utc)
        created = now - timedelta(days=DEFAULT_HALF_LIFE)

        factor = compute_decay_factor(created, None, "unknown_domain", now=now)
        assert abs(factor - 0.5) < 0.01

    def test_naive_datetime_handled(self):
        """Naive datetimes (no tzinfo) should not crash."""
        now = datetime(2025, 6, 1)
        created = datetime(2025, 3, 1)

        factor = compute_decay_factor(created, None, "operational", now=now)
        assert 0.0 < factor < 1.0

    def test_zero_age_returns_one(self):
        now = datetime(2025, 6, 1, tzinfo=timezone.utc)
        factor = compute_decay_factor(now, None, "operational", now=now)
        assert factor == 1.0

    def test_half_lives_are_reasonable(self):
        assert HALF_LIVES["weather"] == 30.0
        assert HALF_LIVES["operational"] == 90.0
        assert HALF_LIVES["customs"] == 180.0


# ---------------------------------------------------------------------------
# Policy Alignment Tests
# ---------------------------------------------------------------------------

class TestPolicyAlignment:
    """Tests for policy alignment scoring."""

    def test_identical_snapshots_are_aligned(self):
        snapshot = ["abc123def456", "ghi789jkl012", "mno345pqr678"]
        assert compute_policy_alignment(snapshot, snapshot) == 1.0

    def test_completely_different_snapshots(self):
        old = ["abc123def456", "ghi789jkl012"]
        new = ["xyz789uvw012", "rst345opq678"]
        assert compute_policy_alignment(old, new) == 0.0

    def test_partial_overlap(self):
        old = ["abc123def456", "ghi789jkl012", "mno345pqr678"]
        new = ["abc123def456", "ghi789jkl012", "new001new001"]
        # Jaccard: intersection=2, union=4 -> 0.5
        assert abs(compute_policy_alignment(old, new) - 0.5) < 0.01

    def test_empty_playbook_snapshot_gets_benefit_of_doubt(self):
        assert compute_policy_alignment([], ["abc123def456"]) == 0.5

    def test_both_empty_is_aligned(self):
        assert compute_policy_alignment([], []) == 1.0

    def test_superset_current_policies(self):
        """New policies added but old ones still active."""
        old = ["abc123def456", "ghi789jkl012"]
        new = ["abc123def456", "ghi789jkl012", "new001new001"]
        # Jaccard: intersection=2, union=3 -> 0.667
        result = compute_policy_alignment(old, new)
        assert 0.6 < result < 0.7

    def test_subset_current_policies(self):
        """Some old policies removed."""
        old = ["abc123def456", "ghi789jkl012", "mno345pqr678"]
        new = ["abc123def456"]
        # Jaccard: intersection=1, union=3 -> 0.333
        result = compute_policy_alignment(old, new)
        assert 0.3 < result < 0.4


# ---------------------------------------------------------------------------
# Aged Score Tests
# ---------------------------------------------------------------------------

class TestSampleConfidence:
    """Tests for sample-size confidence factor."""

    def test_zero_uses_gives_zero(self):
        assert sample_confidence(0) == 0.0

    def test_one_use_gives_low_confidence(self):
        assert sample_confidence(1) == pytest.approx(0.2)

    def test_five_uses_gives_full_confidence(self):
        assert sample_confidence(5) == 1.0

    def test_many_uses_capped_at_one(self):
        assert sample_confidence(100) == 1.0

    def test_three_uses(self):
        assert sample_confidence(3) == pytest.approx(0.6)


class TestAgedScore:
    """Tests for composite aged score."""

    def test_all_perfect_with_high_samples(self):
        # 10 uses -> confidence=1.0, so result = 0.8 * 1.0 * 1.0 * 1.0
        assert compute_aged_score(0.8, 1.0, 1.0, use_count=10) == pytest.approx(0.8)

    def test_zero_decay_gives_zero(self):
        assert compute_aged_score(0.8, 0.0, 1.0, use_count=10) == 0.0

    def test_zero_alignment_gives_zero(self):
        assert compute_aged_score(0.8, 1.0, 0.0, use_count=10) == 0.0

    def test_zero_success_gives_zero(self):
        assert compute_aged_score(0.0, 1.0, 1.0, use_count=10) == 0.0

    def test_low_sample_reduces_score(self):
        # 1 use -> confidence=0.2
        high = compute_aged_score(1.0, 1.0, 1.0, use_count=10)
        low = compute_aged_score(1.0, 1.0, 1.0, use_count=1)
        assert low < high
        assert low == pytest.approx(0.2)

    def test_composite_multiplication(self):
        result = compute_aged_score(0.9, 0.5, 0.8, use_count=5)
        expected = 0.9 * 0.5 * 0.8 * 1.0  # confidence=1.0 at 5 uses
        assert abs(result - expected) < 0.001


# ---------------------------------------------------------------------------
# Policy Snapshot Utilities Tests
# ---------------------------------------------------------------------------

class TestPolicySnapshot:
    """Tests for policy snapshot utilities."""

    def test_policy_text_hash_deterministic(self):
        h1 = policy_text_hash("Some policy text about evidence requirements")
        h2 = policy_text_hash("Some policy text about evidence requirements")
        assert h1 == h2

    def test_policy_text_hash_12_chars(self):
        h = policy_text_hash("Any policy text")
        assert len(h) == 12
        assert all(c in "0123456789abcdef" for c in h)

    def test_different_text_different_hash(self):
        h1 = policy_text_hash("Policy A: require evidence")
        h2 = policy_text_hash("Policy B: require approval")
        assert h1 != h2

    def test_build_policy_snapshot_sorted(self):
        policies = [
            {"text": "Zzz policy about weather"},
            {"text": "Aaa policy about evidence"},
            {"text": "Mmm policy about approvals"},
        ]
        snapshot = build_policy_snapshot(policies)
        assert snapshot == sorted(snapshot)
        assert len(snapshot) == 3

    def test_build_policy_snapshot_skips_empty_text(self):
        policies = [
            {"text": "Real policy"},
            {"text": ""},
            {"other_field": "no text key"},
        ]
        snapshot = build_policy_snapshot(policies)
        assert len(snapshot) == 1


# ---------------------------------------------------------------------------
# Domain Inference Tests
# ---------------------------------------------------------------------------

class TestDomainInference:
    """Tests for domain inference from playbook pattern."""

    def test_weather_only_sources(self):
        pattern = {
            "case_type": "AIRPORT_DISRUPTION",
            "evidence_sources": ["METAR", "TAF"],
        }
        assert infer_domain_from_pattern(pattern) == "weather"

    def test_all_weather_sources(self):
        pattern = {
            "evidence_sources": ["METAR", "TAF", "NWS_ALERTS"],
        }
        assert infer_domain_from_pattern(pattern) == "weather"

    def test_mixed_sources_is_operational(self):
        pattern = {
            "case_type": "AIRPORT_DISRUPTION",
            "evidence_sources": ["METAR", "FAA_NAS", "OPENSKY"],
        }
        assert infer_domain_from_pattern(pattern) == "operational"

    def test_no_sources_is_operational(self):
        pattern = {"case_type": "AIRPORT_DISRUPTION"}
        assert infer_domain_from_pattern(pattern) == "operational"

    def test_empty_pattern_is_operational(self):
        assert infer_domain_from_pattern({}) == "operational"

    def test_empty_sources_list_is_operational(self):
        pattern = {"evidence_sources": []}
        assert infer_domain_from_pattern(pattern) == "operational"

    def test_customs_case_type(self):
        pattern = {"case_type": "CUSTOMS_HOLD", "evidence_sources": ["FAA_NAS"]}
        assert infer_domain_from_pattern(pattern) == "customs"

    def test_import_case_type(self):
        pattern = {"case_type": "IMPORT_DELAY"}
        assert infer_domain_from_pattern(pattern) == "customs"

    def test_export_case_type(self):
        pattern = {"case_type": "EXPORT_COMPLIANCE"}
        assert infer_domain_from_pattern(pattern) == "customs"

    def test_customs_case_type_case_insensitive(self):
        pattern = {"case_type": "customs_review"}
        assert infer_domain_from_pattern(pattern) == "customs"
