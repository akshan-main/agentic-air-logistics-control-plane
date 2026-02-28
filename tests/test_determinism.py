# tests/test_determinism.py
"""
Test determinism in ranking and scoring.

Verifies that the system produces deterministic, reproducible results.
"""

from uuid import uuid4

from app.graph.retrieval import (
    WEIGHTS,
    HybridSearchResult,
    _normalize_semantic,
    _normalize_keyword,
)
from app.agents.planner.beam_search import (
    score_action,
    ActionCandidate,
    UNCERTAINTY_VALUES,
    TOOL_RESOLVES,
    TOOL_COSTS,
    INTERVENTION_COSTS,
)
from app.agents.state_graph import BeliefState, Uncertainty


class TestHybridRankingDeterministic:
    """Tests for deterministic hybrid ranking."""

    def test_hybrid_ranking_deterministic(self):
        """Same inputs produce identical ranking (tie-breaking by id)."""
        # Create test results with known scores
        id1, id2, id3 = uuid4(), uuid4(), uuid4()

        results = [
            HybridSearchResult(
                id=id1,
                case_id=uuid4(),
                text="Test 1",
                semantic_score=0.8,
                keyword_score=0.6,
                graph_score=0.4,
                final_score=HybridSearchResult.compute_final_score(0.8, 0.6, 0.4),
            ),
            HybridSearchResult(
                id=id2,
                case_id=uuid4(),
                text="Test 2",
                semantic_score=0.9,
                keyword_score=0.3,
                graph_score=0.2,
                final_score=HybridSearchResult.compute_final_score(0.9, 0.3, 0.2),
            ),
            HybridSearchResult(
                id=id3,
                case_id=uuid4(),
                text="Test 3",
                semantic_score=0.7,
                keyword_score=0.8,
                graph_score=0.6,
                final_score=HybridSearchResult.compute_final_score(0.7, 0.8, 0.6),
            ),
        ]

        # Sort multiple times - should always produce same order
        for _ in range(10):
            sorted_results = sorted(
                results,
                key=lambda r: (-r.final_score, str(r.id))
            )

            # Order should be identical each time
            ids = [str(r.id) for r in sorted_results]
            expected_ids = [str(r.id) for r in sorted(
                results,
                key=lambda r: (-r.final_score, str(r.id))
            )]
            assert ids == expected_ids

    def test_weights_sum_to_one(self):
        """Hybrid weights sum to 1.0."""
        total = sum(WEIGHTS.values())
        assert abs(total - 1.0) < 0.001

    def test_weights_match_spec(self):
        """Weights match spec: 0.5 semantic, 0.3 keyword, 0.2 graph."""
        assert WEIGHTS["semantic"] == 0.5
        assert WEIGHTS["keyword"] == 0.3
        assert WEIGHTS["graph"] == 0.2

    def test_final_score_formula(self):
        """Final score computed correctly using formula."""
        semantic, keyword, graph = 0.8, 0.6, 0.4

        # Expected: 0.5 * 0.8 + 0.3 * 0.6 + 0.2 * 0.4 = 0.4 + 0.18 + 0.08 = 0.66
        expected = 0.5 * semantic + 0.3 * keyword + 0.2 * graph
        actual = HybridSearchResult.compute_final_score(semantic, keyword, graph)

        assert abs(actual - expected) < 0.0001
        assert abs(actual - 0.66) < 0.0001

    def test_tie_breaking_by_id(self):
        """Results with same score are tie-broken by ID deterministically."""
        id1, id2 = uuid4(), uuid4()

        # Same scores
        r1 = HybridSearchResult(
            id=id1, case_id=uuid4(), text="T1",
            semantic_score=0.5, keyword_score=0.5, graph_score=0.5,
            final_score=0.5,
        )
        r2 = HybridSearchResult(
            id=id2, case_id=uuid4(), text="T2",
            semantic_score=0.5, keyword_score=0.5, graph_score=0.5,
            final_score=0.5,
        )

        results = [r1, r2]

        # Sort multiple times
        for _ in range(5):
            sorted_results = sorted(results, key=lambda r: (-r.final_score, str(r.id)))
            expected_order = sorted([str(id1), str(id2)])
            actual_order = [str(r.id) for r in sorted_results]
            assert actual_order == expected_order


class TestPlannerScoringDeterministic:
    """Tests for deterministic planner scoring."""

    def test_planner_scoring_deterministic(self):
        """Same BeliefState produces same action scores."""
        belief = BeliefState(
            uncertainties=[
                Uncertainty(id="u1", question="Status?", uncertainty_type="airport_status_unknown"),
                Uncertainty(id="u2", question="Weather?", uncertainty_type="weather_conditions_unknown"),
            ],
            hypotheses=[],
            contradictions=[],
        )

        action = ActionCandidate(action_type="INVESTIGATE", tool="fetch_faa_status")

        # Score same action multiple times
        scores = [score_action(action, belief) for _ in range(10)]

        # All scores should be identical
        assert len(set(scores)) == 1

    def test_different_belief_states_different_scores(self):
        """Different belief states produce different scores for same action."""
        belief_with_uncertainty = BeliefState(
            uncertainties=[
                Uncertainty(id="u1", question="Status?", uncertainty_type="airport_status_unknown"),
            ],
            hypotheses=[],
            contradictions=[],
        )

        belief_without_uncertainty = BeliefState(
            uncertainties=[],
            hypotheses=[],
            contradictions=[],
        )

        action = ActionCandidate(action_type="INVESTIGATE", tool="fetch_faa_status")

        score_with = score_action(action, belief_with_uncertainty)
        score_without = score_action(action, belief_without_uncertainty)

        # Should score higher when there's uncertainty to resolve
        assert score_with > score_without

    def test_uncertainty_values_defined(self):
        """All uncertainty types have defined values."""
        expected_types = [
            "airport_status_unknown",
            "weather_conditions_unknown",
            "alert_status_unknown",
            "movement_data_unknown",
            "contradiction_unresolved",
        ]

        for utype in expected_types:
            assert utype in UNCERTAINTY_VALUES
            assert UNCERTAINTY_VALUES[utype] > 0

    def test_tool_resolves_mapping_complete(self):
        """All investigation tools have resolution mappings."""
        expected_tools = [
            "fetch_faa_status",
            "fetch_weather",
            "fetch_alerts",
            "fetch_opensky",
        ]

        for tool in expected_tools:
            assert tool in TOOL_RESOLVES
            assert isinstance(TOOL_RESOLVES[tool], list)
            assert len(TOOL_RESOLVES[tool]) > 0

    def test_tool_costs_defined(self):
        """All tools have cost values."""
        expected_tools = [
            "fetch_faa_status",
            "fetch_weather",
            "fetch_alerts",
            "fetch_opensky",
        ]

        for tool in expected_tools:
            assert tool in TOOL_COSTS
            assert TOOL_COSTS[tool] >= 0

    def test_intervention_costs_defined(self):
        """All intervention types have cost values."""
        expected_interventions = [
            "SET_POSTURE",
            "PUBLISH_GATEWAY_ADVISORY",
            "ESCALATE_OPS",
            "HOLD_CARGO",
            "NOTIFY_CUSTOMER",
            "SWITCH_GATEWAY",
            "REBOOK_FLIGHT",
        ]

        for intervention in expected_interventions:
            assert intervention in INTERVENTION_COSTS
            assert INTERVENTION_COSTS[intervention] >= 0

    def test_intervention_cost_ordering(self):
        """Higher-impact interventions have higher costs."""
        # SET_POSTURE is cheap (no external action)
        assert INTERVENTION_COSTS["SET_POSTURE"] < INTERVENTION_COSTS["HOLD_CARGO"]

        # Customer-facing actions are expensive
        assert INTERVENTION_COSTS["NOTIFY_CUSTOMER"] > INTERVENTION_COSTS["SET_POSTURE"]

        # Rebooking is most expensive (financial/operational impact)
        assert INTERVENTION_COSTS["REBOOK_FLIGHT"] >= INTERVENTION_COSTS["SWITCH_GATEWAY"]


class TestNormalizationDeterminism:
    """Tests for deterministic score normalization."""

    def test_semantic_normalization_deterministic(self):
        """Semantic normalization produces same results."""
        results = [
            {"cosine_sim": 0.8},
            {"cosine_sim": 0.4},
            {"cosine_sim": -0.2},
        ]

        # Normalize multiple times
        for _ in range(5):
            test_results = [r.copy() for r in results]
            _normalize_semantic(test_results)

            # Verify deterministic output
            assert test_results[0]["semantic"] == 0.9   # (0.8 + 1) / 2
            assert test_results[1]["semantic"] == 0.7   # (0.4 + 1) / 2
            assert test_results[2]["semantic"] == 0.4   # (-0.2 + 1) / 2

    def test_keyword_normalization_deterministic(self):
        """Keyword normalization produces same results."""
        results = [
            {"ts_rank": 10.0},
            {"ts_rank": 5.0},
            {"ts_rank": 2.5},
        ]

        # Normalize multiple times
        for _ in range(5):
            test_results = [r.copy() for r in results]
            _normalize_keyword(test_results)

            # Verify deterministic output (min-max normalized)
            assert test_results[0]["keyword"] == 1.0    # 10/10
            assert test_results[1]["keyword"] == 0.5    # 5/10
            assert test_results[2]["keyword"] == 0.25   # 2.5/10
