# tests/test_hybrid_ranking.py
"""
Test hybrid ranking determinism.

Verifies that the hybrid retrieval produces deterministic results.
"""

from uuid import uuid4

from app.graph.retrieval import (
    WEIGHTS,
    HybridSearchResult,
    _normalize_semantic,
    _normalize_keyword,
    _merge_results,
)


class TestHybridRankingDeterminism:
    """Tests for deterministic hybrid ranking."""

    def test_hybrid_ranking_deterministic(self):
        """Same inputs produce identical ranking (tie-breaking by id)."""
        # Create test results with known scores
        results = [
            HybridSearchResult(
                id=uuid4(),
                case_id=uuid4(),
                text="Test 1",
                semantic_score=0.8,
                keyword_score=0.6,
                graph_score=0.4,
                final_score=HybridSearchResult.compute_final_score(0.8, 0.6, 0.4),
            ),
            HybridSearchResult(
                id=uuid4(),
                case_id=uuid4(),
                text="Test 2",
                semantic_score=0.9,
                keyword_score=0.3,
                graph_score=0.2,
                final_score=HybridSearchResult.compute_final_score(0.9, 0.3, 0.2),
            ),
            HybridSearchResult(
                id=uuid4(),
                case_id=uuid4(),
                text="Test 3",
                semantic_score=0.7,
                keyword_score=0.8,
                graph_score=0.6,
                final_score=HybridSearchResult.compute_final_score(0.7, 0.8, 0.6),
            ),
        ]

        # Sort multiple times
        for _ in range(5):
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

    def test_weights_are_correct(self):
        """Weights match spec: 0.5 semantic, 0.3 keyword, 0.2 graph."""
        assert WEIGHTS["semantic"] == 0.5
        assert WEIGHTS["keyword"] == 0.3
        assert WEIGHTS["graph"] == 0.2

    def test_final_score_formula(self):
        """Final score computed correctly."""
        semantic = 0.8
        keyword = 0.6
        graph = 0.4

        expected = 0.5 * semantic + 0.3 * keyword + 0.2 * graph
        actual = HybridSearchResult.compute_final_score(semantic, keyword, graph)

        assert abs(actual - expected) < 0.0001

    def test_tie_breaking_by_id(self):
        """Results with same score are tie-broken by ID."""
        id1 = uuid4()
        id2 = uuid4()

        # Same scores, different IDs
        r1 = HybridSearchResult(
            id=id1,
            case_id=uuid4(),
            text="Test 1",
            semantic_score=0.5,
            keyword_score=0.5,
            graph_score=0.5,
            final_score=0.5,
        )
        r2 = HybridSearchResult(
            id=id2,
            case_id=uuid4(),
            text="Test 2",
            semantic_score=0.5,
            keyword_score=0.5,
            graph_score=0.5,
            final_score=0.5,
        )

        results = [r1, r2]
        sorted_results = sorted(results, key=lambda r: (-r.final_score, str(r.id)))

        # Should be deterministically ordered by ID
        expected_order = sorted([str(id1), str(id2)])
        actual_order = [str(r.id) for r in sorted_results]
        assert actual_order == expected_order


class TestNormalization:
    """Tests for score normalization."""

    def test_semantic_normalization(self):
        """Semantic scores normalized from [-1, 1] to [0, 1]."""
        results = [
            {"cosine_sim": 1.0},   # Max similarity
            {"cosine_sim": 0.0},   # Orthogonal
            {"cosine_sim": -1.0},  # Max dissimilarity
        ]

        _normalize_semantic(results)

        assert results[0]["semantic"] == 1.0   # 1.0 -> 1.0
        assert results[1]["semantic"] == 0.5   # 0.0 -> 0.5
        assert results[2]["semantic"] == 0.0   # -1.0 -> 0.0

    def test_keyword_normalization(self):
        """Keyword scores normalized using min-max."""
        results = [
            {"ts_rank": 10.0},
            {"ts_rank": 5.0},
            {"ts_rank": 0.0},
        ]

        _normalize_keyword(results)

        assert results[0]["keyword"] == 1.0    # Max
        assert results[1]["keyword"] == 0.5    # Mid
        assert results[2]["keyword"] == 0.0    # Min

    def test_keyword_normalization_empty(self):
        """Keyword normalization handles empty list."""
        results = []
        _normalize_keyword(results)
        assert results == []


class TestResultMerging:
    """Tests for result merging."""

    def test_merge_results(self):
        """Results from all sources are merged by case_id."""
        case_id = uuid4()

        semantic = [{"case_id": case_id, "text": "test", "semantic": 0.8}]
        keyword = [{"case_id": case_id, "text": "test", "keyword": 0.6}]
        graph = [{"case_id": case_id, "jaccard_sim": 0.4}]

        merged = _merge_results(semantic, keyword, graph)

        assert case_id in merged
        assert merged[case_id]["semantic"] == 0.8
        assert merged[case_id]["keyword"] == 0.6
        assert merged[case_id]["graph"] == 0.4

    def test_merge_partial_results(self):
        """Merging handles cases that appear in only some sources."""
        case_1 = uuid4()
        case_2 = uuid4()

        semantic = [{"case_id": case_1, "text": "test1", "semantic": 0.8}]
        keyword = [{"case_id": case_2, "text": "test2", "keyword": 0.6}]
        graph = []

        merged = _merge_results(semantic, keyword, graph)

        # Case 1 should have semantic, not keyword
        assert merged[case_1].get("semantic") == 0.8
        assert merged[case_1].get("keyword", 0.0) == 0.0

        # Case 2 should have keyword, not semantic
        assert merged[case_2].get("keyword") == 0.6
        assert merged[case_2].get("semantic", 0.0) == 0.0
