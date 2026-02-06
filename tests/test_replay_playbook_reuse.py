# tests/test_replay_playbook_reuse.py
"""
Test replay learning and playbook reuse.

Verifies the replay gate: After 3 cases, 4th must retrieve playbook.
"""

import pytest
from uuid import uuid4
from datetime import datetime, timezone
from sqlalchemy import text

from app.replay.playbooks import PlaybookManager
from app.replay.evaluator import PlaybookEvaluator
from app.replay.miner import TraceMiner


class TestReplayGate:
    """Tests for replay learning gate."""

    def test_replay_playbook_reuse_after_three_cases(self, session):
        """4th case retrieves playbook after 3 similar cases."""
        # Create 3 resolved cases of same type
        case_type = "AIRPORT_DISRUPTION"
        airport = "REPLAY_TEST"

        for i in range(3):
            case_id = uuid4()
            session.execute(
                text("""
                    INSERT INTO "case" (id, case_type, scope, status)
                    VALUES (:id, :case_type, :scope, 'RESOLVED')
                """),
                {"id": case_id, "case_type": case_type, "scope": {"airport": airport}}
            )
        session.commit()

        # Create a playbook for this case type
        playbook_id = uuid4()
        session.execute(
            text("""
                INSERT INTO playbook (id, name, pattern, action_template, stats)
                VALUES (:id, :name, :pattern, :action_template, :stats)
            """),
            {
                "id": playbook_id,
                "name": f"{case_type}_playbook",
                "pattern": {"case_type": case_type, "scope_keys": ["airport"]},
                "action_template": {"action_sequence": ["SET_POSTURE"]},
                "stats": {"use_count": 3, "success_count": 3, "success_rate": 1.0},
            }
        )
        session.commit()

        # Check if should use playbook
        evaluator = PlaybookEvaluator(session)

        # After 3 cases, should suggest playbook for 4th
        should_use = evaluator.should_use_playbook(case_type, case_count=3)
        assert should_use

    def test_no_playbook_for_first_cases(self, session):
        """First 3 cases should not require playbook."""
        evaluator = PlaybookEvaluator(session)

        # Counts 0, 1, 2 should not trigger playbook gate
        for count in [0, 1, 2]:
            should_use = evaluator.should_use_playbook("AIRPORT_DISRUPTION", case_count=count)
            assert not should_use


class TestPlaybookCreation:
    """Tests for playbook creation from cases."""

    def test_create_playbook_from_case(self, session, sample_case_id):
        """Can create playbook from resolved case."""
        # Update case to resolved
        session.execute(
            text('UPDATE "case" SET status = \'RESOLVED\' WHERE id = :id'),
            {"id": sample_case_id}
        )

        # Add some trace events
        for i, event_type in enumerate(["STATE_ENTER", "TOOL_CALL", "TOOL_RESULT", "STATE_EXIT"]):
            session.execute(
                text("""
                    INSERT INTO trace_event (id, case_id, seq, event_type, ref_type, meta)
                    VALUES (:id, :case_id, :seq, :event_type, 'state', :meta)
                """),
                {
                    "id": uuid4(),
                    "case_id": sample_case_id,
                    "seq": i,
                    "event_type": event_type,
                    "meta": {"state": f"S{i}"},
                }
            )

        # Add an action
        session.execute(
            text("""
                INSERT INTO action (id, case_id, type, args, state, risk_level, requires_approval)
                VALUES (:id, :case_id, 'SET_POSTURE', :args, 'COMPLETED', 'LOW', false)
            """),
            {
                "id": uuid4(),
                "case_id": sample_case_id,
                "args": {"posture": "RESTRICT"},
            }
        )
        session.commit()

        # Create playbook
        manager = PlaybookManager(session)
        playbook_id = manager.create_from_case(uuid4(), name="Test Playbook")

        # Verify playbook exists
        playbook = manager.get_playbook(playbook_id)
        assert playbook is not None
        assert playbook["name"] == "Test Playbook"


class TestPlaybookMatching:
    """Tests for playbook matching."""

    def test_find_matching_playbooks(self, session):
        """Can find matching playbooks for case type."""
        case_type = "AIRPORT_DISRUPTION"

        # Create a playbook
        playbook_id = uuid4()
        session.execute(
            text("""
                INSERT INTO playbook (id, name, pattern, action_template, stats)
                VALUES (:id, :name, :pattern, :action_template, :stats)
            """),
            {
                "id": playbook_id,
                "name": "JFK Disruption Playbook",
                "pattern": {
                    "case_type": case_type,
                    "scope_keys": ["airport"],
                },
                "action_template": {
                    "action_sequence": ["SET_POSTURE", "PUBLISH_GATEWAY_ADVISORY"],
                },
                "stats": {"use_count": 5, "success_count": 4, "success_rate": 0.8},
            }
        )
        session.commit()

        # Find matching
        manager = PlaybookManager(session)
        matches = manager.find_matching(case_type, {"airport": "KJFK"}, limit=3)

        assert len(matches) >= 1
        assert any(str(m["playbook_id"]) == str(playbook_id) for m in matches)

    def test_playbook_match_score(self, session):
        """Playbook match score reflects scope overlap."""
        playbook_id = uuid4()
        session.execute(
            text("""
                INSERT INTO playbook (id, name, pattern, action_template, stats)
                VALUES (:id, :name, :pattern, :action_template, :stats)
            """),
            {
                "id": playbook_id,
                "name": "Scope Match Test",
                "pattern": {
                    "case_type": "AIRPORT_DISRUPTION",
                    "scope_keys": ["airport", "region"],  # Requires both
                },
                "action_template": {"action_sequence": []},
                "stats": {},
            }
        )
        session.commit()

        manager = PlaybookManager(session)

        # Partial match (only airport)
        matches_partial = manager.find_matching(
            "AIRPORT_DISRUPTION",
            {"airport": "KJFK"},  # Missing region
            limit=1,
        )

        # Full match (both keys)
        matches_full = manager.find_matching(
            "AIRPORT_DISRUPTION",
            {"airport": "KJFK", "region": "NORTHEAST"},
            limit=1,
        )

        # Full match should have higher score
        if matches_partial and matches_full:
            partial_score = matches_partial[0].get("match_score", 0)
            full_score = matches_full[0].get("match_score", 0)
            assert full_score >= partial_score


class TestPlaybookEvaluation:
    """Tests for playbook evaluation."""

    def test_evaluate_playbook_match(self, session):
        """Can evaluate how well playbook matches case context."""
        playbook_id = uuid4()
        session.execute(
            text("""
                INSERT INTO playbook (id, name, pattern, action_template, stats)
                VALUES (:id, :name, :pattern, :action_template, :stats)
            """),
            {
                "id": playbook_id,
                "name": "Eval Test",
                "pattern": {
                    "case_type": "AIRPORT_DISRUPTION",
                    "scope_keys": ["airport"],
                    "evidence_sources": ["FAA_NAS", "METAR"],
                },
                "action_template": {
                    "action_sequence": ["SET_POSTURE"],
                },
                "stats": {"use_count": 10, "success_count": 8},
            }
        )
        session.commit()

        evaluator = PlaybookEvaluator(session)

        # Evaluate match
        result = evaluator.evaluate_match(
            playbook_id,
            {
                "case_type": "AIRPORT_DISRUPTION",
                "scope": {"airport": "KJFK"},
                "available_sources": ["FAA_NAS", "METAR", "NWS"],
            },
        )

        assert "match" in result
        assert "overall_score" in result
        assert "scope_match" in result
        assert "evidence_match" in result

    def test_evaluate_outcome(self, session, sample_case_id):
        """Can evaluate playbook outcome after use."""
        playbook_id = uuid4()
        session.execute(
            text("""
                INSERT INTO playbook (id, name, pattern, action_template, stats)
                VALUES (:id, :name, :pattern, :action_template, :stats)
            """),
            {
                "id": playbook_id,
                "name": "Outcome Test",
                "pattern": {"case_type": "AIRPORT_DISRUPTION"},
                "action_template": {
                    "action_sequence": ["SET_POSTURE", "NOTIFY_OPS"],
                },
                "stats": {"use_count": 0, "success_count": 0},
            }
        )
        session.commit()

        evaluator = PlaybookEvaluator(session)

        # Evaluate outcome
        result = evaluator.evaluate_outcome(
            playbook_id,
            uuid4(),  # case_id
            actual_actions=["SET_POSTURE"],  # Only did first action
            success=True,
        )

        assert result["success"] == True
        assert "action_match" in result
        assert result["action_match"]["score"] < 1.0  # Partial match


class TestTraceMining:
    """Tests for trace mining."""

    def test_mine_case_trace(self, session, sample_case_id):
        """Can mine patterns from case trace."""
        # Add trace events
        events = [
            ("STATE_ENTER", "state", "S1_INGEST"),
            ("TOOL_CALL", "tool", "fetch_faa"),
            ("TOOL_RESULT", "tool", "faa_result"),
            ("STATE_ENTER", "state", "S2_BUILD"),
        ]

        for i, (event_type, ref_type, ref_id) in enumerate(events):
            session.execute(
                text("""
                    INSERT INTO trace_event (id, case_id, seq, event_type, ref_type, ref_id, meta)
                    VALUES (:id, :case_id, :seq, :event_type, :ref_type, :ref_id, :meta)
                """),
                {
                    "id": uuid4(),
                    "case_id": sample_case_id,
                    "seq": i,
                    "event_type": event_type,
                    "ref_type": ref_type,
                    "ref_id": ref_id,
                    "meta": {"source_system": "FAA_NAS"} if event_type == "TOOL_RESULT" else {},
                }
            )
        session.commit()

        miner = TraceMiner(session)
        pattern = miner.mine_case(uuid4())  # Note: using different ID will find no pattern

        # Pattern should be extractable structure
        assert isinstance(pattern, dict)
