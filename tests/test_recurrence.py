"""Tests for pipewatch.recurrence."""
from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.recurrence import RecurrenceTracker


def _alert(pipeline: str = "pipe_a", rule: str = "low_success") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name=rule,
        severity=AlertSeverity.WARNING,
        message="test alert",
        value=0.5,
    )


@pytest.fixture
def tracker() -> RecurrenceTracker:
    return RecurrenceTracker()


def _ts(offset_seconds: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset_seconds)


def test_no_alerts_produces_empty_result(tracker):
    result = tracker.evaluate([], ts=_ts())
    assert result.entries == []
    assert result.total_recurring == 0


def test_single_alert_recorded_once(tracker):
    result = tracker.evaluate([_alert()], ts=_ts())
    assert len(result.entries) == 1
    assert result.entries[0].occurrences == 1
    assert result.total_recurring == 0


def test_same_alert_twice_increments_occurrences(tracker):
    tracker.evaluate([_alert()], ts=_ts(0))
    result = tracker.evaluate([_alert()], ts=_ts(60))
    assert len(result.entries) == 1
    entry = result.entries[0]
    assert entry.occurrences == 2
    assert result.total_recurring == 1


def test_first_seen_and_last_seen_are_tracked(tracker):
    t1 = _ts(0)
    t2 = _ts(120)
    tracker.evaluate([_alert()], ts=t1)
    result = tracker.evaluate([_alert()], ts=t2)
    entry = result.entries[0]
    assert entry.first_seen == t1
    assert entry.last_seen == t2


def test_different_pipelines_are_tracked_separately(tracker):
    tracker.evaluate([_alert("pipe_a"), _alert("pipe_b")], ts=_ts(0))
    result = tracker.evaluate([_alert("pipe_a")], ts=_ts(60))
    keys = {(e.pipeline, e.rule_name) for e in result.entries}
    assert ("pipe_a", "low_success") in keys
    assert ("pipe_b", "low_success") in keys
    pipe_a_entry = next(e for e in result.entries if e.pipeline == "pipe_a")
    pipe_b_entry = next(e for e in result.entries if e.pipeline == "pipe_b")
    assert pipe_a_entry.occurrences == 2
    assert pipe_b_entry.occurrences == 1


def test_different_rules_are_tracked_separately(tracker):
    tracker.evaluate([_alert(rule="rule_a"), _alert(rule="rule_b")], ts=_ts(0))
    result = tracker.evaluate([_alert(rule="rule_a")], ts=_ts(60))
    rule_a = next(e for e in result.entries if e.rule_name == "rule_a")
    rule_b = next(e for e in result.entries if e.rule_name == "rule_b")
    assert rule_a.occurrences == 2
    assert rule_b.occurrences == 1


def test_reset_removes_entry(tracker):
    tracker.evaluate([_alert()], ts=_ts(0))
    tracker.reset("pipe_a", "low_success")
    result = tracker.evaluate([], ts=_ts(60))
    assert result.entries == []


def test_frequent_entries_filters_single_occurrence(tracker):
    tracker.evaluate([_alert("pipe_a"), _alert("pipe_b")], ts=_ts(0))
    tracker.evaluate([_alert("pipe_a")], ts=_ts(60))
    result = tracker.evaluate([], ts=_ts(120))
    frequent = result.frequent_entries
    assert len(frequent) == 1
    assert frequent[0].pipeline == "pipe_a"


def test_to_dict_contains_expected_keys(tracker):
    tracker.evaluate([_alert()], ts=_ts(0))
    result = tracker.evaluate([_alert()], ts=_ts(60))
    d = result.to_dict()
    assert "total_recurring" in d
    assert "entries" in d
    assert d["total_recurring"] == 1
