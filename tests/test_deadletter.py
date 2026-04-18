"""Tests for dead-letter queue."""
from datetime import timezone

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.deadletter import DeadLetterQueue
from pipewatch.deadletter_reporter import format_deadletter_report, deadletter_report_to_json


def _alert(pipeline: str = "pipe_a", rule: str = "low_success", severity: AlertSeverity = AlertSeverity.WARNING) -> Alert:
    return Alert(pipeline=pipeline, rule_name=rule, severity=severity, message="test alert", value=0.5)


@pytest.fixture
def queue() -> DeadLetterQueue:
    return DeadLetterQueue()


def test_empty_queue_has_zero_count(queue):
    assert queue.count() == 0


def test_push_adds_entry(queue):
    queue.push(_alert(), reason="timeout")
    assert queue.count() == 1


def test_push_returns_entry_with_correct_fields(queue):
    alert = _alert()
    entry = queue.push(alert, reason="connection refused", attempts=3)
    assert entry.reason == "connection refused"
    assert entry.attempts == 3
    assert entry.alert is alert


def test_all_returns_all_entries(queue):
    queue.push(_alert("a"), reason="err")
    queue.push(_alert("b"), reason="err")
    assert len(queue.all()) == 2


def test_drain_clears_queue(queue):
    queue.push(_alert(), reason="err")
    drained = queue.drain()
    assert len(drained) == 1
    assert queue.count() == 0


def test_find_filters_by_pipeline(queue):
    queue.push(_alert("pipe_x"), reason="err")
    queue.push(_alert("pipe_y"), reason="err")
    results = queue.find("pipe_x")
    assert len(results) == 1
    assert results[0].alert.pipeline == "pipe_x"


def test_most_recent_returns_last_entry(queue):
    queue.push(_alert("first"), reason="err")
    queue.push(_alert("last"), reason="err")
    assert queue.most_recent().alert.pipeline == "last"


def test_most_recent_empty_returns_none(queue):
    assert queue.most_recent() is None


def test_format_report_empty(queue):
    report = format_deadletter_report(queue)
    assert "empty" in report


def test_format_report_shows_entries(queue):
    queue.push(_alert("pipe_a"), reason="timeout", attempts=2)
    report = format_deadletter_report(queue)
    assert "pipe_a" in report
    assert "timeout" in report
    assert "attempts: 2" in report


def test_to_json_contains_count(queue):
    import json
    queue.push(_alert(), reason="err")
    data = json.loads(deadletter_report_to_json(queue))
    assert data["dead_letter_count"] == 1
    assert len(data["entries"]) == 1
