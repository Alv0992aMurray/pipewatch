"""Tests for pipewatch.dedup — alert deduplication logic."""
from __future__ import annotations

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.dedup import AlertDeduplicator, DedupResult


def _alert(pipeline: str = "orders", rule: str = "low_success", msg: str = "rate low") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name=rule,
        severity=AlertSeverity.WARNING,
        message=msg,
        metric_value=0.5,
        threshold=0.9,
    )


@pytest.fixture()
def dedup() -> AlertDeduplicator:
    return AlertDeduplicator(cooldown_seconds=60.0)


def test_first_alert_is_always_kept(dedup):
    result = dedup.process([_alert()])
    assert len(result.kept) == 1
    assert len(result.suppressed) == 0


def test_duplicate_within_cooldown_is_suppressed(dedup):
    dedup.process([_alert()])
    result = dedup.process([_alert()])
    assert len(result.kept) == 0
    assert len(result.suppressed) == 1


def test_duplicate_after_cooldown_is_kept(dedup):
    t = 1_000_000.0
    dedup._now = lambda: t
    dedup.process([_alert()])

    dedup._now = lambda: t + 61.0  # past cooldown
    result = dedup.process([_alert()])
    assert len(result.kept) == 1
    assert len(result.suppressed) == 0


def test_different_pipelines_are_independent(dedup):
    alerts = [_alert(pipeline="orders"), _alert(pipeline="payments")]
    result = dedup.process(alerts)
    assert len(result.kept) == 2

    result2 = dedup.process(alerts)
    assert len(result2.suppressed) == 2


def test_different_rules_same_pipeline_are_independent(dedup):
    a1 = _alert(rule="low_success")
    a2 = _alert(rule="high_errors")
    result = dedup.process([a1, a2])
    assert len(result.kept) == 2


def test_suppressed_count_increments(dedup):
    dedup.process([_alert()])
    dedup.process([_alert()])
    dedup.process([_alert()])
    entries = dedup.entries()
    assert entries[0].count == 3


def test_clear_resets_state(dedup):
    dedup.process([_alert()])
    dedup.clear()
    result = dedup.process([_alert()])
    assert len(result.kept) == 1


def test_empty_alert_list_returns_empty_result(dedup):
    result = dedup.process([])
    assert isinstance(result, DedupResult)
    assert result.total_suppressed == 0
    assert result.kept == []
