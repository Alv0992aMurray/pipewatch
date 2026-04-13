"""Tests for pipewatch.escalation."""
from datetime import datetime, timezone

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.escalation import (
    AlertEscalator,
    EscalationPolicy,
    EscalationResult,
)

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _alert(pipeline: str = "pipe_a", rule: str = "low_success") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name=rule,
        severity=AlertSeverity.WARNING,
        message="success rate too low",
        value=0.5,
    )


@pytest.fixture
def policy() -> EscalationPolicy:
    return EscalationPolicy(
        pipeline="pipe_a",
        rule_name="low_success",
        threshold=3,
        escalate_to=AlertSeverity.CRITICAL,
    )


@pytest.fixture
def escalator() -> AlertEscalator:
    return AlertEscalator()


def test_first_alert_not_escalated(escalator, policy):
    result = escalator.process([_alert()], [policy], when=_NOW)
    assert result.total_escalated == 0
    assert result.alerts[0].severity == AlertSeverity.WARNING


def test_alert_escalated_at_threshold(escalator, policy):
    for _ in range(2):
        escalator.process([_alert()], [policy], when=_NOW)
    result = escalator.process([_alert()], [policy], when=_NOW)
    assert result.total_escalated == 1
    assert result.alerts[0].severity == AlertSeverity.CRITICAL


def test_escalated_message_prefixed(escalator, policy):
    for _ in range(3):
        result = escalator.process([_alert()], [policy], when=_NOW)
    assert result.alerts[0].message.startswith("[ESCALATED]")


def test_no_policy_alert_unchanged(escalator):
    result = escalator.process([_alert()], [], when=_NOW)
    assert result.total_escalated == 0
    assert result.alerts[0].severity == AlertSeverity.WARNING


def test_reset_clears_count(escalator, policy):
    for _ in range(3):
        escalator.process([_alert()], [policy], when=_NOW)
    escalator.reset("pipe_a", "low_success")
    result = escalator.process([_alert()], [policy], when=_NOW)
    assert result.total_escalated == 0


def test_different_pipelines_tracked_independently(escalator, policy):
    policy_b = EscalationPolicy(
        pipeline="pipe_b",
        rule_name="low_success",
        threshold=3,
        escalate_to=AlertSeverity.CRITICAL,
    )
    for _ in range(3):
        escalator.process([_alert("pipe_a")], [policy], when=_NOW)
    result = escalator.process([_alert("pipe_b")], [policy_b], when=_NOW)
    assert result.total_escalated == 0


def test_entries_returns_all_tracked(escalator, policy):
    escalator.process([_alert()], [policy], when=_NOW)
    entries = escalator.entries()
    assert len(entries) == 1
    assert entries[0].pipeline == "pipe_a"
    assert entries[0].count == 1


def test_entry_to_dict_structure(escalator, policy):
    escalator.process([_alert()], [policy], when=_NOW)
    d = escalator.entries()[0].to_dict()
    assert set(d.keys()) == {"pipeline", "rule_name", "count", "last_fired"}


def test_empty_alert_list_returns_empty_result(escalator, policy):
    result = escalator.process([], [policy], when=_NOW)
    assert result.alerts == []
    assert result.total_escalated == 0
