"""Tests for pipewatch.muting."""
from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.muting import MuteRule, MuteResult, apply_mutes


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


def _alert(pipeline: str = "pipe_a", rule_name: str = "low_success") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name=rule_name,
        severity=AlertSeverity.WARNING,
        message="test alert",
    )


@pytest.fixture
def future() -> datetime:
    return _now() + timedelta(hours=1)


@pytest.fixture
def past() -> datetime:
    return _now() - timedelta(seconds=1)


def test_rule_is_active_before_expiry(future):
    rule = MuteRule(pipeline="pipe_a", rule_name=None, expires_at=future)
    assert rule.is_active(_now()) is True


def test_rule_is_not_active_after_expiry(past):
    rule = MuteRule(pipeline="pipe_a", rule_name=None, expires_at=past)
    assert rule.is_active(_now()) is False


def test_exact_match_mutes_alert(future):
    rule = MuteRule(pipeline="pipe_a", rule_name="low_success", expires_at=future)
    alert = _alert("pipe_a", "low_success")
    result = apply_mutes([alert], [rule], now=_now())
    assert len(result.muted) == 1
    assert len(result.kept) == 0


def test_wildcard_pipeline_mutes_any_pipeline(future):
    rule = MuteRule(pipeline=None, rule_name="low_success", expires_at=future)
    alerts = [_alert("pipe_a"), _alert("pipe_b")]
    result = apply_mutes(alerts, [rule], now=_now())
    assert result.total_muted == 2


def test_wildcard_rule_name_mutes_any_rule(future):
    rule = MuteRule(pipeline="pipe_a", rule_name=None, expires_at=future)
    alerts = [_alert("pipe_a", "low_success"), _alert("pipe_a", "high_errors")]
    result = apply_mutes(alerts, [rule], now=_now())
    assert result.total_muted == 2


def test_expired_rule_does_not_mute(past):
    rule = MuteRule(pipeline="pipe_a", rule_name="low_success", expires_at=past)
    alert = _alert("pipe_a", "low_success")
    result = apply_mutes([alert], [rule], now=_now())
    assert len(result.kept) == 1
    assert result.total_muted == 0


def test_no_rules_keeps_all_alerts():
    alerts = [_alert(), _alert("pipe_b")]
    result = apply_mutes(alerts, [], now=_now())
    assert len(result.kept) == 2
    assert result.total_muted == 0


def test_wrong_pipeline_does_not_mute(future):
    rule = MuteRule(pipeline="pipe_x", rule_name=None, expires_at=future)
    alert = _alert("pipe_a")
    result = apply_mutes([alert], [rule], now=_now())
    assert len(result.kept) == 1


def test_to_dict_contains_expected_keys(future):
    rule = MuteRule(pipeline="pipe_a", rule_name="low_success", expires_at=future, reason="maintenance")
    d = rule.to_dict()
    assert d["pipeline"] == "pipe_a"
    assert d["rule_name"] == "low_success"
    assert d["reason"] == "maintenance"
    assert d["active"] is True


def test_mute_result_to_dict(future):
    rule = MuteRule(pipeline="pipe_a", rule_name=None, expires_at=future)
    result = apply_mutes([_alert()], [rule], now=_now())
    d = result.to_dict()
    assert "kept" in d
    assert "muted" in d
    assert d["total_muted"] == 1
