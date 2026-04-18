"""Tests for pipewatch.dispatch."""
import pytest
from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.dispatch import DispatchTarget, dispatch_alerts


def _alert(pipeline: str, severity: AlertSeverity, rule: str = "r") -> Alert:
    return Alert(pipeline=pipeline, rule=rule, severity=severity, message="msg")


@pytest.fixture
def warning_alert():
    return _alert("pipe_a", AlertSeverity.WARNING)


@pytest.fixture
def critical_alert():
    return _alert("pipe_b", AlertSeverity.CRITICAL)


def test_no_targets_drops_all_alerts(warning_alert, critical_alert):
    result = dispatch_alerts([warning_alert, critical_alert], [])
    assert result.total_dispatched == 0
    assert result.total_dropped == 2


def test_target_accepts_matching_severity(warning_alert):
    target = DispatchTarget(name="slack", min_severity=AlertSeverity.WARNING)
    result = dispatch_alerts([warning_alert], [target])
    assert result.total_dispatched == 1
    assert result.total_dropped == 0


def test_target_rejects_below_min_severity(warning_alert):
    target = DispatchTarget(name="pager", min_severity=AlertSeverity.CRITICAL)
    result = dispatch_alerts([warning_alert], [target])
    assert result.total_dispatched == 0
    assert result.total_dropped == 1


def test_target_accepts_critical_when_min_is_warning(critical_alert):
    target = DispatchTarget(name="slack", min_severity=AlertSeverity.WARNING)
    result = dispatch_alerts([critical_alert], [target])
    assert result.total_dispatched == 1


def test_pipeline_filter_accepts_matching_pipeline(warning_alert):
    target = DispatchTarget(name="slack", pipelines=["pipe_a"])
    result = dispatch_alerts([warning_alert], [target])
    assert result.total_dispatched == 1


def test_pipeline_filter_rejects_other_pipeline(critical_alert):
    target = DispatchTarget(name="slack", pipelines=["pipe_a"])
    result = dispatch_alerts([critical_alert], [target])
    assert result.total_dispatched == 0
    assert result.total_dropped == 1


def test_alert_dispatched_to_multiple_targets(warning_alert):
    t1 = DispatchTarget(name="slack", min_severity=AlertSeverity.WARNING)
    t2 = DispatchTarget(name="email", min_severity=AlertSeverity.WARNING)
    result = dispatch_alerts([warning_alert], [t1, t2])
    assert result.total_dispatched == 2
    assert set(result.targets_hit()) == {"slack", "email"}


def test_dispatch_record_to_dict(warning_alert):
    target = DispatchTarget(name="slack", min_severity=AlertSeverity.WARNING)
    result = dispatch_alerts([warning_alert], [target])
    d = result.records[0].to_dict()
    assert d["target"] == "slack"
    assert d["pipeline"] == "pipe_a"
    assert d["severity"] == AlertSeverity.WARNING.value


def test_targets_hit_deduplicates(warning_alert, critical_alert):
    target = DispatchTarget(name="slack", min_severity=AlertSeverity.WARNING)
    result = dispatch_alerts([warning_alert, critical_alert], [target])
    assert result.targets_hit() == ["slack"]
