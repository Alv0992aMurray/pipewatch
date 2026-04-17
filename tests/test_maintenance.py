"""Tests for pipewatch.maintenance."""
from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.maintenance import MaintenanceWindow, apply_maintenance


def _now():
    return datetime.utcnow()


def _alert(pipeline="pipe_a", msg="low success", severity=AlertSeverity.WARNING):
    return Alert(pipeline=pipeline, message=msg, severity=severity)


def _window(pipeline="pipe_a", offset_start=-1, offset_end=1, reason=""):
    now = _now()
    return MaintenanceWindow(
        pipeline=pipeline,
        start=now + timedelta(hours=offset_start),
        end=now + timedelta(hours=offset_end),
        reason=reason,
    )


def test_window_is_active_within_range():
    w = _window()
    assert w.is_active()


def test_window_is_not_active_before_start():
    now = _now()
    w = MaintenanceWindow("*", now + timedelta(hours=1), now + timedelta(hours=2))
    assert not w.is_active()


def test_window_is_not_active_after_end():
    now = _now()
    w = MaintenanceWindow("*", now - timedelta(hours=2), now - timedelta(hours=1))
    assert not w.is_active()


def test_exact_pipeline_matches_alert():
    w = _window(pipeline="pipe_a")
    a = _alert(pipeline="pipe_a")
    assert w.matches(a)


def test_wildcard_matches_any_pipeline():
    w = _window(pipeline="*")
    a = _alert(pipeline="any_pipeline")
    assert w.matches(a)


def test_wrong_pipeline_does_not_match():
    w = _window(pipeline="pipe_a")
    a = _alert(pipeline="pipe_b")
    assert not w.matches(a)


def test_alert_suppressed_during_active_window():
    alerts = [_alert()]
    windows = [_window(pipeline="pipe_a")]
    result = apply_maintenance(alerts, windows)
    assert len(result.suppressed) == 1
    assert len(result.kept) == 0


def test_alert_kept_when_no_active_windows():
    now = _now()
    alerts = [_alert()]
    windows = [
        MaintenanceWindow("pipe_a", now + timedelta(hours=1), now + timedelta(hours=2))
    ]
    result = apply_maintenance(alerts, windows)
    assert len(result.kept) == 1
    assert result.total_suppressed == 0


def test_mixed_pipelines_only_matching_suppressed():
    alerts = [_alert("pipe_a"), _alert("pipe_b")]
    windows = [_window(pipeline="pipe_a")]
    result = apply_maintenance(alerts, windows)
    assert len(result.suppressed) == 1
    assert result.suppressed[0].pipeline == "pipe_a"
    assert len(result.kept) == 1
    assert result.kept[0].pipeline == "pipe_b"


def test_to_dict_contains_expected_keys():
    w = _window(reason="planned upgrade")
    d = w.to_dict()
    assert "pipeline" in d
    assert "start" in d
    assert "end" in d
    assert "reason" in d
    assert d["reason"] == "planned upgrade"
