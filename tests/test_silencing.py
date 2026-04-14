"""Tests for pipewatch.silencing."""
from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.silencing import SilenceWindow, apply_silences


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


def _alert(pipeline: str = "orders") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name="low_success",
        message="success rate too low",
        severity=AlertSeverity.WARNING,
        value=0.5,
    )


def _window(
    pipeline: str = "orders",
    offset_start: int = -60,
    offset_end: int = 60,
) -> SilenceWindow:
    now = _now()
    return SilenceWindow(
        name="test-window",
        pipeline=pipeline,
        start=now + timedelta(minutes=offset_start),
        end=now + timedelta(minutes=offset_end),
    )


def test_window_is_active_within_range():
    w = _window()
    assert w.is_active(_now()) is True


def test_window_is_not_active_before_start():
    w = _window(offset_start=10, offset_end=60)
    assert w.is_active(_now()) is False


def test_window_is_not_active_after_end():
    w = _window(offset_start=-120, offset_end=-10)
    assert w.is_active(_now()) is False


def test_window_matches_specific_pipeline():
    w = _window(pipeline="orders")
    assert w.matches(_alert("orders"), _now()) is True


def test_window_does_not_match_other_pipeline():
    w = _window(pipeline="orders")
    assert w.matches(_alert("payments"), _now()) is False


def test_wildcard_pipeline_matches_any():
    w = _window(pipeline="*")
    assert w.matches(_alert("payments"), _now()) is True
    assert w.matches(_alert("orders"), _now()) is True


def test_inactive_window_does_not_match():
    w = _window(offset_start=10, offset_end=60)
    assert w.matches(_alert("orders"), _now()) is False


def test_apply_silences_keeps_unmatched_alerts():
    alerts = [_alert("orders"), _alert("payments")]
    windows = [_window(pipeline="orders")]
    result = apply_silences(alerts, windows, _now())
    assert len(result.kept) == 1
    assert result.kept[0].pipeline == "payments"
    assert result.total_silenced == 1


def test_apply_silences_empty_windows_keeps_all():
    alerts = [_alert(), _alert("payments")]
    result = apply_silences(alerts, [], _now())
    assert len(result.kept) == 2
    assert result.total_silenced == 0


def test_apply_silences_all_silenced_by_wildcard():
    alerts = [_alert("orders"), _alert("payments")]
    windows = [_window(pipeline="*")]
    result = apply_silences(alerts, windows, _now())
    assert result.kept == []
    assert result.total_silenced == 2
