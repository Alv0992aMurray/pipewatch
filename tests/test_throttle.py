"""Tests for pipewatch.throttle."""

from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.throttle import AlertThrottler, ThrottleConfig


def _alert(pipeline: str = "orders", rule: str = "low_success") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule=rule,
        severity=AlertSeverity.WARNING,
        message=f"{pipeline}: {rule} triggered",
        value=0.5,
    )


@pytest.fixture
def config() -> ThrottleConfig:
    return ThrottleConfig(pipeline="orders", window_seconds=60, max_alerts=2)


@pytest.fixture
def throttler(config: ThrottleConfig) -> AlertThrottler:
    return AlertThrottler(configs=[config])


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


def test_first_alert_is_kept(throttler: AlertThrottler) -> None:
    result = throttler.apply([_alert()], now=_now())
    assert len(result.kept) == 1
    assert len(result.suppressed) == 0


def test_alerts_within_limit_are_kept(throttler: AlertThrottler) -> None:
    now = _now()
    result = throttler.apply([_alert(), _alert()], now=now)
    assert len(result.kept) == 2
    assert result.total_suppressed == 0


def test_alert_exceeding_limit_is_suppressed(throttler: AlertThrottler) -> None:
    now = _now()
    alerts = [_alert(), _alert(), _alert()]
    result = throttler.apply(alerts, now=now)
    assert len(result.kept) == 2
    assert result.total_suppressed == 1


def test_alert_allowed_after_window_expires(throttler: AlertThrottler) -> None:
    t0 = _now()
    # Fill up the window
    throttler.apply([_alert(), _alert()], now=t0)
    # Advance past the window
    t1 = t0 + timedelta(seconds=61)
    result = throttler.apply([_alert()], now=t1)
    assert len(result.kept) == 1
    assert result.total_suppressed == 0


def test_unconfigured_pipeline_always_passes() -> None:
    throttler = AlertThrottler(configs=[])
    alerts = [_alert(pipeline="unknown")] * 10
    result = throttler.apply(alerts, now=_now())
    assert len(result.kept) == 10
    assert result.total_suppressed == 0


def test_wildcard_config_applies_to_any_pipeline() -> None:
    wildcard = ThrottleConfig(pipeline="*", window_seconds=60, max_alerts=1)
    throttler = AlertThrottler(configs=[wildcard])
    alerts = [_alert(pipeline="orders"), _alert(pipeline="orders")]
    result = throttler.apply(alerts, now=_now())
    assert len(result.kept) == 1
    assert result.total_suppressed == 1


def test_different_pipelines_tracked_independently() -> None:
    cfg_a = ThrottleConfig(pipeline="orders", window_seconds=60, max_alerts=1)
    cfg_b = ThrottleConfig(pipeline="invoices", window_seconds=60, max_alerts=1)
    throttler = AlertThrottler(configs=[cfg_a, cfg_b])
    alerts = [_alert(pipeline="orders"), _alert(pipeline="invoices")]
    result = throttler.apply(alerts, now=_now())
    assert len(result.kept) == 2
    assert result.total_suppressed == 0


def test_to_dict_contains_expected_keys(throttler: AlertThrottler) -> None:
    result = throttler.apply([_alert()], now=_now())
    d = result.to_dict()
    assert "kept" in d
    assert "suppressed" in d
    assert "total_suppressed" in d
    assert d["total_suppressed"] == 0
