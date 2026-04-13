"""Tests for pipewatch.rate_limit."""
from datetime import datetime, timedelta

import pytest

from pipewatch.rate_limit import AlertRateLimiter, RateLimitConfig


@pytest.fixture()
def config() -> RateLimitConfig:
    """Allow at most 3 alerts per 60-second window for all pipelines."""
    return RateLimitConfig(max_alerts=3, window_seconds=60)


@pytest.fixture()
def limiter(config: RateLimitConfig) -> AlertRateLimiter:
    return AlertRateLimiter(config)


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


def test_first_alert_is_allowed(limiter: AlertRateLimiter) -> None:
    result = limiter.check("orders", now=_now())
    assert result.allowed is True
    assert result.current_count == 0


def test_alerts_within_limit_are_allowed(limiter: AlertRateLimiter) -> None:
    now = _now()
    for i in range(3):
        result = limiter.check("orders", now=now + timedelta(seconds=i))
        assert result.allowed is True


def test_alert_exceeding_limit_is_blocked(limiter: AlertRateLimiter) -> None:
    now = _now()
    for _ in range(3):
        limiter.check("orders", now=now)
    # 4th alert in the same window should be blocked
    result = limiter.check("orders", now=now + timedelta(seconds=1))
    assert result.allowed is False
    assert result.current_count == 3


def test_old_alerts_fall_outside_window(limiter: AlertRateLimiter) -> None:
    now = _now()
    # Fill up the window
    for _ in range(3):
        limiter.check("orders", now=now)
    # Advance past the window
    later = now + timedelta(seconds=61)
    result = limiter.check("orders", now=later)
    assert result.allowed is True
    assert result.current_count == 0


def test_different_pipelines_tracked_independently(limiter: AlertRateLimiter) -> None:
    now = _now()
    for _ in range(3):
        limiter.check("orders", now=now)
    # A different pipeline should still be allowed
    result = limiter.check("inventory", now=now)
    assert result.allowed is True


def test_reset_clears_pipeline_history(limiter: AlertRateLimiter) -> None:
    now = _now()
    for _ in range(3):
        limiter.check("orders", now=now)
    limiter.reset("orders")
    result = limiter.check("orders", now=now)
    assert result.allowed is True
    assert result.current_count == 0


def test_pipeline_scoped_config_ignores_other_pipelines() -> None:
    config = RateLimitConfig(max_alerts=1, window_seconds=60, pipeline="orders")
    limiter = AlertRateLimiter(config)
    now = _now()
    # Exhaust limit for 'orders'
    limiter.check("orders", now=now)
    assert limiter.check("orders", now=now).allowed is False
    # 'inventory' should be unaffected
    assert limiter.check("inventory", now=now).allowed is True


def test_result_to_dict_contains_expected_keys(limiter: AlertRateLimiter) -> None:
    result = limiter.check("orders", now=_now())
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "allowed", "current_count", "max_alerts", "window_seconds"}
    assert d["pipeline"] == "orders"
    assert d["allowed"] is True
