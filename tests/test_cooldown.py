"""Tests for pipewatch.cooldown."""

from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.cooldown import CooldownConfig, CooldownEntry, CooldownManager


def _alert(pipeline: str = "pipe_a", severity: AlertSeverity = AlertSeverity.WARNING) -> Alert:
    return Alert(pipeline=pipeline, message="test alert", severity=severity)


@pytest.fixture
def config() -> CooldownConfig:
    return CooldownConfig(pipeline="pipe_a", seconds=300)


@pytest.fixture
def manager(config: CooldownConfig) -> CooldownManager:
    return CooldownManager(configs=[config])


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


def test_first_alert_is_always_kept(manager: CooldownManager) -> None:
    result = manager.apply([_alert()], now=_now())
    assert len(result.kept) == 1
    assert result.total_suppressed == 0


def test_duplicate_within_cooldown_is_suppressed(manager: CooldownManager) -> None:
    now = _now()
    manager.apply([_alert()], now=now)
    result = manager.apply([_alert()], now=now + timedelta(seconds=60))
    assert result.total_suppressed == 1
    assert len(result.kept) == 0


def test_alert_after_cooldown_is_kept(manager: CooldownManager) -> None:
    now = _now()
    manager.apply([_alert()], now=now)
    result = manager.apply([_alert()], now=now + timedelta(seconds=301))
    assert len(result.kept) == 1
    assert result.total_suppressed == 0


def test_unknown_pipeline_is_always_kept(manager: CooldownManager) -> None:
    result = manager.apply([_alert(pipeline="other_pipe")], now=_now())
    assert len(result.kept) == 1


def test_wildcard_config_applies_to_any_pipeline() -> None:
    mgr = CooldownManager(configs=[CooldownConfig(pipeline="*", seconds=120)])
    now = _now()
    mgr.apply([_alert(pipeline="pipe_x")], now=now)
    result = mgr.apply([_alert(pipeline="pipe_x")], now=now + timedelta(seconds=30))
    assert result.total_suppressed == 1


def test_entry_recorded_after_first_alert(manager: CooldownManager) -> None:
    manager.apply([_alert()], now=_now())
    entries = manager.entries()
    assert len(entries) == 1
    assert entries[0].pipeline == "pipe_a"


def test_entry_is_cooling_within_window() -> None:
    entry = CooldownEntry(pipeline="p", last_alert_at=_now(), cooldown_seconds=300)
    assert entry.is_cooling(now=_now() + timedelta(seconds=100))


def test_entry_is_not_cooling_after_window() -> None:
    entry = CooldownEntry(pipeline="p", last_alert_at=_now(), cooldown_seconds=300)
    assert not entry.is_cooling(now=_now() + timedelta(seconds=400))


def test_to_dict_contains_expected_keys() -> None:
    entry = CooldownEntry(pipeline="p", last_alert_at=_now(), cooldown_seconds=60)
    d = entry.to_dict()
    assert "pipeline" in d
    assert "last_alert_at" in d
    assert "cooldown_seconds" in d
