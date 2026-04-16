"""Tests for pipewatch.staleness."""

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.staleness import (
    StalenessConfig,
    StalenessResult,
    check_staleness,
    check_all_staleness,
)


TZ = timezone.utc


def _ts(offset_seconds: float = 0.0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=TZ) + timedelta(seconds=offset_seconds)


def _snap(offset_seconds: float = 0.0, success_rate: float = 1.0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe",
        timestamp=_ts(offset_seconds),
        success_rate=success_rate,
        throughput=100.0,
        error_count=0,
        healthy=True,
    )


@pytest.fixture
def config() -> StalenessConfig:
    return StalenessConfig(pipeline="pipe", max_age_seconds=300)


@pytest.fixture
def now() -> datetime:
    return _ts(0)


# --- empty history ---

def test_empty_history_is_stale(config, now):
    history = PipelineHistory(pipeline="pipe")
    result = check_staleness(config, history, now=now)
    assert result.is_stale is True
    assert result.last_seen is None
    assert result.age_seconds is None


def test_empty_history_result_has_correct_pipeline(config, now):
    history = PipelineHistory(pipeline="pipe")
    result = check_staleness(config, history, now=now)
    assert result.pipeline == "pipe"
    assert result.threshold_seconds == 300


# --- fresh snapshot ---

def test_recent_snapshot_is_not_stale(config, now):
    history = PipelineHistory(pipeline="pipe")
    history.add(_snap(offset_seconds=-60))  # 60 s ago
    result = check_staleness(config, history, now=now)
    assert result.is_stale is False
    assert result.age_seconds == pytest.approx(60.0, abs=0.1)


def test_snapshot_exactly_at_threshold_is_not_stale(config, now):
    history = PipelineHistory(pipeline="pipe")
    history.add(_snap(offset_seconds=-300))  # exactly 300 s ago
    result = check_staleness(config, history, now=now)
    assert result.is_stale is False


# --- stale snapshot ---

def test_old_snapshot_is_stale(config, now):
    history = PipelineHistory(pipeline="pipe")
    history.add(_snap(offset_seconds=-600))  # 600 s ago
    result = check_staleness(config, history, now=now)
    assert result.is_stale is True
    assert result.age_seconds == pytest.approx(600.0, abs=0.1)


# --- to_dict ---

def test_to_dict_keys(config, now):
    history = PipelineHistory(pipeline="pipe")
    history.add(_snap(offset_seconds=-100))
    result = check_staleness(config, history, now=now)
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "last_seen", "age_seconds", "is_stale", "threshold_seconds"}


def test_to_dict_empty_history_has_null_last_seen(config, now):
    history = PipelineHistory(pipeline="pipe")
    result = check_staleness(config, history, now=now)
    assert result.to_dict()["last_seen"] is None
    assert result.to_dict()["age_seconds"] is None


# --- check_all_staleness ---

def test_check_all_returns_one_result_per_config(now):
    configs = [
        StalenessConfig(pipeline="a", max_age_seconds=300),
        StalenessConfig(pipeline="b", max_age_seconds=600),
    ]
    hist_a = PipelineHistory(pipeline="a")
    hist_a.add(_snap(offset_seconds=-100))
    histories = {"a": hist_a}
    results = check_all_staleness(configs, histories, now=now)
    assert len(results) == 2
    assert results[0].pipeline == "a"
    assert results[1].pipeline == "b"


def test_check_all_missing_history_is_stale(now):
    configs = [StalenessConfig(pipeline="missing", max_age_seconds=300)]
    results = check_all_staleness(configs, {}, now=now)
    assert results[0].is_stale is True
