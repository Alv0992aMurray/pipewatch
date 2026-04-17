"""Tests for pipewatch.heartbeat and pipewatch.heartbeat_reporter."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.heartbeat import HeartbeatConfig, check_heartbeat
from pipewatch.heartbeat_reporter import format_heartbeat_result, format_heartbeat_report, heartbeat_report_to_json
from pipewatch.history import PipelineHistory
from pipewatch.metrics import PipelineMetric
from pipewatch.history import MetricSnapshot


def _ts(offset_seconds: float = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset_seconds)


def _snap(ts: datetime, success_rate: float = 1.0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe",
        timestamp=ts,
        success_rate=success_rate,
        throughput=100.0,
        error_count=0,
        healthy=True,
    )


@pytest.fixture
def config() -> HeartbeatConfig:
    return HeartbeatConfig(pipeline="pipe", expected_interval_seconds=300, grace_seconds=60)


def test_empty_history_is_missed(config):
    history = PipelineHistory(pipeline="pipe")
    result = check_heartbeat(config, history, now=_ts(0))
    assert result.missed is True
    assert result.last_seen is None
    assert result.seconds_since_last is None


def test_recent_snapshot_is_ok(config):
    history = PipelineHistory(pipeline="pipe")
    history.add(_snap(_ts(-100)))
    result = check_heartbeat(config, history, now=_ts(0))
    assert result.missed is False
    assert result.seconds_since_last == pytest.approx(100.0)


def test_snapshot_within_grace_is_ok(config):
    history = PipelineHistory(pipeline="pipe")
    history.add(_snap(_ts(-350)))  # 300 + 60 = 360 deadline; 350 < 360
    result = check_heartbeat(config, history, now=_ts(0))
    assert result.missed is False


def test_snapshot_beyond_grace_is_missed(config):
    history = PipelineHistory(pipeline="pipe")
    history.add(_snap(_ts(-400)))  # 400 > 360
    result = check_heartbeat(config, history, now=_ts(0))
    assert result.missed is True


def test_to_dict_contains_expected_keys(config):
    history = PipelineHistory(pipeline="pipe")
    history.add(_snap(_ts(-50)))
    result = check_heartbeat(config, history, now=_ts(0))
    d = result.to_dict()
    assert "pipeline" in d
    assert "last_seen" in d
    assert "missed" in d
    assert "seconds_since_last" in d


def test_format_result_ok(config):
    history = PipelineHistory(pipeline="pipe")
    history.add(_snap(_ts(-50)))
    result = check_heartbeat(config, history, now=_ts(0))
    text = format_heartbeat_result(result)
    assert "OK" in text
    assert "pipe" in text


def test_format_result_missed_shows_missed(config):
    history = PipelineHistory(pipeline="pipe")
    result = check_heartbeat(config, history, now=_ts(0))
    text = format_heartbeat_result(result)
    assert "MISSED" in text
    assert "never seen" in text


def test_format_report_empty():
    text = format_heartbeat_report([])
    assert "no pipelines" in text


def test_format_report_summary(config):
    history = PipelineHistory(pipeline="pipe")
    result = check_heartbeat(config, history, now=_ts(0))
    text = format_heartbeat_report([result])
    assert "1 missed" in text


def test_heartbeat_report_to_json(config):
    import json
    history = PipelineHistory(pipeline="pipe")
    result = check_heartbeat(config, history, now=_ts(0))
    data = json.loads(heartbeat_report_to_json([result]))
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe"
