"""Tests for pipewatch.projection."""
import pytest
from datetime import datetime, timedelta
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.projection import project, ProjectionResult


def _ts(offset: int) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) + timedelta(minutes=offset)


def _snap(pipeline: str, success_rate: float, throughput: float, offset: int) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_ts(offset),
        success_rate=success_rate,
        throughput=throughput,
        error_rate=1.0 - success_rate,
        is_healthy=success_rate >= 0.95,
    )


def _history(snaps):
    h = PipelineHistory()
    for s in snaps:
        h.add(s)
    return h


def test_empty_history_returns_none():
    h = PipelineHistory()
    assert project(h) is None


def test_insufficient_data_sets_flag():
    snaps = [_snap("p", 0.9, 100, i * 5) for i in range(2)]
    result = project(_history(snaps), steps=3, min_snapshots=3)
    assert result is not None
    assert result.insufficient_data is True
    assert result.points == []


def test_stable_series_projects_near_same_value():
    snaps = [_snap("p", 0.95, 200.0, i * 5) for i in range(5)]
    result = project(_history(snaps), steps=3)
    assert result is not None
    assert not result.insufficient_data
    assert len(result.points) == 3
    for pt in result.points:
        assert abs(pt.predicted_success_rate - 0.95) < 0.05
        assert abs(pt.predicted_throughput - 200.0) < 10.0


def test_rising_series_predicts_higher_rate():
    rates = [0.80, 0.85, 0.88, 0.91, 0.94]
    snaps = [_snap("p", r, 100.0, i * 5) for i, r in enumerate(rates)]
    result = project(_history(snaps), steps=2)
    assert result is not None
    assert result.points[0].predicted_success_rate > rates[-1]


def test_declining_series_predicts_lower_rate():
    rates = [0.98, 0.95, 0.91, 0.87, 0.82]
    snaps = [_snap("p", r, 100.0, i * 5) for i, r in enumerate(rates)]
    result = project(_history(snaps), steps=2)
    assert result is not None
    assert result.points[0].predicted_success_rate < rates[-1]


def test_predicted_success_rate_clamped_between_zero_and_one():
    rates = [0.99, 0.995, 0.998, 0.999, 1.0]
    snaps = [_snap("p", r, 100.0, i * 5) for i, r in enumerate(rates)]
    result = project(_history(snaps), steps=3)
    assert result is not None
    for pt in result.points:
        assert 0.0 <= pt.predicted_success_rate <= 1.0


def test_to_dict_structure():
    snaps = [_snap("pipe1", 0.9, 150.0, i * 5) for i in range(4)]
    result = project(_history(snaps), steps=2)
    assert result is not None
    d = result.to_dict()
    assert d["pipeline"] == "pipe1"
    assert d["steps"] == 2
    assert isinstance(d["points"], list)
    assert len(d["points"]) == 2
    assert "predicted_success_rate" in d["points"][0]
    assert "predicted_throughput" in d["points"][0]


def test_step_numbers_are_sequential():
    snaps = [_snap("p", 0.9, 100.0, i * 5) for i in range(5)]
    result = project(_history(snaps), steps=4)
    assert result is not None
    for i, pt in enumerate(result.points):
        assert pt.step == i + 1
