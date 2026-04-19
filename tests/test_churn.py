"""Tests for pipewatch.churn."""
import pytest
from datetime import datetime, timedelta
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.churn import detect_churn, _count_transitions


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) + timedelta(minutes=offset)


def _snap(pipeline: str, healthy: bool, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_ts(offset),
        success_rate=1.0 if healthy else 0.2,
        throughput=100.0,
        error_count=0 if healthy else 10,
        is_healthy=healthy,
    )


def _history(states: list[bool]) -> PipelineHistory:
    h = PipelineHistory(pipeline="test-pipe")
    for i, state in enumerate(states):
        h.add(_snap("test-pipe", state, offset=i))
    return h


def test_empty_history_returns_none():
    h = PipelineHistory(pipeline="test-pipe")
    assert detect_churn(h) is None


def test_too_few_snapshots_returns_none():
    h = _history([True, False])
    assert detect_churn(h) is None


def test_stable_healthy_has_no_churn():
    h = _history([True] * 10)
    result = detect_churn(h)
    assert result is not None
    assert result.transitions == 0
    assert not result.is_churning


def test_alternating_states_is_churning():
    h = _history([True, False, True, False, True, False, True, False])
    result = detect_churn(h)
    assert result is not None
    assert result.is_churning
    assert result.transitions == 7


def test_churn_rate_calculation():
    # 2 transitions in 4 intervals (5 snapshots)
    h = _history([True, True, False, False, True])
    result = detect_churn(h, window=5)
    assert result is not None
    assert result.transitions == 2
    assert abs(result.churn_rate - 2 / 4) < 1e-6


def test_custom_threshold_respected():
    # churn_rate = 2/4 = 0.5, threshold=0.6 => not churning
    h = _history([True, True, False, False, True])
    result = detect_churn(h, window=5, threshold=0.6)
    assert result is not None
    assert not result.is_churning


def test_note_present_when_churning():
    h = _history([True, False] * 5)
    result = detect_churn(h)
    assert result is not None
    assert result.is_churning
    assert len(result.note) > 0


def test_note_empty_when_not_churning():
    h = _history([True] * 8)
    result = detect_churn(h)
    assert result is not None
    assert result.note == ""


def test_to_dict_keys():
    h = _history([True, False, True, False, True])
    result = detect_churn(h)
    assert result is not None
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "window_size", "transitions", "churn_rate", "is_churning", "note"}


def test_count_transitions_empty():
    assert _count_transitions([]) == 0


def test_count_transitions_single():
    assert _count_transitions([True]) == 0
