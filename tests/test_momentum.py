"""Tests for pipewatch.momentum."""
from datetime import datetime, timedelta
import pytest

from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.momentum import detect_momentum, _velocity, _classify


def _ts(offset_seconds: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset_seconds)


def _snap(success_rate: float, throughput: float = 100.0, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="test_pipe",
        timestamp=_ts(offset),
        success_rate=success_rate,
        throughput=throughput,
        error_count=0,
        is_healthy=success_rate >= 0.95,
    )


def _history(snaps) -> PipelineHistory:
    h = PipelineHistory(pipeline="test_pipe")
    for s in snaps:
        h.add(s)
    return h


# --- unit helpers ---

def test_velocity_empty_returns_none():
    assert _velocity([]) is None


def test_velocity_single_returns_none():
    assert _velocity([0.9]) is None


def test_velocity_constant_series_is_zero():
    assert _velocity([0.9, 0.9, 0.9]) == pytest.approx(0.0)


def test_velocity_rising_series_is_positive():
    assert _velocity([0.1, 0.2, 0.3]) == pytest.approx(0.1)


def test_classify_positive_acceleration():
    assert _classify(0.05) == "accelerating"


def test_classify_negative_acceleration():
    assert _classify(-0.05) == "decelerating"


def test_classify_near_zero_is_stable():
    assert _classify(0.001) == "stable"


# --- detect_momentum integration ---

def test_insufficient_data_returns_no_result():
    h = _history([_snap(0.9, offset=i * 10) for i in range(4)])
    result = detect_momentum(h, min_snapshots=6)
    assert not result.sufficient_data
    assert result.label == "insufficient data"
    assert result.acceleration is None


def test_stable_series_is_labelled_stable():
    snaps = [_snap(0.95, offset=i * 10) for i in range(10)]
    h = _history(snaps)
    result = detect_momentum(h, min_snapshots=6)
    assert result.sufficient_data
    assert result.label == "stable"
    assert result.acceleration == pytest.approx(0.0, abs=1e-9)


def test_accelerating_improvement_detected():
    # first half: slow improvement; second half: fast improvement
    first = [0.50 + i * 0.01 for i in range(5)]   # +0.01/step
    second = [first[-1] + i * 0.05 for i in range(5)]  # +0.05/step
    snaps = [_snap(v, offset=i * 10) for i, v in enumerate(first + second)]
    h = _history(snaps)
    result = detect_momentum(h, min_snapshots=6)
    assert result.sufficient_data
    assert result.label == "accelerating"
    assert result.acceleration > 0


def test_decelerating_improvement_detected():
    # first half: fast improvement; second half: slow improvement
    first = [0.50 + i * 0.05 for i in range(5)]
    second = [first[-1] + i * 0.001 for i in range(5)]
    snaps = [_snap(v, offset=i * 10) for i, v in enumerate(first + second)]
    h = _history(snaps)
    result = detect_momentum(h, min_snapshots=6)
    assert result.sufficient_data
    assert result.label == "decelerating"
    assert result.acceleration < 0


def test_result_to_dict_has_expected_keys():
    snaps = [_snap(0.95, offset=i * 10) for i in range(10)]
    h = _history(snaps)
    result = detect_momentum(h, min_snapshots=6)
    d = result.to_dict()
    for key in ("pipeline", "metric", "snapshots_used", "first_velocity",
                "second_velocity", "acceleration", "label", "sufficient_data"):
        assert key in d


def test_pipeline_name_propagated():
    snaps = [_snap(0.95, offset=i * 10) for i in range(10)]
    h = _history(snaps)
    result = detect_momentum(h, min_snapshots=6)
    assert result.pipeline == "test_pipe"
