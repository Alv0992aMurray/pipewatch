"""Tests for pipewatch.forecast."""
import pytest
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.forecast import forecast, ForecastResult
from datetime import datetime, timezone


def _ts(i: int) -> datetime:
    return datetime(2024, 1, i + 1, tzinfo=timezone.utc)


def _history(rates, pipeline="pipe") -> PipelineHistory:
    h = PipelineHistory(pipeline=pipeline)
    for i, r in enumerate(rates):
        snap = MetricSnapshot(
            pipeline=pipeline,
            timestamp=_ts(i),
            success_rate=r,
            throughput=100.0,
            error_count=0,
            healthy=r >= 0.95,
        )
        h.add(snap)
    return h


def test_insufficient_data_returns_none():
    h = _history([1.0, 0.9])  # only 2 points, min=3
    result = forecast(h, horizon=1, min_points=3)
    assert result.predicted_rate is None
    assert result.data_points == 2
    assert "Insufficient" in result.message


def test_stable_series_predicts_near_mean():
    h = _history([0.9] * 10)
    result = forecast(h, horizon=1)
    assert result.predicted_rate is not None
    assert abs(result.predicted_rate - 0.9) < 0.01
    assert abs(result.slope) < 0.001


def test_rising_series_predicts_higher():
    rates = [0.5 + i * 0.05 for i in range(10)]
    h = _history(rates)
    result = forecast(h, horizon=1)
    assert result.predicted_rate is not None
    assert result.slope > 0
    assert result.predicted_rate > rates[-1]


def test_declining_series_predicts_lower():
    rates = [0.95 - i * 0.05 for i in range(10)]
    h = _history(rates)
    result = forecast(h, horizon=1)
    assert result.predicted_rate is not None
    assert result.slope < 0


def test_predicted_rate_clamped_to_0_1():
    rates = [0.1] * 5 + [0.0] * 5
    h = _history(rates)
    result = forecast(h, horizon=100)
    assert result.predicted_rate is not None
    assert 0.0 <= result.predicted_rate <= 1.0


def test_to_dict_keys():
    h = _history([0.9, 0.91, 0.92, 0.93])
    result = forecast(h)
    d = result.to_dict()
    for key in ("pipeline", "horizon", "predicted_rate", "slope", "intercept", "data_points", "message"):
        assert key in d


def test_horizon_affects_prediction():
    rates = [0.5 + i * 0.05 for i in range(10)]
    h = _history(rates)
    r1 = forecast(h, horizon=1)
    r5 = forecast(h, horizon=5)
    assert r5.predicted_rate > r1.predicted_rate
