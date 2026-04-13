"""Tests for pipewatch.correlation."""
import pytest
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.correlation import correlate, _pearson, _interpret
from datetime import datetime, timezone


def _ts():
    return datetime.now(timezone.utc)


def _snap(pipeline: str, success_rate: float, throughput: float = 100.0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline_name=pipeline,
        timestamp=_ts(),
        success_rate=success_rate,
        throughput=throughput,
        error_count=0,
        is_healthy=success_rate >= 0.9,
    )


def _history(name: str, rates: list[float]) -> PipelineHistory:
    h = PipelineHistory(pipeline_name=name)
    for r in rates:
        h.add(_snap(name, r))
    return h


# --- _pearson unit tests ---

def test_pearson_perfect_positive():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert _pearson(xs, xs) == pytest.approx(1.0, abs=1e-4)


def test_pearson_perfect_negative():
    xs = [1.0, 2.0, 3.0]
    ys = [3.0, 2.0, 1.0]
    assert _pearson(xs, ys) == pytest.approx(-1.0, abs=1e-4)


def test_pearson_returns_none_for_single_point():
    assert _pearson([0.9], [0.8]) is None


def test_pearson_returns_none_for_zero_variance():
    assert _pearson([1.0, 1.0, 1.0], [0.5, 0.6, 0.7]) is None


# --- _interpret unit tests ---

def test_interpret_strong_positive():
    assert _interpret(0.95) == "strong positive correlation"


def test_interpret_moderate_negative():
    assert _interpret(-0.65) == "moderate negative correlation"


def test_interpret_no_correlation():
    assert _interpret(0.1) == "no meaningful correlation"


def test_interpret_none():
    assert _interpret(None) == "insufficient data"


# --- correlate integration tests ---

def test_correlate_strong_positive():
    rates = [0.95, 0.90, 0.85, 0.80, 0.75]
    ha = _history("pipe_a", rates)
    hb = _history("pipe_b", rates)
    result = correlate(ha, hb)
    assert result.coefficient == pytest.approx(1.0, abs=1e-4)
    assert "strong positive" in result.interpretation
    assert result.n == 5


def test_correlate_aligns_to_shorter_history():
    ha = _history("pipe_a", [0.9, 0.8, 0.7, 0.6])
    hb = _history("pipe_b", [0.9, 0.8, 0.7])
    result = correlate(ha, hb)
    assert result.n == 3


def test_correlate_insufficient_data_returns_none_coefficient():
    ha = _history("pipe_a", [0.9])
    hb = _history("pipe_b", [0.8])
    result = correlate(ha, hb)
    assert result.coefficient is None
    assert result.interpretation == "insufficient data"


def test_correlate_empty_history():
    ha = _history("pipe_a", [])
    hb = _history("pipe_b", [0.9, 0.8])
    result = correlate(ha, hb)
    assert result.n == 0
    assert result.coefficient is None


def test_correlate_to_dict_keys():
    ha = _history("pipe_a", [0.9, 0.8, 0.7])
    hb = _history("pipe_b", [0.85, 0.75, 0.65])
    d = correlate(ha, hb).to_dict()
    assert set(d.keys()) == {"pipeline_a", "pipeline_b", "metric", "n", "coefficient", "interpretation"}
    assert d["pipeline_a"] == "pipe_a"
    assert d["metric"] == "success_rate"
