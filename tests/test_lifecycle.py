"""Tests for pipewatch.lifecycle."""
from datetime import datetime, timezone, timedelta
import pytest
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.lifecycle import (
    evaluate_lifecycle,
    LifecycleResult,
    _infer_state,
)


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=offset)


def _snap(pipeline: str, healthy: bool, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline=pipeline,
        timestamp=_ts(offset),
        success_rate=1.0 if healthy else 0.3,
        throughput=100.0,
        error_count=0 if healthy else 10,
        is_healthy=healthy,
    )


def _history(pipeline: str, pattern: list) -> PipelineHistory:
    h = PipelineHistory(pipeline=pipeline)
    for i, healthy in enumerate(pattern):
        h.add(_snap(pipeline, healthy, offset=i))
    return h


def test_empty_history_returns_unknown():
    h = PipelineHistory(pipeline="p1")
    assert _infer_state(h) == "unknown"


def test_all_healthy_is_healthy():
    h = _history("p1", [True, True, True])
    assert _infer_state(h) == "healthy"


def test_all_failing_is_degraded():
    h = _history("p1", [False, False, False])
    assert _infer_state(h) == "degraded"


def test_last_healthy_after_failures_is_recovering():
    h = _history("p1", [False, False, True])
    assert _infer_state(h) == "recovering"


def test_last_failing_after_mix_is_degraded():
    h = _history("p1", [True, True, False])
    assert _infer_state(h) == "degraded"


def test_evaluate_lifecycle_returns_state_per_pipeline():
    h1 = _history("alpha", [True, True, True])
    h2 = _history("beta", [False, False, False])
    result = evaluate_lifecycle([h1, h2])
    assert result.get("alpha").state == "healthy"
    assert result.get("beta").state == "degraded"


def test_evaluate_lifecycle_preserves_since_when_state_unchanged():
    h = _history("p1", [True, True, True])
    r1 = evaluate_lifecycle([h])
    r2 = evaluate_lifecycle([h], previous=r1)
    assert r2.get("p1").since == r1.get("p1").since


def test_evaluate_lifecycle_records_previous_state_on_transition():
    h = _history("p1", [True, True, True])
    r1 = evaluate_lifecycle([h])
    h2 = _history("p1", [False, False, False])
    r2 = evaluate_lifecycle([h2], previous=r1)
    assert r2.get("p1").previous == "healthy"
    assert r2.get("p1").state == "degraded"


def test_to_dict_contains_expected_keys():
    h = _history("p1", [True])
    result = evaluate_lifecycle([h])
    d = result.to_dict()
    assert "states" in d
    assert d["states"][0]["pipeline"] == "p1"
