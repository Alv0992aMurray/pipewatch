"""Tests for pipewatch.saturation."""
from __future__ import annotations
from datetime import datetime, timezone
from pipewatch.history import PipelineHistory, MetricSnapshot
from pipewatch.saturation import SaturationConfig, detect_saturation


def _ts(offset: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)


def _snap(rows: int, duration: float, offset: int = 0) -> MetricSnapshot:
    return MetricSnapshot(
        pipeline="pipe",
        timestamp=_ts(offset),
        success_rate=1.0,
        rows_processed=rows,
        error_count=0,
        duration_seconds=duration,
    )


def _history(snaps) -> PipelineHistory:
    h = PipelineHistory(pipeline="pipe")
    for s in snaps:
        h.add(s)
    return h


def config(ceiling: float = 100.0) -> SaturationConfig:
    return SaturationConfig(pipeline="pipe", ceiling=ceiling)


def test_empty_history_returns_insufficient_data():
    h = _history([])
    result = detect_saturation(h, config())
    assert result is not None
    assert result.insufficient_data is True
    assert result.level == "ok"


def test_low_utilisation_is_ok():
    snaps = [_snap(rows=50, duration=1.0, offset=i) for i in range(5)]
    h = _history(snaps)
    result = detect_saturation(h, config(ceiling=100.0))
    assert result is not None
    assert result.level == "ok"
    assert result.utilisation < 0.80


def test_high_utilisation_is_warning():
    # 85 rows/s out of 100 ceiling => 85% => warning
    snaps = [_snap(rows=85, duration=1.0, offset=i) for i in range(5)]
    h = _history(snaps)
    result = detect_saturation(h, config(ceiling=100.0))
    assert result is not None
    assert result.level == "warning"


def test_critical_utilisation_is_critical():
    snaps = [_snap(rows=97, duration=1.0, offset=i) for i in range(5)]
    h = _history(snaps)
    result = detect_saturation(h, config(ceiling=100.0))
    assert result is not None
    assert result.level == "critical"


def test_utilisation_capped_at_one():
    snaps = [_snap(rows=200, duration=1.0, offset=i) for i in range(5)]
    h = _history(snaps)
    result = detect_saturation(h, config(ceiling=100.0))
    assert result is not None
    assert result.utilisation == 1.0


def test_to_dict_has_expected_keys():
    snaps = [_snap(rows=50, duration=1.0, offset=i) for i in range(3)]
    h = _history(snaps)
    result = detect_saturation(h, config())
    d = result.to_dict()
    assert "pipeline" in d
    assert "avg_throughput" in d
    assert "ceiling" in d
    assert "utilisation" in d
    assert "level" in d
    assert "insufficient_data" in d
