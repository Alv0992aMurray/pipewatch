"""Tests for pipewatch.burndown and pipewatch.burndown_reporter."""
from datetime import datetime, timezone

import pytest

from pipewatch.burndown import BurndownPoint, BurndownResult, compute_burndown
from pipewatch.burndown_reporter import (
    burndown_report_to_json,
    format_burndown_report,
    format_burndown_result,
)


def _ts(offset: int) -> datetime:
    return datetime(2024, 1, 1, offset, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def improving_events():
    return [
        {"timestamp": _ts(0), "opened": 5, "resolved": 1},
        {"timestamp": _ts(1), "opened": 3, "resolved": 2},
        {"timestamp": _ts(2), "opened": 1, "resolved": 3},
    ]


def test_compute_burndown_creates_correct_points(improving_events):
    result = compute_burndown("pipe-a", improving_events)
    assert len(result.points) == 3
    assert result.points[0].open_count == 5
    assert result.points[2].open_count == 1


def test_total_opened_and_resolved(improving_events):
    result = compute_burndown("pipe-a", improving_events)
    assert result.total_opened == 9
    assert result.total_resolved == 6


def test_resolution_rate(improving_events):
    result = compute_burndown("pipe-a", improving_events)
    assert result.resolution_rate == pytest.approx(6 / 9, rel=1e-4)


def test_resolution_rate_none_when_no_opens():
    result = compute_burndown("pipe-b", [])
    assert result.resolution_rate is None


def test_trend_improving(improving_events):
    result = compute_burndown("pipe-a", improving_events)
    assert result.trend == "improving"


def test_trend_worsening():
    events = [
        {"timestamp": _ts(0), "opened": 1, "resolved": 0},
        {"timestamp": _ts(1), "opened": 4, "resolved": 0},
    ]
    result = compute_burndown("pipe-c", events)
    assert result.trend == "worsening"


def test_trend_stable():
    events = [
        {"timestamp": _ts(0), "opened": 3, "resolved": 1},
        {"timestamp": _ts(1), "opened": 3, "resolved": 1},
    ]
    result = compute_burndown("pipe-d", events)
    assert result.trend == "stable"


def test_trend_insufficient_data():
    result = compute_burndown("pipe-e", [{"timestamp": _ts(0), "opened": 2, "resolved": 1}])
    assert result.trend == "insufficient_data"


def test_to_dict_keys(improving_events):
    result = compute_burndown("pipe-a", improving_events)
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "total_opened", "total_resolved", "resolution_rate", "trend", "points"}


def test_format_result_contains_pipeline(improving_events):
    result = compute_burndown("pipe-a", improving_events)
    text = format_burndown_result(result)
    assert "pipe-a" in text
    assert "improving" in text


def test_format_report_empty():
    assert format_burndown_report([]) == "No burndown data."


def test_burndown_report_to_json(improving_events):
    result = compute_burndown("pipe-a", improving_events)
    payload = burndown_report_to_json([result])
    import json
    data = json.loads(payload)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe-a"
