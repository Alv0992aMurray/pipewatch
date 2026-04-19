"""Tests for pipewatch.breach_reporter."""
import json
from datetime import datetime
from pipewatch.breach import BreachConfig, BreachResult, BreachEvent
from pipewatch.breach_reporter import (
    format_breach_result,
    format_breach_report,
    breach_report_to_json,
)


def _cfg(metric="success_rate", threshold=0.95, direction="below"):
    return BreachConfig(metric=metric, threshold=threshold, direction=direction)


def _event(pipeline="pipe", value=0.80):
    return BreachEvent(
        pipeline=pipeline,
        metric="success_rate",
        value=value,
        threshold=0.95,
        direction="below",
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


def _result_no_breaches():
    return BreachResult(pipeline="pipe", config=_cfg())


def _result_with_breach():
    r = BreachResult(pipeline="pipe", config=_cfg())
    r.events.append(_event())
    return r


def test_format_result_no_breach_shows_checkmark():
    out = format_breach_result(_result_no_breaches())
    assert "✅" in out


def test_format_result_breach_shows_red_circle():
    out = format_breach_result(_result_with_breach())
    assert "🔴" in out


def test_format_result_contains_pipeline_name():
    out = format_breach_result(_result_with_breach())
    assert "pipe" in out


def test_format_result_shows_breach_count():
    out = format_breach_result(_result_with_breach())
    assert "1 breach" in out


def test_format_result_shows_latest_value():
    out = format_breach_result(_result_with_breach())
    assert "0.8000" in out


def test_format_report_empty_shows_message():
    out = format_breach_report([])
    assert "No breach" in out


def test_format_report_multiple_results():
    results = [_result_no_breaches(), _result_with_breach()]
    out = format_breach_report(results)
    assert out.count("pipe") >= 2


def test_breach_report_to_json_is_valid():
    results = [_result_with_breach()]
    raw = breach_report_to_json(results)
    data = json.loads(raw)
    assert isinstance(data, list)
    assert data[0]["total_breaches"] == 1
