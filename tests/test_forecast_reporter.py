"""Tests for pipewatch.forecast_reporter."""
import json
import pytest
from pipewatch.forecast import ForecastResult
from pipewatch.forecast_reporter import (
    format_forecast_result,
    format_forecast_report,
    forecast_report_to_json,
)


def _result(predicted=0.87, slope=0.005, pipeline="pipe", horizon=1, pts=10):
    return ForecastResult(
        pipeline=pipeline,
        horizon=horizon,
        predicted_rate=predicted,
        slope=slope,
        intercept=0.8,
        data_points=pts,
        message="Trend is improving",
    )


def _no_data_result(pipeline="pipe"):
    return ForecastResult(
        pipeline=pipeline,
        horizon=1,
        predicted_rate=None,
        slope=None,
        intercept=None,
        data_points=1,
        message="Insufficient data (1 points, need 3)",
    )


def test_format_result_rising():
    r = _result(slope=0.01)
    line = format_forecast_result(r)
    assert "↑" in line
    assert "87.0%" in line


def test_format_result_declining():
    r = _result(slope=-0.01)
    line = format_forecast_result(r)
    assert "↓" in line


def test_format_result_stable():
    r = _result(slope=0.0)
    line = format_forecast_result(r)
    assert "→" in line


def test_format_result_no_data():
    r = _no_data_result()
    line = format_forecast_result(r)
    assert "unavailable" in line
    assert "Insufficient" in line


def test_format_result_includes_pipeline_name():
    r = _result(pipeline="my-pipeline")
    line = format_forecast_result(r)
    assert "my-pipeline" in line


def test_format_report_empty():
    out = format_forecast_report([])
    assert "no pipelines" in out


def test_format_report_multiple():
    results = [_result(pipeline="a"), _result(pipeline="b")]
    out = format_forecast_report(results)
    assert "[a]" in out
    assert "[b]" in out
    assert "Forecast Report" in out


def test_to_json_is_valid():
    results = [_result(), _no_data_result(pipeline="empty")]
    raw = forecast_report_to_json(results)
    parsed = json.loads(raw)
    assert len(parsed) == 2
    assert parsed[0]["predicted_rate"] == pytest.approx(0.87, abs=0.001)
    assert parsed[1]["predicted_rate"] is None


def test_to_json_contains_pipeline_key():
    """Each JSON entry should include the pipeline name for identification."""
    results = [_result(pipeline="mypipe")]
    raw = forecast_report_to_json(results)
    parsed = json.loads(raw)
    assert parsed[0]["pipeline"] == "mypipe"
