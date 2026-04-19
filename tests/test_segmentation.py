"""Tests for pipewatch.segmentation and pipewatch.segmentation_reporter."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.segmentation import (
    SegmentCriteria,
    segment_metrics,
)
from pipewatch.segmentation_reporter import (
    format_segmentation_report,
    segmentation_report_to_json,
)
import json


def _metric(name: str, tags: dict, rows: int = 100, failed: int = 0) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        rows_processed=rows,
        rows_failed=failed,
        latency_seconds=1.0,
        tags=tags,
    )


def test_criteria_matches_when_tag_present():
    c = SegmentCriteria(tag_key="env", tag_value="prod")
    m = _metric("p", {"env": "prod"})
    assert c.matches(m)


def test_criteria_rejects_wrong_tag_value():
    c = SegmentCriteria(tag_key="env", tag_value="prod")
    m = _metric("p", {"env": "staging"})
    assert not c.matches(m)


def test_criteria_matches_any_value_when_tag_value_is_none():
    c = SegmentCriteria(tag_key="env", tag_value=None)
    m = _metric("p", {"env": "anything"})
    assert c.matches(m)


def test_criteria_rejects_missing_tag():
    c = SegmentCriteria(tag_key="env")
    m = _metric("p", {})
    assert not c.matches(m)


def test_segment_metrics_places_metrics_correctly():
    prod = _metric("prod-pipe", {"env": "prod"})
    staging = _metric("staging-pipe", {"env": "staging"})
    criteria_map = {
        "production": SegmentCriteria("env", "prod"),
        "staging": SegmentCriteria("env", "staging"),
    }
    result = segment_metrics([prod, staging], criteria_map)
    assert result.get("production").count == 1
    assert result.get("staging").count == 1
    assert len(result.unmatched) == 0


def test_unmatched_metrics_collected():
    m = _metric("orphan", {})
    result = segment_metrics([m], {"prod": SegmentCriteria("env", "prod")})
    assert len(result.unmatched) == 1
    assert result.get("prod").count == 0


def test_segment_healthy_count():
    healthy = _metric("a", {"env": "prod"}, rows=100, failed=0)
    failing = _metric("b", {"env": "prod"}, rows=100, failed=60)
    result = segment_metrics([healthy, failing], {"prod": SegmentCriteria("env", "prod")})
    seg = result.get("prod")
    assert seg.healthy_count == 1


def test_segment_avg_success_rate():
    m1 = _metric("a", {"env": "prod"}, rows=100, failed=0)
    m2 = _metric("b", {"env": "prod"}, rows=100, failed=50)
    result = segment_metrics([m1, m2], {"prod": SegmentCriteria("env", "prod")})
    seg = result.get("prod")
    assert abs(seg.avg_success_rate - 0.75) < 1e-6


def test_total_metrics_includes_unmatched():
    m1 = _metric("a", {"env": "prod"})
    m2 = _metric("b", {})
    result = segment_metrics([m1, m2], {"prod": SegmentCriteria("env", "prod")})
    assert result.total_metrics == 2


def test_format_segmentation_report_contains_segment_name():
    m = _metric("a", {"env": "prod"})
    result = segment_metrics([m], {"production": SegmentCriteria("env", "prod")})
    report = format_segmentation_report(result)
    assert "production" in report


def test_format_segmentation_report_no_segments():
    from pipewatch.segmentation import SegmentationResult
    result = SegmentationResult(segments=[])
    assert "No segments" in format_segmentation_report(result)


def test_segmentation_report_to_json_structure():
    m = _metric("a", {"env": "prod"})
    result = segment_metrics([m], {"prod": SegmentCriteria("env", "prod")})
    data = json.loads(segmentation_report_to_json(result))
    assert "segments" in data
    assert data["total_metrics"] == 1
    assert data["segments"][0]["name"] == "prod"
