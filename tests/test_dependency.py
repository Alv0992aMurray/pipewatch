"""Tests for pipewatch.dependency and pipewatch.dependency_reporter."""
import json
import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.dependency import (
    DependencyEdge,
    build_graph,
    check_dependencies,
    check_all_dependencies,
)
from pipewatch.dependency_reporter import (
    format_dependency_result,
    format_dependency_report,
    dependency_report_to_json,
)


@pytest.fixture()
def healthy_metric() -> PipelineMetric:
    return PipelineMetric(pipeline="up_ok", rows_processed=100, rows_failed=0)


@pytest.fixture()
def failing_metric() -> PipelineMetric:
    return PipelineMetric(pipeline="up_bad", rows_processed=100, rows_failed=60)


# ---------------------------------------------------------------------------
# build_graph
# ---------------------------------------------------------------------------

def test_build_graph_single_edge():
    edges = [DependencyEdge(upstream="A", downstream="B")]
    graph = build_graph(edges)
    assert graph == {"B": ["A"]}


def test_build_graph_multiple_upstreams():
    edges = [
        DependencyEdge(upstream="A", downstream="C"),
        DependencyEdge(upstream="B", downstream="C"),
    ]
    graph = build_graph(edges)
    assert set(graph["C"]) == {"A", "B"}


# ---------------------------------------------------------------------------
# check_dependencies
# ---------------------------------------------------------------------------

def test_no_upstreams_returns_none(healthy_metric):
    graph = build_graph([])
    result = check_dependencies("some_pipeline", {"up_ok": healthy_metric}, graph)
    assert result is None


def test_all_healthy_upstreams_not_blocked(healthy_metric):
    edges = [DependencyEdge(upstream="up_ok", downstream="down")]
    graph = build_graph(edges)
    result = check_dependencies("down", {"up_ok": healthy_metric}, graph)
    assert result is not None
    assert not result.is_blocked
    assert result.blocked_by == []


def test_unhealthy_upstream_blocks_downstream(failing_metric):
    edges = [DependencyEdge(upstream="up_bad", downstream="down")]
    graph = build_graph(edges)
    result = check_dependencies("down", {"up_bad": failing_metric}, graph)
    assert result is not None
    assert result.is_blocked
    assert "up_bad" in result.blocked_by


def test_missing_upstream_metric_not_counted_as_blocked():
    """If an upstream has no metric yet, we cannot call it unhealthy."""
    edges = [DependencyEdge(upstream="unknown", downstream="down")]
    graph = build_graph(edges)
    result = check_dependencies("down", {}, graph)
    assert result is not None
    assert not result.is_blocked


def test_check_all_dependencies_returns_results(healthy_metric, failing_metric):
    edges = [
        DependencyEdge(upstream="up_ok", downstream="d1"),
        DependencyEdge(upstream="up_bad", downstream="d2"),
    ]
    graph = build_graph(edges)
    metrics = {"up_ok": healthy_metric, "up_bad": failing_metric}
    results = check_all_dependencies(metrics, graph)
    assert len(results) == 2
    blocked = [r for r in results if r.is_blocked]
    assert len(blocked) == 1
    assert blocked[0].pipeline == "d2"


# ---------------------------------------------------------------------------
# reporter
# ---------------------------------------------------------------------------

def test_format_dependency_result_not_blocked(healthy_metric):
    edges = [DependencyEdge(upstream="up_ok", downstream="down")]
    graph = build_graph(edges)
    result = check_dependencies("down", {"up_ok": healthy_metric}, graph)
    text = format_dependency_result(result)
    assert "healthy" in text
    assert "\u2705" in text


def test_format_dependency_result_blocked(failing_metric):
    edges = [DependencyEdge(upstream="up_bad", downstream="down")]
    graph = build_graph(edges)
    result = check_dependencies("down", {"up_bad": failing_metric}, graph)
    text = format_dependency_result(result)
    assert "up_bad" in text
    assert "\u274c" in text


def test_format_dependency_report_empty():
    text = format_dependency_report([])
    assert "No dependency" in text


def test_dependency_report_to_json(healthy_metric):
    edges = [DependencyEdge(upstream="up_ok", downstream="down")]
    graph = build_graph(edges)
    result = check_dependencies("down", {"up_ok": healthy_metric}, graph)
    payload = json.loads(dependency_report_to_json([result]))
    assert isinstance(payload, list)
    assert payload[0]["pipeline"] == "down"
    assert payload[0]["is_blocked"] is False
