"""Tests for pipewatch.clustering_reporter."""
import json
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.clustering import cluster_metrics, Cluster, ClusteringResult
from pipewatch.clustering_reporter import (
    format_cluster,
    format_clustering_report,
    clustering_report_to_json,
)


def _metric(name: str, succeeded: int, failed: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        rows_processed=succeeded + failed,
        rows_succeeded=succeeded,
        rows_failed=failed,
        duration_seconds=10.0,
    )


def test_format_cluster_shows_label():
    result = cluster_metrics([_metric("pipe_a", 100, 0)])
    healthy = next(c for c in result.clusters if c.label == "healthy")
    text = format_cluster(healthy)
    assert "HEALTHY" in text


def test_format_cluster_lists_pipeline_name():
    result = cluster_metrics([_metric("my_pipeline", 100, 0)])
    healthy = next(c for c in result.clusters if c.label == "healthy")
    text = format_cluster(healthy)
    assert "my_pipeline" in text


def test_format_cluster_shows_success_rate():
    result = cluster_metrics([_metric("p", 80, 20)])
    degraded = next(c for c in result.clusters if c.label == "degraded")
    text = format_cluster(degraded)
    assert "%" in text


def test_format_report_empty_clusters_shows_message():
    empty = ClusteringResult(clusters=[])
    text = format_clustering_report(empty)
    assert "No pipelines" in text


def test_format_report_contains_pipeline_count():
    result = cluster_metrics([_metric("a", 100, 0), _metric("b", 10, 90)])
    text = format_clustering_report(result)
    assert "2" in text


def test_format_report_omits_empty_clusters():
    result = cluster_metrics([_metric("a", 100, 0)])
    text = format_clustering_report(result)
    assert "FAILING" not in text
    assert "DEGRADED" not in text


def test_clustering_report_to_json_is_valid():
    result = cluster_metrics([_metric("a", 95, 5)])
    raw = clustering_report_to_json(result)
    parsed = json.loads(raw)
    assert "clusters" in parsed
    assert "total_pipelines" in parsed
