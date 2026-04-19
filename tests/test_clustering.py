"""Tests for pipewatch.clustering."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.clustering import cluster_metrics, ClusteringResult


def _metric(name: str, succeeded: int, failed: int, duration: float = 10.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        rows_processed=succeeded + failed,
        rows_succeeded=succeeded,
        rows_failed=failed,
        duration_seconds=duration,
    )


def test_empty_metrics_returns_none():
    assert cluster_metrics([]) is None


def test_single_healthy_metric_goes_to_healthy_cluster():
    result = cluster_metrics([_metric("pipe_a", 100, 0)])
    assert result is not None
    healthy = next(c for c in result.clusters if c.label == "healthy")
    assert healthy.count == 1
    assert healthy.entries[0].pipeline == "pipe_a"


def test_single_failing_metric_goes_to_failing_cluster():
    result = cluster_metrics([_metric("pipe_b", 10, 90)])
    assert result is not None
    failing = next(c for c in result.clusters if c.label == "failing")
    assert failing.count == 1


def test_degraded_metric_between_70_and_95():
    result = cluster_metrics([_metric("pipe_c", 80, 20)])
    assert result is not None
    degraded = next(c for c in result.clusters if c.label == "degraded")
    assert degraded.count == 1


def test_total_pipelines_sums_all_clusters():
    metrics = [
        _metric("a", 100, 0),
        _metric("b", 80, 20),
        _metric("c", 10, 90),
    ]
    result = cluster_metrics(metrics)
    assert result.total_pipelines == 3


def test_avg_success_rate_computed_correctly():
    metrics = [_metric("a", 95, 5), _metric("b", 100, 0)]
    result = cluster_metrics(metrics)
    healthy = next(c for c in result.clusters if c.label == "healthy")
    assert abs(healthy.avg_success_rate - 0.975) < 0.001


def test_to_dict_contains_expected_keys():
    result = cluster_metrics([_metric("x", 100, 0)])
    d = result.to_dict()
    assert "total_pipelines" in d
    assert "clusters" in d
    assert isinstance(d["clusters"], list)


def test_entry_to_dict_has_all_fields():
    result = cluster_metrics([_metric("z", 90, 10)])
    for cluster in result.clusters:
        for entry in cluster.entries:
            d = entry.to_dict()
            assert "pipeline" in d
            assert "success_rate" in d
            assert "throughput" in d
            assert "error_rate" in d


def test_zero_rows_does_not_raise():
    m = PipelineMetric(
        pipeline_name="empty",
        rows_processed=0,
        rows_succeeded=0,
        rows_failed=0,
        duration_seconds=5.0,
    )
    result = cluster_metrics([m])
    assert result is not None
    failing = next(c for c in result.clusters if c.label == "failing")
    assert failing.count == 1
