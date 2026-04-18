"""Tests for pipewatch.ranking and pipewatch.ranking_reporter."""
import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.ranking import rank_pipelines, RankingResult
from pipewatch.ranking_reporter import format_ranking_report, ranking_report_to_json


def _metric(pipeline: str, rows_in: int, rows_out: int, errors: int = 0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        rows_processed=rows_in,
        rows_passed=rows_out,
        rows_failed=errors,
        latency_seconds=1.0,
    )


def test_empty_metrics_returns_empty_result():
    result = rank_pipelines([])
    assert result.entries == []


def test_single_pipeline_is_rank_one():
    m = _metric("pipe_a", 100, 100)
    result = rank_pipelines([m])
    assert len(result.entries) == 1
    assert result.entries[0].rank == 1
    assert result.entries[0].pipeline == "pipe_a"


def test_healthier_pipeline_ranks_higher():
    good = _metric("good", 100, 100, errors=0)
    bad = _metric("bad", 100, 10, errors=90)
    result = rank_pipelines([bad, good])
    assert result.entries[0].pipeline == "good"
    assert result.entries[1].pipeline == "bad"


def test_ranks_are_sequential():
    metrics = [_metric(f"pipe_{i}", 100, 100 - i * 10) for i in range(5)]
    result = rank_pipelines(metrics)
    ranks = [e.rank for e in result.entries]
    assert ranks == list(range(1, 6))


def test_top_returns_correct_slice():
    metrics = [_metric(f"pipe_{i}", 100, 100 - i * 5) for i in range(10)]
    result = rank_pipelines(metrics)
    top3 = result.top(3)
    assert len(top3) == 3
    assert top3[0].rank == 1


def test_bottom_returns_worst():
    metrics = [_metric(f"pipe_{i}", 100, 100 - i * 5) for i in range(10)]
    result = rank_pipelines(metrics)
    bottom2 = result.bottom(2)
    assert bottom2[-1].rank == 10


def test_score_between_zero_and_one():
    m = _metric("pipe", 200, 180, errors=20)
    result = rank_pipelines([m])
    assert 0.0 <= result.entries[0].score <= 1.0


def test_format_ranking_report_empty():
    result = RankingResult()
    report = format_ranking_report(result)
    assert "no pipelines" in report


def test_format_ranking_report_contains_pipeline_name():
    m = _metric("my_pipeline", 100, 95)
    result = rank_pipelines([m])
    report = format_ranking_report(result)
    assert "my_pipeline" in report


def test_format_ranking_report_limit():
    metrics = [_metric(f"pipe_{i}", 100, 100 - i) for i in range(10)]
    result = rank_pipelines(metrics)
    report = format_ranking_report(result, limit=3)
    assert "pipe_0" in report
    assert "pipe_9" not in report


def test_ranking_report_to_json_is_valid():
    import json
    m = _metric("json_pipe", 50, 50)
    result = rank_pipelines([m])
    data = json.loads(ranking_report_to_json(result))
    assert "rankings" in data
    assert data["rankings"][0]["pipeline"] == "json_pipe"
