"""Metric value normalization for cross-pipeline comparison."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class NormalizationConfig:
    success_rate_min: float = 0.0
    success_rate_max: float = 1.0
    throughput_min: float = 0.0
    throughput_ceiling: float = 10_000.0
    error_rate_min: float = 0.0
    error_rate_max: float = 1.0


@dataclass
class NormalizedMetric:
    pipeline: str
    success_rate: Optional[float]
    throughput: Optional[float]
    error_rate: Optional[float]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "success_rate": self.success_rate,
            "throughput": self.throughput,
            "error_rate": self.error_rate,
        }


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _normalize(value: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return _clamp((value - lo) / (hi - lo), 0.0, 1.0)


def normalize_metric(
    metric: PipelineMetric,
    config: Optional[NormalizationConfig] = None,
) -> NormalizedMetric:
    cfg = config or NormalizationConfig()

    total = metric.rows_processed
    success_rate: Optional[float] = None
    error_rate: Optional[float] = None
    if total and total > 0:
        raw_sr = metric.rows_passed / total
        success_rate = _normalize(raw_sr, cfg.success_rate_min, cfg.success_rate_max)
        raw_er = metric.rows_failed / total
        error_rate = _normalize(raw_er, cfg.error_rate_min, cfg.error_rate_max)

    throughput: Optional[float] = None
    if metric.duration_seconds and metric.duration_seconds > 0 and total:
        raw_tp = total / metric.duration_seconds
        throughput = _normalize(raw_tp, cfg.throughput_min, cfg.throughput_ceiling)

    return NormalizedMetric(
        pipeline=metric.pipeline_name,
        success_rate=success_rate,
        throughput=throughput,
        error_rate=error_rate,
    )


def normalize_metrics(
    metrics: List[PipelineMetric],
    config: Optional[NormalizationConfig] = None,
) -> List[NormalizedMetric]:
    return [normalize_metric(m, config) for m in metrics]
