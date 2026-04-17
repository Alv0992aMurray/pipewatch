"""Aggregate health scoring across multiple pipelines."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric, is_healthy, success_rate, throughput


@dataclass
class PipelineHealthScore:
    pipeline: str
    success_rate: float
    throughput: float
    is_healthy: bool
    score: float  # 0.0 - 100.0
    grade: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "success_rate": round(self.success_rate, 4),
            "throughput": round(self.throughput, 2),
            "is_healthy": self.is_healthy,
            "score": round(self.score, 2),
            "grade": self.grade,
        }


@dataclass
class HealthScoreReport:
    scores: List[PipelineHealthScore] = field(default_factory=list)

    @property
    def average_score(self) -> Optional[float]:
        if not self.scores:
            return None
        return sum(s.score for s in self.scores) / len(self.scores)

    @property
    def healthy_count(self) -> int:
        return sum(1 for s in self.scores if s.is_healthy)

    @property
    def unhealthy_count(self) -> int:
        return len(self.scores) - self.healthy_count


def _grade(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def _compute_score(metric: PipelineMetric, throughput_ceiling: float = 1000.0) -> float:
    sr = success_rate(metric)
    tp = throughput(metric)
    tp_norm = min(tp / throughput_ceiling, 1.0) if throughput_ceiling > 0 else 0.0
    raw = sr * 0.75 + tp_norm * 0.25
    return raw * 100.0


def score_metric(metric: PipelineMetric, throughput_ceiling: float = 1000.0) -> PipelineHealthScore:
    sr = success_rate(metric)
    tp = throughput(metric)
    score = _compute_score(metric, throughput_ceiling)
    return PipelineHealthScore(
        pipeline=metric.pipeline,
        success_rate=sr,
        throughput=tp,
        is_healthy=is_healthy(metric),
        score=score,
        grade=_grade(score),
    )


def build_health_score_report(
    metrics: List[PipelineMetric],
    throughput_ceiling: float = 1000.0,
) -> HealthScoreReport:
    scores = [score_metric(m, throughput_ceiling) for m in metrics]
    return HealthScoreReport(scores=scores)
