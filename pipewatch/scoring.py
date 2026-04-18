"""Pipeline health scoring — computes a composite 0-100 score for a pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class ScoringWeights:
    success_rate: float = 0.6
    throughput: float = 0.25
    error_rate: float = 0.15

    def validate(self) -> None:
        total = self.success_rate + self.throughput + self.error_rate
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"Weights must sum to 1.0, got {total:.4f}")
        for name, value in [
            ("success_rate", self.success_rate),
            ("throughput", self.throughput),
            ("error_rate", self.error_rate),
        ]:
            if value < 0:
                raise ValueError(f"Weight '{name}' must be non-negative, got {value}")


@dataclass
class ScoreResult:
    pipeline: str
    score: float  # 0-100
    grade: str
    breakdown: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "score": round(self.score, 2),
            "grade": self.grade,
            "breakdown": {k: round(v, 4) for k, v in self.breakdown.items()},
        }


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


def _normalise_throughput(value: float, ceiling: float = 10_000.0) -> float:
    """Map throughput rows/s to a 0-1 scale capped at *ceiling*."""
    if ceiling <= 0:
        return 0.0
    return min(value / ceiling, 1.0)


def score_metric(
    metric: PipelineMetric,
    weights: Optional[ScoringWeights] = None,
    throughput_ceiling: float = 10_000.0,
) -> ScoreResult:
    if weights is None:
        weights = ScoringWeights()
    weights.validate()

    sr = metric.success_rate() if metric.total_rows > 0 else 0.0
    tp = _normalise_throughput(metric.throughput(), throughput_ceiling)
    er = 1.0 - sr  # error contribution (lower is better, so invert)

    sr_contrib = sr * weights.success_rate * 100
    tp_contrib = tp * weights.throughput * 100
    er_contrib = (1.0 - er) * weights.error_rate * 100

    score = sr_contrib + tp_contrib + er_contrib

    return ScoreResult(
        pipeline=metric.pipeline_name,
        score=score,
        grade=_grade(score),
        breakdown={
            "success_rate": sr_contrib,
            "throughput": tp_contrib,
            "error_rate": er_contrib,
        },
    )
