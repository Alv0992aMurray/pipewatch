"""Volatility detection: measures variability in success_rate over a history window."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.history import PipelineHistory

_MIN_SAMPLES = 4


@dataclass
class VolatilityResult:
    pipeline: str
    metric: str
    sample_count: int
    mean: float
    std_dev: float
    coefficient_of_variation: float  # std_dev / mean (0-1 scale)
    is_volatile: bool
    insufficient_data: bool
    threshold: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "sample_count": self.sample_count,
            "mean": round(self.mean, 4),
            "std_dev": round(self.std_dev, 4),
            "coefficient_of_variation": round(self.coefficient_of_variation, 4),
            "is_volatile": self.is_volatile,
            "insufficient_data": self.insufficient_data,
            "threshold": self.threshold,
        }


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _std_dev(values: list[float], mean: float) -> float:
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return variance ** 0.5


def _get_values(history: PipelineHistory, metric: str) -> list[float]:
    snaps = history.last_n(len(history.snapshots))
    result = []
    for s in snaps:
        val = s.success_rate if metric == "success_rate" else s.throughput
        if val is not None:
            result.append(val)
    return result


def detect_volatility(
    history: PipelineHistory,
    metric: str = "success_rate",
    threshold: float = 0.15,
) -> Optional[VolatilityResult]:
    """Compute coefficient of variation for *metric* over *history*.

    Returns None when history is empty.  Sets ``insufficient_data`` when fewer
    than ``_MIN_SAMPLES`` values are available.
    """
    values = _get_values(history, metric)
    if not values:
        return None

    insufficient = len(values) < _MIN_SAMPLES
    if insufficient:
        return VolatilityResult(
            pipeline=history.pipeline,
            metric=metric,
            sample_count=len(values),
            mean=_mean(values),
            std_dev=0.0,
            coefficient_of_variation=0.0,
            is_volatile=False,
            insufficient_data=True,
            threshold=threshold,
        )

    mu = _mean(values)
    sd = _std_dev(values, mu)
    cov = (sd / mu) if mu > 0 else 0.0

    return VolatilityResult(
        pipeline=history.pipeline,
        metric=metric,
        sample_count=len(values),
        mean=mu,
        std_dev=sd,
        coefficient_of_variation=cov,
        is_volatile=cov > threshold,
        insufficient_data=False,
        threshold=threshold,
    )
