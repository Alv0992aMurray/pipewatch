"""Correlation analysis between two pipeline metric histories."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.history import PipelineHistory


@dataclass
class CorrelationResult:
    pipeline_a: str
    pipeline_b: str
    metric: str
    n: int
    coefficient: Optional[float]  # Pearson r, None when insufficient data
    interpretation: str

    def to_dict(self) -> dict:
        return {
            "pipeline_a": self.pipeline_a,
            "pipeline_b": self.pipeline_b,
            "metric": self.metric,
            "n": self.n,
            "coefficient": self.coefficient,
            "interpretation": self.interpretation,
        }


def _get_values(history: PipelineHistory, metric: str) -> list[float]:
    """Extract a named metric series from a history object."""
    result = []
    for snap in history.snapshots:
        val = snap.to_dict().get(metric)
        if val is not None:
            result.append(float(val))
    return result


def _pearson(xs: list[float], ys: list[float]) -> Optional[float]:
    """Compute Pearson correlation coefficient for two equal-length series."""
    n = len(xs)
    if n < 2:
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = sum((x - mean_x) ** 2 for x in xs) ** 0.5
    den_y = sum((y - mean_y) ** 2 for y in ys) ** 0.5
    if den_x == 0 or den_y == 0:
        return None
    return round(num / (den_x * den_y), 4)


def _interpret(r: Optional[float]) -> str:
    if r is None:
        return "insufficient data"
    abs_r = abs(r)
    direction = "positive" if r >= 0 else "negative"
    if abs_r >= 0.8:
        return f"strong {direction} correlation"
    if abs_r >= 0.5:
        return f"moderate {direction} correlation"
    if abs_r >= 0.2:
        return f"weak {direction} correlation"
    return "no meaningful correlation"


def correlate(
    history_a: PipelineHistory,
    history_b: PipelineHistory,
    metric: str = "success_rate",
) -> CorrelationResult:
    """Return a CorrelationResult comparing *metric* across two histories."""
    xs = _get_values(history_a, metric)
    ys = _get_values(history_b, metric)
    # Align to the shorter series (most-recent values)
    n = min(len(xs), len(ys))
    xs, ys = xs[-n:], ys[-n:]
    r = _pearson(xs, ys) if n >= 2 else None
    return CorrelationResult(
        pipeline_a=history_a.pipeline_name,
        pipeline_b=history_b.pipeline_name,
        metric=metric,
        n=n,
        coefficient=r,
        interpretation=_interpret(r),
    )
