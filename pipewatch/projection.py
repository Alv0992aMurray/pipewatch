"""Project future metric values based on historical trends."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List
from pipewatch.history import PipelineHistory


@dataclass
class ProjectionPoint:
    step: int
    predicted_success_rate: float
    predicted_throughput: float

    def to_dict(self) -> dict:
        return {
            "step": self.step,
            "predicted_success_rate": round(self.predicted_success_rate, 4),
            "predicted_throughput": round(self.predicted_throughput, 4),
        }


@dataclass
class ProjectionResult:
    pipeline: str
    steps: int
    points: List[ProjectionPoint] = field(default_factory=list)
    insufficient_data: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "steps": self.steps,
            "insufficient_data": self.insufficient_data,
            "points": [p.to_dict() for p in self.points],
        }


def _linear_extrapolate(values: List[float], steps: int) -> List[float]:
    n = len(values)
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    denom = sum((x - mean_x) ** 2 for x in xs)
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, values)) / denom if denom else 0.0
    intercept = mean_y - slope * mean_x
    return [max(0.0, min(1.0, slope * (n + i) + intercept)) for i in range(steps)]


def _extrapolate_throughput(values: List[float], steps: int) -> List[float]:
    n = len(values)
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    denom = sum((x - mean_x) ** 2 for x in xs)
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, values)) / denom if denom else 0.0
    intercept = mean_y - slope * mean_x
    return [max(0.0, slope * (n + i) + intercept) for i in range(steps)]


def project(history: PipelineHistory, steps: int = 3, min_snapshots: int = 3) -> Optional[ProjectionResult]:
    snapshots = history.last_n(50)
    if not snapshots:
        return None
    pipeline = snapshots[0].pipeline
    result = ProjectionResult(pipeline=pipeline, steps=steps)
    if len(snapshots) < min_snapshots:
        result.insufficient_data = True
        return result
    rates = [s.success_rate for s in snapshots]
    throughputs = [s.throughput for s in snapshots]
    projected_rates = _linear_extrapolate(rates, steps)
    projected_throughputs = _extrapolate_throughput(throughputs, steps)
    for i in range(steps):
        result.points.append(ProjectionPoint(
            step=i + 1,
            predicted_success_rate=projected_rates[i],
            predicted_throughput=projected_throughputs[i],
        ))
    return result
