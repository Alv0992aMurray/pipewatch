"""SLA (Service Level Agreement) tracking for pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import PipelineHistory, MetricSnapshot


@dataclass
class SLAConfig:
    pipeline: str
    min_success_rate: float  # e.g. 0.95 for 95%
    max_error_rate: float    # e.g. 0.05 for 5%
    window: int = 10         # number of recent snapshots to evaluate


@dataclass
class SLAViolation:
    pipeline: str
    metric: str
    threshold: float
    actual: float
    message: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "threshold": self.threshold,
            "actual": round(self.actual, 4),
            "message": self.message,
        }


@dataclass
class SLAResult:
    pipeline: str
    config: SLAConfig
    violations: List[SLAViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "passed": self.passed,
            "violations": [v.to_dict() for v in self.violations],
        }


def evaluate_sla(history: PipelineHistory, config: SLAConfig) -> Optional[SLAResult]:
    """Evaluate SLA compliance over the last N snapshots."""
    snapshots: List[MetricSnapshot] = history.last_n(config.window)
    if not snapshots:
        return None

    avg_success = sum(s.success_rate for s in snapshots) / len(snapshots)
    avg_error = sum(s.error_rate for s in snapshots) / len(snapshots)

    violations: List[SLAViolation] = []

    if avg_success < config.min_success_rate:
        violations.append(SLAViolation(
            pipeline=config.pipeline,
            metric="success_rate",
            threshold=config.min_success_rate,
            actual=avg_success,
            message=(
                f"Average success rate {avg_success:.2%} is below "
                f"SLA minimum of {config.min_success_rate:.2%}"
            ),
        ))

    if avg_error > config.max_error_rate:
        violations.append(SLAViolation(
            pipeline=config.pipeline,
            metric="error_rate",
            threshold=config.max_error_rate,
            actual=avg_error,
            message=(
                f"Average error rate {avg_error:.2%} exceeds "
                f"SLA maximum of {config.max_error_rate:.2%}"
            ),
        ))

    return SLAResult(pipeline=config.pipeline, config=config, violations=violations)
