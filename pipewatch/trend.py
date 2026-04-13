"""Analyse metric history to detect trends and regressions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.history import PipelineHistory


@dataclass
class TrendReport:
    """Summary of recent trend for a pipeline."""

    pipeline: str
    sample_size: int
    avg_success_rate: Optional[float]
    latest_success_rate: Optional[float]
    is_degrading: bool
    is_recovering: bool
    delta: Optional[float]  # latest - average of prior window

    def summary_line(self) -> str:
        if self.sample_size == 0:
            return f"{self.pipeline}: no history available"
        direction = (
            "degrading ↓"
            if self.is_degrading
            else ("recovering ↑" if self.is_recovering else "stable")
        )
        return (
            f"{self.pipeline}: success_rate={self.latest_success_rate:.2%} "
            f"(avg={self.avg_success_rate:.2%}, {direction})"
        )


def analyse_trend(
    history: PipelineHistory,
    window: int = 5,
    degradation_threshold: float = 0.05,
) -> TrendReport:
    """Compute a trend report from the last *window* snapshots.

    A pipeline is considered *degrading* when the latest success rate is more
    than ``degradation_threshold`` below the window average of prior entries.
    It is *recovering* when the opposite is true.
    """
    snapshots = history.last_n(window)
    sample_size = len(snapshots)

    if sample_size == 0:
        return TrendReport(
            pipeline=history.pipeline,
            sample_size=0,
            avg_success_rate=None,
            latest_success_rate=None,
            is_degrading=False,
            is_recovering=False,
            delta=None,
        )

    latest = snapshots[-1].success_rate

    if sample_size == 1:
        return TrendReport(
            pipeline=history.pipeline,
            sample_size=1,
            avg_success_rate=latest,
            latest_success_rate=latest,
            is_degrading=False,
            is_recovering=False,
            delta=0.0,
        )

    prior = snapshots[:-1]
    avg_prior = sum(s.success_rate for s in prior) / len(prior)
    delta = round(latest - avg_prior, 4)

    return TrendReport(
        pipeline=history.pipeline,
        sample_size=sample_size,
        avg_success_rate=round(avg_prior, 4),
        latest_success_rate=round(latest, 4),
        is_degrading=delta < -degradation_threshold,
        is_recovering=delta > degradation_threshold,
        delta=delta,
    )
