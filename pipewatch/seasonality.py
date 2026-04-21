"""Seasonality detection for pipeline metrics.

Detects periodic patterns (hourly, daily, weekly) in a pipeline's
success-rate history so operators can distinguish expected cyclical
behaviour from genuine regressions.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import PipelineHistory


# Minimum snapshots required to attempt any seasonality analysis.
_MIN_SNAPSHOTS = 8


@dataclass
class SeasonalityResult:
    """Outcome of a seasonality analysis for one pipeline."""

    pipeline: str
    metric: str  # e.g. "success_rate"
    period_hours: Optional[float]  # dominant period detected, or None
    strength: float  # 0.0 – 1.0; proportion of variance explained by period
    label: str  # human-readable description
    note: Optional[str] = None
    sufficient_data: bool = True

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "period_hours": self.period_hours,
            "strength": round(self.strength, 4),
            "label": self.label,
            "note": self.note,
            "sufficient_data": self.sufficient_data,
        }


def _get_values(history: PipelineHistory, metric: str) -> List[float]:
    """Extract a time-ordered list of metric values from history."""
    snapshots = history.last_n(len(history.snapshots))
    out: List[float] = []
    for snap in snapshots:
        val = snap.to_dict().get(metric)
        if val is not None:
            out.append(float(val))
    return out


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _autocorrelation(values: List[float], lag: int) -> float:
    """Compute the Pearson autocorrelation of *values* at the given *lag*.

    Returns a value in [-1, 1].  Returns 0.0 when the denominator is zero
    (constant series or too-short series).
    """
    n = len(values)
    if lag >= n:
        return 0.0
    mu = _mean(values)
    numerator = sum(
        (values[i] - mu) * (values[i - lag] - mu) for i in range(lag, n)
    )
    denominator = sum((v - mu) ** 2 for v in values)
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


def _dominant_period(
    values: List[float],
    interval_seconds: float,
    candidate_lags: Optional[List[int]] = None,
) -> tuple[Optional[int], float]:
    """Return (best_lag, strength) for the lag with highest autocorrelation.

    *interval_seconds* is the average time between consecutive snapshots.
    *candidate_lags* defaults to lags 1 … len(values)//2.
    Returns (None, 0.0) when no meaningful periodicity is found.
    """
    max_lag = len(values) // 2
    if max_lag < 1:
        return None, 0.0

    lags = candidate_lags or list(range(1, max_lag + 1))
    best_lag: Optional[int] = None
    best_r = 0.0

    for lag in lags:
        r = _autocorrelation(values, lag)
        if r > best_r:
            best_r = r
            best_lag = lag

    return best_lag, best_r


def _label_period(period_hours: float) -> str:
    """Return a human-readable label for a detected period."""
    if period_hours < 1.5:
        return "sub-hourly"
    if period_hours < 6:
        return f"{period_hours:.1f}-hour cycle"
    if 20 <= period_hours <= 28:
        return "daily cycle"
    if 140 <= period_hours <= 196:
        return "weekly cycle"
    return f"{period_hours:.1f}-hour cycle"


def detect_seasonality(
    history: PipelineHistory,
    metric: str = "success_rate",
    strength_threshold: float = 0.35,
) -> Optional[SeasonalityResult]:
    """Analyse *history* for periodic patterns in *metric*.

    Parameters
    ----------
    history:
        A ``PipelineHistory`` instance containing timestamped snapshots.
    metric:
        The snapshot field to analyse (default: ``"success_rate"``).
    strength_threshold:
        Minimum autocorrelation strength to consider a period meaningful.
        Values below this produce a "no seasonality detected" result.

    Returns
    -------
    ``SeasonalityResult`` or ``None`` if the history has no snapshots.
    """
    snaps = history.last_n(len(history.snapshots))
    if not snaps:
        return None

    pipeline = snaps[0].pipeline

    if len(snaps) < _MIN_SNAPSHOTS:
        return SeasonalityResult(
            pipeline=pipeline,
            metric=metric,
            period_hours=None,
            strength=0.0,
            label="insufficient data",
            note=f"need at least {_MIN_SNAPSHOTS} snapshots, have {len(snaps)}",
            sufficient_data=False,
        )

    values = _get_values(history, metric)
    if len(values) < _MIN_SNAPSHOTS:
        return SeasonalityResult(
            pipeline=pipeline,
            metric=metric,
            period_hours=None,
            strength=0.0,
            label="insufficient data",
            note=f"metric '{metric}' absent from most snapshots",
            sufficient_data=False,
        )

    # Estimate the average interval between snapshots in seconds.
    timestamps = [s.timestamp for s in snaps]
    intervals = [
        (timestamps[i] - timestamps[i - 1]).total_seconds()
        for i in range(1, len(timestamps))
    ]
    avg_interval_seconds = _mean(intervals) if intervals else 3600.0

    best_lag, strength = _dominant_period(values, avg_interval_seconds)

    if best_lag is None or strength < strength_threshold:
        return SeasonalityResult(
            pipeline=pipeline,
            metric=metric,
            period_hours=None,
            strength=strength,
            label="no seasonality detected",
        )

    period_seconds = best_lag * avg_interval_seconds
    period_hours = period_seconds / 3600.0
    label = _label_period(period_hours)

    return SeasonalityResult(
        pipeline=pipeline,
        metric=metric,
        period_hours=round(period_hours, 2),
        strength=strength,
        label=label,
    )
