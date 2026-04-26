"""Plateau detection for pipeline metrics.

A plateau occurs when a metric's values stop changing meaningfully over a
sustained window — distinct from stagnation (which tracks absolute level)
and ceiling (which tracks proximity to a maximum).  Here we care only about
the *rate of change* flattening out, which can signal a pipeline that has
stopped processing new data or is stuck in a steady-error loop.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import PipelineHistory, MetricSnapshot


@dataclass
class PlateauResult:
    """Result of a plateau detection check for a single pipeline."""

    pipeline: str
    metric: str
    is_plateau: bool
    # Absolute range (max - min) observed across the window.
    observed_range: Optional[float]
    # Minimum range threshold below which we declare a plateau.
    threshold: float
    # Number of snapshots analysed.
    sample_count: int
    # Human-readable note explaining the outcome.
    note: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "is_plateau": self.is_plateau,
            "observed_range": self.observed_range,
            "threshold": self.threshold,
            "sample_count": self.sample_count,
            "note": self.note,
        }


def _get_values(snapshots: List[MetricSnapshot], metric: str) -> List[float]:
    """Extract numeric values for *metric* from a list of snapshots."""
    out: List[float] = []
    for snap in snapshots:
        val = snap.to_dict().get(metric)
        if val is not None:
            try:
                out.append(float(val))
            except (TypeError, ValueError):
                pass
    return out


def detect_plateau(
    history: PipelineHistory,
    metric: str = "success_rate",
    min_samples: int = 5,
    range_threshold: float = 0.01,
) -> Optional[PlateauResult]:
    """Detect whether *metric* values in *history* have plateaued.

    A plateau is declared when the observed range (max − min) of the most
    recent *min_samples* values is smaller than *range_threshold*.

    Parameters
    ----------
    history:
        Pipeline history containing snapshots.
    metric:
        The metric field to inspect (e.g. ``"success_rate"``,
        ``"throughput"``).
    min_samples:
        Minimum number of data points required before a verdict is returned.
        Returns ``None`` when fewer samples are available.
    range_threshold:
        Values whose max-minus-min falls below this are considered a plateau.
        Defaults to ``0.01`` (1 percentage-point for rates expressed as 0–1).

    Returns
    -------
    PlateauResult or None
        ``None`` when there is insufficient data.
    """
    snapshots = history.last_n(min_samples)
    values = _get_values(snapshots, metric)

    if len(values) < min_samples:
        return None

    observed_range = max(values) - min(values)
    is_plateau = observed_range < range_threshold

    if is_plateau:
        note = (
            f"Values have not changed by more than {range_threshold:.4f} "
            f"across the last {len(values)} snapshots (range={observed_range:.6f})."
        )
    else:
        note = (
            f"Values are changing normally "
            f"(range={observed_range:.6f} >= threshold={range_threshold:.4f})."
        )

    return PlateauResult(
        pipeline=history.pipeline,
        metric=metric,
        is_plateau=is_plateau,
        observed_range=observed_range,
        threshold=range_threshold,
        sample_count=len(values),
        note=note,
    )
