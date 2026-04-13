"""Retention policy for pipeline history snapshots.

Provides configurable pruning of old MetricSnapshot entries from
PipelineHistory, keeping storage bounded during long-running sessions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from pipewatch.history import PipelineHistory, MetricSnapshot


@dataclass
class RetentionPolicy:
    """Defines how long or how many snapshots to retain.

    Attributes:
        max_snapshots: Maximum number of snapshots to keep per pipeline.
            Older entries beyond this count are dropped. ``None`` means
            no count-based limit.
        max_age_hours: Maximum age (in hours) of snapshots to retain.
            Snapshots older than this are pruned. ``None`` means no
            time-based limit.
    """

    max_snapshots: Optional[int] = None
    max_age_hours: Optional[float] = None

    def is_unlimited(self) -> bool:
        """Return True when no limits are configured."""
        return self.max_snapshots is None and self.max_age_hours is None


@dataclass
class PruneResult:
    """Summary of a prune operation.

    Attributes:
        pipeline_name: Name of the pipeline that was pruned.
        removed: Number of snapshots removed.
        retained: Number of snapshots kept.
    """

    pipeline_name: str
    removed: int
    retained: int

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"{self.pipeline_name}: removed {self.removed}, "
            f"retained {self.retained}"
        )


def _cutoff_time(max_age_hours: float) -> datetime:
    """Return the UTC datetime before which snapshots should be pruned."""
    return datetime.now(tz=timezone.utc) - timedelta(hours=max_age_hours)


def prune_history(
    history: PipelineHistory,
    policy: RetentionPolicy,
) -> PruneResult:
    """Apply *policy* to *history*, removing snapshots that exceed limits.

    When both ``max_snapshots`` and ``max_age_hours`` are set, a satisfy **both** constraints to be retained (i.e. the stricter
    rule wins).

    Args:
        history: The :class:`~pipewatch.history.PipelineHistory` to prune
            in-place.
        policy: The :class:`RetentionPolicy` to apply.

    Returns:
        A :class:`PruneResult` describing how many entries were kept or
        removed.
    """
    if policy.is_unlimited():
        return PruneResult(
            pipeline_name=history.pipeline_name,
            removed=0,
            retained=len(history.snapshots),
        )

    snapshots: list[MetricSnapshot] = list(history.snapshots)

    # Apply age-based filter first (oldest-first order assumed).
    if policy.max_age_hours is not None:
        cutoff = _cutoff_time(policy.max_age_hours)
        snapshots = [s for s in snapshots if s.timestamp >= cutoff]

    # Apply count-based limit, keeping the most recent entries.
    if policy.max_snapshots is not None and len(snapshots) > policy.max_snapshots:
        snapshots = snapshots[-policy.max_snapshots :]

    removed = len(history.snapshots) - len(snapshots)
    history.snapshots = snapshots

    return PruneResult(
        pipeline_name=history.pipeline_name,
        removed=removed,
        retained=len(snapshots),
    )


def prune_all(
    histories: list[PipelineHistory],
    policy: RetentionPolicy,
) -> list[PruneResult]:
    """Apply *policy* to every history in *histories*.

    Args:
        histories: Collection of pipeline histories to prune.
        policy: Shared retention policy applied to all histories.

    Returns:
        A list of :class:`PruneResult` objects, one per history.
    """
    return [prune_history(h, policy) for h in histories]
