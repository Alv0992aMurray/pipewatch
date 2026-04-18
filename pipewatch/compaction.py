"""Compaction: reduce a PipelineHistory by merging older snapshots into summary buckets."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.history import MetricSnapshot, PipelineHistory


@dataclass
class CompactionBucket:
    start: datetime
    end: datetime
    snapshot_count: int
    avg_success_rate: float
    avg_throughput: float
    any_unhealthy: bool

    def to_dict(self) -> dict:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "snapshot_count": self.snapshot_count,
            "avg_success_rate": round(self.avg_success_rate, 4),
            "avg_throughput": round(self.avg_throughput, 4),
            "any_unhealthy": self.any_unhealthy,
        }


@dataclass
class CompactionResult:
    pipeline: str
    bucket_size_minutes: int
    buckets: List[CompactionBucket] = field(default_factory=list)
    retained_snapshots: List[MetricSnapshot] = field(default_factory=list)

    @property
    def total_buckets(self) -> int:
        return len(self.buckets)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "bucket_size_minutes": self.bucket_size_minutes,
            "total_buckets": self.total_buckets,
            "retained_snapshots": len(self.retained_snapshots),
            "buckets": [b.to_dict() for b in self.buckets],
        }


def compact_history(
    history: PipelineHistory,
    retain_minutes: int = 60,
    bucket_minutes: int = 15,
    now: Optional[datetime] = None,
) -> CompactionResult:
    """Compact snapshots older than *retain_minutes* into fixed-size buckets."""
    if now is None:
        now = datetime.utcnow()

    cutoff = now - timedelta(minutes=retain_minutes)
    all_snaps = history.last_n(len(history._snapshots))

    recent = [s for s in all_snaps if s.timestamp >= cutoff]
    older = [s for s in all_snaps if s.timestamp < cutoff]

    buckets: List[CompactionBucket] = []
    if older:
        bucket_delta = timedelta(minutes=bucket_minutes)
        oldest = min(s.timestamp for s in older)
        bucket_start = oldest
        while bucket_start < cutoff:
            bucket_end = bucket_start + bucket_delta
            group = [s for s in older if bucket_start <= s.timestamp < bucket_end]
            if group:
                buckets.append(
                    CompactionBucket(
                        start=bucket_start,
                        end=bucket_end,
                        snapshot_count=len(group),
                        avg_success_rate=sum(s.success_rate for s in group) / len(group),
                        avg_throughput=sum(s.throughput for s in group) / len(group),
                        any_unhealthy=any(not s.healthy for s in group),
                    )
                )
            bucket_start = bucket_end

    return CompactionResult(
        pipeline=history.pipeline,
        bucket_size_minutes=bucket_minutes,
        buckets=buckets,
        retained_snapshots=recent,
    )
