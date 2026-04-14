"""Checkpoint tracking: record and compare pipeline run timestamps."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


@dataclass
class CheckpointEntry:
    pipeline: str
    last_run: datetime
    run_count: int = 0
    last_status: str = "unknown"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_run": self.last_run.isoformat(),
            "run_count": self.run_count,
            "last_status": self.last_status,
        }

    @staticmethod
    def from_dict(data: dict) -> "CheckpointEntry":
        return CheckpointEntry(
            pipeline=data["pipeline"],
            last_run=datetime.fromisoformat(data["last_run"]),
            run_count=data.get("run_count", 0),
            last_status=data.get("last_status", "unknown"),
        )


@dataclass
class CheckpointStore:
    _entries: Dict[str, CheckpointEntry] = field(default_factory=dict)

    def record(self, pipeline: str, status: str, at: Optional[datetime] = None) -> CheckpointEntry:
        """Record a pipeline run checkpoint."""
        ts = at or datetime.now(timezone.utc)
        existing = self._entries.get(pipeline)
        run_count = (existing.run_count + 1) if existing else 1
        entry = CheckpointEntry(
            pipeline=pipeline,
            last_run=ts,
            run_count=run_count,
            last_status=status,
        )
        self._entries[pipeline] = entry
        return entry

    def get(self, pipeline: str) -> Optional[CheckpointEntry]:
        return self._entries.get(pipeline)

    def all_entries(self) -> list[CheckpointEntry]:
        return list(self._entries.values())

    def seconds_since_last_run(self, pipeline: str, now: Optional[datetime] = None) -> Optional[float]:
        entry = self._entries.get(pipeline)
        if entry is None:
            return None
        ref = now or datetime.now(timezone.utc)
        return (ref - entry.last_run).total_seconds()

    def clear(self, pipeline: str) -> None:
        self._entries.pop(pipeline, None)
