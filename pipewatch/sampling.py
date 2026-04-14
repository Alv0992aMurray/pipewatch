"""Metric sampling: collect and downsample pipeline metrics over a window."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.history import MetricSnapshot


@dataclass
class SamplingConfig:
    """Configuration for a sampling window."""
    pipeline_name: str
    window_seconds: int = 300          # default: 5-minute window
    max_samples: int = 10              # max samples to retain in window

    @property
    def window(self) -> timedelta:
        return timedelta(seconds=self.window_seconds)


@dataclass
class SampleWindow:
    """Holds a bounded, time-windowed list of metric snapshots."""
    config: SamplingConfig
    _samples: List[MetricSnapshot] = field(default_factory=list, repr=False)

    def add(self, snapshot: MetricSnapshot) -> None:
        """Add a snapshot, pruning expired and excess samples."""
        self._samples.append(snapshot)
        self._prune(snapshot.timestamp)

    def _prune(self, now: datetime) -> None:
        cutoff = now - self.config.window
        self._samples = [
            s for s in self._samples if s.timestamp >= cutoff
        ]
        if len(self._samples) > self.config.max_samples:
            self._samples = self._samples[-self.config.max_samples :]

    def samples(self) -> List[MetricSnapshot]:
        return list(self._samples)

    def average_success_rate(self) -> Optional[float]:
        """Mean success rate across all samples in the window."""
        rates = [s.success_rate for s in self._samples if s.success_rate is not None]
        if not rates:
            return None
        return sum(rates) / len(rates)

    def average_throughput(self) -> Optional[float]:
        """Mean throughput across all samples in the window."""
        values = [s.throughput for s in self._samples if s.throughput is not None]
        if not values:
            return None
        return sum(values) / len(values)

    def is_empty(self) -> bool:
        return len(self._samples) == 0

    def to_dict(self) -> dict:
        return {
            "pipeline": self.config.pipeline_name,
            "window_seconds": self.config.window_seconds,
            "sample_count": len(self._samples),
            "average_success_rate": self.average_success_rate(),
            "average_throughput": self.average_throughput(),
        }
