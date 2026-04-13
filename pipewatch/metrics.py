"""Core metrics collection and representation for ETL pipeline health."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class PipelineMetric:
    """Represents a single health metric snapshot for an ETL pipeline."""

    pipeline_name: str
    rows_processed: int
    rows_failed: int
    duration_seconds: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    stage: Optional[str] = None

    @property
    def success_rate(self) -> float:
        """Calculate the percentage of successfully processed rows."""
        total = self.rows_processed + self.rows_failed
        if total == 0:
            return 100.0
        return round((self.rows_processed / total) * 100, 2)

    @property
    def throughput(self) -> float:
        """Calculate rows processed per second."""
        if self.duration_seconds <= 0:
            return 0.0
        return round(self.rows_processed / self.duration_seconds, 2)

    def is_healthy(self, min_success_rate: float = 95.0) -> bool:
        """Determine if the pipeline metric meets the health threshold."""
        return self.success_rate >= min_success_rate

    def to_dict(self) -> dict:
        """Serialize metric to a plain dictionary."""
        return {
            "pipeline_name": self.pipeline_name,
            "stage": self.stage,
            "rows_processed": self.rows_processed,
            "rows_failed": self.rows_failed,
            "duration_seconds": self.duration_seconds,
            "success_rate": self.success_rate,
            "throughput": self.throughput,
            "timestamp": self.timestamp.isoformat(),
            "healthy": self.is_healthy(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineMetric":
        """Deserialize a metric from a plain dictionary.

        Args:
            data: A dictionary as produced by ``to_dict``.

        Returns:
            A ``PipelineMetric`` instance populated from the given data.

        Raises:
            KeyError: If a required field is missing from *data*.
            ValueError: If ``timestamp`` cannot be parsed as an ISO 8601 string.
        """
        return cls(
            pipeline_name=data["pipeline_name"],
            rows_processed=data["rows_processed"],
            rows_failed=data["rows_failed"],
            duration_seconds=data["duration_seconds"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            stage=data.get("stage"),
        )
