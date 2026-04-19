"""Saturation detection: measures how close a pipeline's throughput is to its configured ceiling."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
from pipewatch.history import PipelineHistory


@dataclass
class SaturationConfig:
    pipeline: str
    ceiling: float  # max expected rows_processed
    warn_pct: float = 0.80  # warn when >= 80% of ceiling
    critical_pct: float = 0.95

    @property
    def window(self) -> int:
        return 10  # last N snapshots


@dataclass
class SaturationResult:
    pipeline: str
    avg_throughput: float
    ceiling: float
    utilisation: float  # 0.0 – 1.0
    level: str  # "ok", "warning", "critical"
    insufficient_data: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "avg_throughput": round(self.avg_throughput, 2),
            "ceiling": self.ceiling,
            "utilisation": round(self.utilisation, 4),
            "level": self.level,
            "insufficient_data": self.insufficient_data,
        }


def _get_throughputs(history: PipelineHistory, n: int) -> list[float]:
    snaps = history.last_n(n)
    return [s.rows_processed / max(s.duration_seconds, 1) for s in snaps if s.duration_seconds > 0]


def detect_saturation(
    history: PipelineHistory,
    config: SaturationConfig,
) -> Optional[SaturationResult]:
    values = _get_throughputs(history, config.window)
    if not values:
        return SaturationResult(
            pipeline=config.pipeline,
            avg_throughput=0.0,
            ceiling=config.ceiling,
            utilisation=0.0,
            level="ok",
            insufficient_data=True,
        )
    avg = sum(values) / len(values)
    util = avg / config.ceiling if config.ceiling > 0 else 0.0
    util = min(util, 1.0)
    if util >= config.critical_pct:
        level = "critical"
    elif util >= config.warn_pct:
        level = "warning"
    else:
        level = "ok"
    return SaturationResult(
        pipeline=config.pipeline,
        avg_throughput=avg,
        ceiling=config.ceiling,
        utilisation=util,
        level=level,
    )
