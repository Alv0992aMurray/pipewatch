"""Pipeline ranking — score and rank pipelines by overall health."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, success_rate, throughput, is_healthy


@dataclass
class RankEntry:
    pipeline: str
    score: float  # 0.0 – 1.0
    rank: int
    healthy: bool
    success_rate: float
    throughput: float

    def to_dict(self) -> dict:
        return {
            "rank": self.rank,
            "pipeline": self.pipeline,
            "score": round(self.score, 4),
            "healthy": self.healthy,
            "success_rate": round(self.success_rate, 4),
            "throughput": self.throughput,
        }


@dataclass
class RankingResult:
    entries: List[RankEntry] = field(default_factory=list)

    def top(self, n: int = 5) -> List[RankEntry]:
        return self.entries[:n]

    def bottom(self, n: int = 5) -> List[RankEntry]:
        return self.entries[-n:]

    def to_dict(self) -> dict:
        return {"rankings": [e.to_dict() for e in self.entries]}


def _score(metric: PipelineMetric, throughput_ceiling: float) -> float:
    sr = success_rate(metric)
    tp = throughput(metric)
    tp_norm = min(tp / throughput_ceiling, 1.0) if throughput_ceiling > 0 else 0.0
    return round(0.7 * sr + 0.3 * tp_norm, 6)


def rank_pipelines(
    metrics: List[PipelineMetric],
    throughput_ceiling: float = 1000.0,
) -> RankingResult:
    """Rank pipelines from best to worst by composite health score."""
    if not metrics:
        return RankingResult()

    scored = [
        (
            m,
            _score(m, throughput_ceiling),
            success_rate(m),
            throughput(m),
        )
        for m in metrics
    ]
    scored.sort(key=lambda x: x[1], reverse=True)

    entries = [
        RankEntry(
            pipeline=m.pipeline,
            score=score,
            rank=idx + 1,
            healthy=is_healthy(m),
            success_rate=sr,
            throughput=tp,
        )
        for idx, (m, score, sr, tp) in enumerate(scored)
    ]
    return RankingResult(entries=entries)
