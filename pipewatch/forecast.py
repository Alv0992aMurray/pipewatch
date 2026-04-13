"""Simple linear-regression forecast for pipeline success rates."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import PipelineHistory


@dataclass
class ForecastResult:
    pipeline: str
    horizon: int  # steps ahead
    predicted_rate: Optional[float]
    slope: Optional[float]
    intercept: Optional[float]
    data_points: int
    message: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "horizon": self.horizon,
            "predicted_rate": round(self.predicted_rate, 4) if self.predicted_rate is not None else None,
            "slope": round(self.slope, 6) if self.slope is not None else None,
            "intercept": round(self.intercept, 4) if self.intercept is not None else None,
            "data_points": self.data_points,
            "message": self.message,
        }

    @property
    def is_concerning(self) -> bool:
        """Return True if the predicted rate falls below 0.8 or the trend is sharply declining."""
        if self.predicted_rate is not None and self.predicted_rate < 0.8:
            return True
        if self.slope is not None and self.slope < -0.05:
            return True
        return False


def _linear_regression(xs: List[float], ys: List[float]):
    """Return (slope, intercept) for a simple OLS fit."""
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    ss_xx = sum((x - mean_x) ** 2 for x in xs)
    ss_xy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    if ss_xx == 0:
        return 0.0, mean_y
    slope = ss_xy / ss_xx
    intercept = mean_y - slope * mean_x
    return slope, intercept


def forecast(history: PipelineHistory, horizon: int = 1, min_points: int = 3) -> ForecastResult:
    """Forecast success_rate *horizon* steps into the future using OLS."""
    snapshots = history.last_n(50)
    n = len(snapshots)

    if n < min_points:
        return ForecastResult(
            pipeline=history.pipeline,
            horizon=horizon,
            predicted_rate=None,
            slope=None,
            intercept=None,
            data_points=n,
            message=f"Insufficient data ({n} points, need {min_points})",
        )

    xs = list(range(n))
    ys = [s.success_rate for s in snapshots]
    slope, intercept = _linear_regression(xs, ys)
    predicted = intercept + slope * (n - 1 + horizon)
    predicted = max(0.0, min(1.0, predicted))

    direction = "improving" if slope > 0.001 else ("declining" if slope < -0.001 else "stable")
    msg = (
        f"Trend is {direction}; predicted success rate in {horizon} step(s): "
        f"{predicted:.1%}"
    )
    return ForecastResult(
        pipeline=history.pipeline,
        horizon=horizon,
        predicted_rate=predicted,
        slope=slope,
        intercept=intercept,
        data_points=n,
        message=msg,
    )
