"""CLI helpers for the 'baseline' sub-commands (capture / compare)."""

from __future__ import annotations

from pathlib import Path
from typing import List

from pipewatch.metrics import PipelineMetric
from pipewatch.baseline import (
    DEFAULT_BASELINE_PATH,
    capture_baseline,
    compare_to_baseline,
    save_baseline,
    load_baseline,
    BaselineDelta,
)
from pipewatch.baseline_reporter import format_baseline_report, baseline_report_to_json


def cmd_capture(
    metric: PipelineMetric,
    path: Path = DEFAULT_BASELINE_PATH,
) -> str:
    """Capture the current metric as the new baseline and return a status message."""
    entry = capture_baseline(metric)
    save_baseline(entry, path=path)
    return (
        f"Baseline saved for '{metric.pipeline_id}': "
        f"success_rate={entry.success_rate:.2%}, "
        f"throughput={entry.throughput:.2f} rows/s"
    )


def cmd_compare(
    metrics: List[PipelineMetric],
    tolerance: float = 0.05,
    path: Path = DEFAULT_BASELINE_PATH,
    as_json: bool = False,
) -> str:
    """Compare a list of metrics against their stored baselines.

    Returns a formatted report string (plain text or JSON).
    """
    deltas: List[BaselineDelta] = []
    for metric in metrics:
        baseline = load_baseline(metric.pipeline_id, path=path)
        if baseline is None:
            continue
        deltas.append(compare_to_baseline(metric, baseline, tolerance=tolerance))

    if as_json:
        return baseline_report_to_json(deltas)
    return format_baseline_report(deltas)


def has_regressions(
    metrics: List[PipelineMetric],
    tolerance: float = 0.05,
    path: Path = DEFAULT_BASELINE_PATH,
) -> bool:
    """Return True if any metric has regressed beyond tolerance vs its baseline."""
    for metric in metrics:
        baseline = load_baseline(metric.pipeline_id, path=path)
        if baseline is None:
            continue
        delta = compare_to_baseline(metric, baseline, tolerance=tolerance)
        if delta.regressed:
            return True
    return False
