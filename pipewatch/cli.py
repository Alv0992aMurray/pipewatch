"""CLI entry point for pipewatch using argparse."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.alert_config import load_rules
from pipewatch.metrics import PipelineMetric
from pipewatch.reporter import report_to_json
from pipewatch.runner import print_run_result, run_checks


DEFAULT_CONFIG = "pipewatch_alerts.yml"


def _build_metric_from_args(args: argparse.Namespace) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=args.pipeline,
        rows_in=args.rows_in,
        rows_out=args.rows_out,
        error_count=args.errors,
        duration_seconds=args.duration,
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor and alert on ETL pipeline health metrics.",
    )
    parser.add_argument("--pipeline", required=True, help="Pipeline name")
    parser.add_argument("--rows-in", type=int, required=True, dest="rows_in")
    parser.add_argument("--rows-out", type=int, required=True, dest="rows_out")
    parser.add_argument("--errors", type=int, default=0)
    parser.add_argument("--duration", type=float, default=0.0)
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="Alert config YAML")
    parser.add_argument("--json", action="store_true", dest="output_json",
                        help="Output report as JSON")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        rules = load_rules(args.config)
    except Exception as exc:
        print(f"Error loading config '{args.config}': {exc}", file=sys.stderr)
        return 2

    metric = _build_metric_from_args(args)
    result = run_checks(metric, rules)

    if args.output_json:
        print(report_to_json(result.report))
    else:
        print_run_result(result)

    return 1 if result.has_critical else 0


if __name__ == "__main__":
    sys.exit(main())
