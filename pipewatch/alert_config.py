"""Load and validate alert rules from a YAML configuration file."""

from pathlib import Path
from typing import List

import yaml

from pipewatch.alerts import AlertRule, AlertSeverity

DEFAULT_CONFIG_PATH = Path("pipewatch_alerts.yml")

VALID_METRICS = {"success_rate", "throughput", "error_count"}
VALID_OPERATORS = {"lt", "gt", "lte", "gte"}


class AlertConfigError(Exception):
    """Raised when the alert configuration is invalid."""


def _parse_rule(raw: dict, index: int) -> AlertRule:
    """Parse and validate a single rule dict."""
    for required in ("name", "metric", "threshold", "operator"):
        if required not in raw:
            raise AlertConfigError(
                f"Rule #{index} is missing required field '{required}'"
            )

    metric = raw["metric"]
    if metric not in VALID_METRICS:
        raise AlertConfigError(
            f"Rule '{raw['name']}': unknown metric '{metric}'. "
            f"Valid options: {sorted(VALID_METRICS)}"
        )

    operator = raw["operator"]
    if operator not in VALID_OPERATORS:
        raise AlertConfigError(
            f"Rule '{raw['name']}': unknown operator '{operator}'. "
            f"Valid options: {sorted(VALID_OPERATORS)}"
        )

    try:
        severity = AlertSeverity(raw.get("severity", "warning"))
    except ValueError as exc:
        raise AlertConfigError(
            f"Rule '{raw['name']}': invalid severity '{raw.get('severity')}'"
        ) from exc

    return AlertRule(
        name=raw["name"],
        metric=metric,
        threshold=float(raw["threshold"]),
        operator=operator,
        severity=severity,
        message=raw.get("message"),
    )


def load_rules(config_path: Path = DEFAULT_CONFIG_PATH) -> List[AlertRule]:
    """Load alert rules from a YAML file."""
    if not config_path.exists():
        raise AlertConfigError(f"Config file not found: {config_path}")

    with config_path.open() as fh:
        data = yaml.safe_load(fh) or {}

    raw_rules = data.get("rules", [])
    if not isinstance(raw_rules, list):
        raise AlertConfigError("'rules' must be a list")

    return [_parse_rule(raw, i) for i, raw in enumerate(raw_rules)]
