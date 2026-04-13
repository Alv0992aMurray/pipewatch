"""Load escalation policies from YAML configuration."""
from __future__ import annotations

from pathlib import Path
from typing import List

import yaml

from pipewatch.alerts import AlertSeverity
from pipewatch.escalation import EscalationPolicy

_DEFAULT_PATH = Path("pipewatch_escalations.yml")


class EscalationConfigError(Exception):
    """Raised when the escalation config file is invalid."""


def _parse_policy(raw: dict, index: int) -> EscalationPolicy:
    for required in ("pipeline", "rule"):
        if required not in raw:
            raise EscalationConfigError(
                f"Escalation policy #{index} missing required field '{required}'"
            )

    severity_str = raw.get("escalate_to", "critical").upper()
    try:
        escalate_to = AlertSeverity[severity_str]
    except KeyError:
        raise EscalationConfigError(
            f"Escalation policy #{index} has unknown severity '{severity_str}'"
        )

    threshold = raw.get("threshold", 3)
    if not isinstance(threshold, int) or threshold < 1:
        raise EscalationConfigError(
            f"Escalation policy #{index} threshold must be a positive integer"
        )

    return EscalationPolicy(
        pipeline=str(raw["pipeline"]),
        rule_name=str(raw["rule"]),
        threshold=threshold,
        escalate_to=escalate_to,
    )


def load_escalation_policies(
    path: Path = _DEFAULT_PATH,
) -> List[EscalationPolicy]:
    """Load escalation policies from *path*; returns empty list if file absent."""
    if not path.exists():
        return []

    with path.open() as fh:
        data = yaml.safe_load(fh)

    if not data:
        return []

    raw_policies = data.get("escalations", [])
    if not isinstance(raw_policies, list):
        raise EscalationConfigError("'escalations' must be a list")

    return [_parse_policy(r, i) for i, r in enumerate(raw_policies)]
