"""Load alert routing rules from a YAML config file."""
from __future__ import annotations

from pathlib import Path
from typing import List

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

from pipewatch.alerts import AlertSeverity
from pipewatch.routing import RoutingRule

_DEFAULT_PATH = Path("pipewatch_routing.yml")


class RoutingConfigError(Exception):
    """Raised when the routing config file is invalid."""


def _parse_rule(raw: dict) -> RoutingRule:
    if "destination" not in raw:
        raise RoutingConfigError("Each routing rule must have a 'destination' field.")
    severity_raw = raw.get("min_severity", "warning").upper()
    try:
        severity = AlertSeverity[severity_raw]
    except KeyError:
        raise RoutingConfigError(f"Unknown severity: {severity_raw}")
    return RoutingRule(
        destination=raw["destination"],
        pipeline=raw.get("pipeline"),
        min_severity=severity,
    )


def load_routing_rules(path: Path = _DEFAULT_PATH) -> List[RoutingRule]:
    """Parse routing rules from *path*; return empty list if file is absent."""
    if yaml is None:  # pragma: no cover
        raise RoutingConfigError("PyYAML is required to load routing config.")
    if not path.exists():
        return []
    data = yaml.safe_load(path.read_text()) or {}
    raw_rules = data.get("routing", [])
    return [_parse_rule(r) for r in raw_rules]
