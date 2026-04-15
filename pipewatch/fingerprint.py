"""Alert fingerprinting — generate stable identifiers for alerts to support dedup, tracking, and correlation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pipewatch.alerts import Alert, AlertSeverity


@dataclass
class Fingerprint:
    """A stable identifier for a class of alert."""

    pipeline: str
    rule_name: str
    severity: str
    digest: str  # hex SHA-256 prefix

    def __str__(self) -> str:
        return f"{self.pipeline}/{self.rule_name}/{self.severity}#{self.digest}"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "rule_name": self.rule_name,
            "severity": self.severity,
            "digest": self.digest,
        }


@dataclass
class FingerprintedAlert:
    """An alert paired with its computed fingerprint."""

    alert: Alert
    fingerprint: Fingerprint
    first_seen: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "fingerprint": self.fingerprint.to_dict(),
            "message": self.alert.message,
            "severity": self.alert.severity.value,
            "first_seen": self.first_seen.isoformat(),
        }


def _compute_digest(pipeline: str, rule_name: str, severity: str) -> str:
    """Return a short SHA-256 hex digest for the given alert dimensions."""
    raw = json.dumps({"pipeline": pipeline, "rule": rule_name, "severity": severity}, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def fingerprint_alert(alert: Alert) -> Fingerprint:
    """Compute a Fingerprint for a single Alert."""
    severity = alert.severity.value if isinstance(alert.severity, AlertSeverity) else str(alert.severity)
    digest = _compute_digest(alert.pipeline, alert.rule_name, severity)
    return Fingerprint(
        pipeline=alert.pipeline,
        rule_name=alert.rule_name,
        severity=severity,
        digest=digest,
    )


def fingerprint_alerts(alerts: list[Alert]) -> list[FingerprintedAlert]:
    """Return a FingerprintedAlert for every alert in the list."""
    return [FingerprintedAlert(alert=a, fingerprint=fingerprint_alert(a)) for a in alerts]


def group_by_fingerprint(alerts: list[Alert]) -> dict[str, list[Alert]]:
    """Group alerts by their fingerprint string, useful for aggregation."""
    groups: dict[str, list[Alert]] = {}
    for alert in alerts:
        fp = str(fingerprint_alert(alert))
        groups.setdefault(fp, []).append(alert)
    return groups
