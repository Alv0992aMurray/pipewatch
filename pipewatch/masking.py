"""Field masking for sensitive pipeline metric data in reports and exports."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any

_DEFAULT_MASK = "***"

SENSITIVE_KEYS = {"api_key", "password", "secret", "token", "credential"}


@dataclass
class MaskingConfig:
    keys: set[str] = field(default_factory=lambda: set(SENSITIVE_KEYS))
    mask: str = _DEFAULT_MASK
    case_sensitive: bool = False

    def should_mask(self, key: str) -> bool:
        k = key if self.case_sensitive else key.lower()
        targets = self.keys if self.case_sensitive else {t.lower() for t in self.keys}
        return k in targets


@dataclass
class MaskResult:
    original: dict[str, Any]
    masked: dict[str, Any]
    fields_masked: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "masked": self.masked,
            "fields_masked": self.fields_masked,
            "count": len(self.fields_masked),
        }


def mask_dict(data: dict[str, Any], config: MaskingConfig | None = None) -> MaskResult:
    """Recursively mask sensitive keys in a dict."""
    if config is None:
        config = MaskingConfig()
    masked: dict[str, Any] = {}
    fields_masked: list[str] = []

    def _recurse(src: dict[str, Any], dst: dict[str, Any], prefix: str) -> None:
        for k, v in src.items():
            full_key = f"{prefix}.{k}" if prefix else k
            if config.should_mask(k):
                dst[k] = config.mask
                fields_masked.append(full_key)
            elif isinstance(v, dict):
                dst[k] = {}
                _recurse(v, dst[k], full_key)
            else:
                dst[k] = v

    _recurse(data, masked, "")
    return MaskResult(original=data, masked=masked, fields_masked=fields_masked)


def apply_masking(records: list[dict[str, Any]], config: MaskingConfig | None = None) -> list[dict[str, Any]]:
    """Return a list of masked dicts."""
    return [mask_dict(r, config).masked for r in records]
