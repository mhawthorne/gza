"""Schema compatibility diagnostics shared by DB and verify classification."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Literal, cast

SCHEMA_COMPATIBILITY_DIAGNOSTIC_MARKER = "GZA_SCHEMA_COMPATIBILITY_DIAGNOSTIC"
SCHEMA_RUNTIME_SKEW_EXIT_STATUS = "schema runtime skew"
SCHEMA_RUNTIME_SKEW_FAILURE_ORIGIN = "schema_runtime_skew"
SCHEMA_RUNTIME_SKEW_STATUS = "schema-runtime-skew"

SchemaCompatibilityDbRole = Literal["live_shared", "private_snapshot"]

_LEGACY_NEWER_SCHEMA_RE = re.compile(
    r"^Database schema v(?P<observed>\d+) is newer than supported v(?P<supported>\d+)\.$",
    re.MULTILINE,
)
_MARKER_RE = re.compile(rf"^{SCHEMA_COMPATIBILITY_DIAGNOSTIC_MARKER}\s+(?P<payload>\{{.*\}})$", re.MULTILINE)


@dataclass(frozen=True)
class SchemaCompatibilityDiagnostic:
    """Machine-readable schema compatibility result for runtime skew."""

    observed_db_version: int
    supported_db_version: int
    db_role: SchemaCompatibilityDbRole
    reason: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "observed_db_version": self.observed_db_version,
            "supported_db_version": self.supported_db_version,
            "db_role": self.db_role,
            "reason": self.reason,
        }

    @classmethod
    def from_payload(cls, payload: object) -> SchemaCompatibilityDiagnostic | None:
        if not isinstance(payload, dict):
            return None
        observed = payload.get("observed_db_version")
        supported = payload.get("supported_db_version")
        db_role = payload.get("db_role")
        reason = payload.get("reason")
        if isinstance(observed, bool) or not isinstance(observed, int):
            return None
        if isinstance(supported, bool) or not isinstance(supported, int):
            return None
        if db_role not in {"live_shared", "private_snapshot"}:
            return None
        if not isinstance(reason, str) or not reason:
            return None
        return cls(
            observed_db_version=observed,
            supported_db_version=supported,
            db_role=cast(SchemaCompatibilityDbRole, db_role),
            reason=reason,
        )

    def marker_line(self) -> str:
        return f"{SCHEMA_COMPATIBILITY_DIAGNOSTIC_MARKER} {json.dumps(self.to_payload(), sort_keys=True)}"


def newer_schema_compatibility_diagnostic(
    *,
    observed_db_version: int,
    supported_db_version: int,
    db_role: SchemaCompatibilityDbRole,
) -> SchemaCompatibilityDiagnostic:
    return SchemaCompatibilityDiagnostic(
        observed_db_version=observed_db_version,
        supported_db_version=supported_db_version,
        db_role=db_role,
        reason="newer_schema",
    )


def newer_schema_compatibility_message(diagnostic: SchemaCompatibilityDiagnostic) -> str:
    return (
        f"Database schema v{diagnostic.observed_db_version} is newer than supported "
        f"v{diagnostic.supported_db_version}.\n{diagnostic.marker_line()}"
    )


def parse_schema_compatibility_diagnostic(text: str | None) -> SchemaCompatibilityDiagnostic | None:
    """Parse structured diagnostics or the exact legacy newer-schema message."""
    if not text:
        return None
    marker_match = _MARKER_RE.search(text)
    if marker_match is not None:
        try:
            payload = json.loads(marker_match.group("payload"))
        except json.JSONDecodeError:
            payload = None
        diagnostic = SchemaCompatibilityDiagnostic.from_payload(payload)
        if diagnostic is not None:
            return diagnostic

    legacy_match = _LEGACY_NEWER_SCHEMA_RE.search(text)
    if legacy_match is None:
        return None
    return newer_schema_compatibility_diagnostic(
        observed_db_version=int(legacy_match.group("observed")),
        supported_db_version=int(legacy_match.group("supported")),
        db_role="live_shared",
    )
