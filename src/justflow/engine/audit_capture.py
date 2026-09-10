"""Deterministic audit capture and JSON Pointer redaction."""

from __future__ import annotations

from typing import Any

from justflow.config.models import (
    AuditCaptureConfig,
    MetadataOnlyAuditCapture,
    RedactedAuditCapture,
)
from justflow.engine.serialization import (
    STRICT_JSON_VERSION,
    StrictJsonLayout,
    strict_json_bytes,
)

REDACTED_VALUE = "[REDACTED]"
PAYLOAD_FIELDS = frozenset({"params", "globals", "input", "output", "result", "message"})
STEP_METADATA_FIELDS = frozenset(
    {
        "seq",
        "status",
        "started_at",
        "duration_ms",
        "condition",
        "then",
        "signal",
        "sleep_sec",
        "attempts",
        "cache",
        "items_total",
        "items_failed",
        "code",
    }
)
FAILURE_METADATA_FIELDS = frozenset({"step", "code", "cause_code"})


class AuditCaptureError(ValueError):
    pass


class AuditCaptureLimitError(AuditCaptureError):
    def __init__(self, *, actual: int, limit: int) -> None:
        self.actual = actual
        self.limit = limit
        super().__init__(f"Captured audit payload is {actual} bytes; configured maximum is {limit}")


def capture_audit_record(
    record: dict[str, Any],
    capture: AuditCaptureConfig,
) -> dict[str, Any]:
    if isinstance(capture, MetadataOnlyAuditCapture):
        captured = _metadata_only(record)
    else:
        captured = _copy_json(record)
        if isinstance(capture, RedactedAuditCapture):
            for pointer in capture.paths:
                captured = _redact_pointer(captured, pointer)
        _enforce_payload_limit(captured, capture.max_payload_bytes)

    captured["capture_mode"] = capture.mode
    captured["serialization_version"] = STRICT_JSON_VERSION
    return captured


def _metadata_only(record: dict[str, Any]) -> dict[str, Any]:
    captured = {
        key: _copy_json(value)
        for key, value in record.items()
        if key not in {"params", "steps", "error", "failures", "result", "secondary"}
    }
    steps = record.get("steps")
    if isinstance(steps, dict):
        captured["steps"] = {
            step_name: {
                key: _copy_json(value)
                for key, value in entry.items()
                if key in STEP_METADATA_FIELDS
            }
            for step_name, entry in steps.items()
            if isinstance(entry, dict)
        }
    elif isinstance(steps, list):
        captured["steps"] = _copy_json(steps)

    error = record.get("error")
    if isinstance(error, dict):
        captured["error"] = {
            key: _copy_json(value)
            for key, value in error.items()
            if key in FAILURE_METADATA_FIELDS or key in {"category", "phase", "retryable"}
        }
    failures = record.get("failures")
    if isinstance(failures, list):
        captured["failures"] = [
            {
                key: _copy_json(value)
                for key, value in failure.items()
                if key in FAILURE_METADATA_FIELDS
            }
            for failure in failures
            if isinstance(failure, dict)
        ]
    secondary = record.get("secondary")
    if isinstance(secondary, list):
        captured["secondary"] = [
            {
                key: _copy_json(value)
                for key, value in failure.items()
                if key in FAILURE_METADATA_FIELDS or key in {"category", "phase", "retryable"}
            }
            for failure in secondary
            if isinstance(failure, dict)
        ]
    return captured


def _enforce_payload_limit(record: dict[str, Any], limit: int) -> None:
    payloads: list[Any] = []
    _collect_payloads(record, payloads)
    actual = len(strict_json_bytes(payloads, layout=StrictJsonLayout.CANONICAL))
    if actual > limit:
        raise AuditCaptureLimitError(actual=actual, limit=limit)


def _collect_payloads(value: Any, payloads: list[Any]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in PAYLOAD_FIELDS:
                payloads.append(item)
            else:
                _collect_payloads(item, payloads)
    elif isinstance(value, list):
        for item in value:
            _collect_payloads(item, payloads)


def _redact_pointer(document: Any, pointer: str) -> Any:
    tokens = _pointer_tokens(pointer)
    return _redact_at(document, tokens)


def _pointer_tokens(pointer: str) -> tuple[str, ...]:
    if not pointer.startswith("/"):
        raise AuditCaptureError("Redaction paths must be JSON Pointers beginning with '/'")
    return tuple(token.replace("~1", "/").replace("~0", "~") for token in pointer[1:].split("/"))


def _redact_at(value: Any, tokens: tuple[str, ...]) -> Any:
    if not tokens:
        return REDACTED_VALUE
    token, remaining = tokens[0], tokens[1:]
    if isinstance(value, dict):
        if token == "*":
            return {key: _redact_at(item, remaining) for key, item in value.items()}
        if token not in value:
            return value
        return {
            key: (_redact_at(item, remaining) if key == token else item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        if token == "*":
            return [_redact_at(item, remaining) for item in value]
        try:
            index = int(token)
        except ValueError:
            return value
        if index < 0 or index >= len(value):
            return value
        return [
            _redact_at(item, remaining) if item_index == index else item
            for item_index, item in enumerate(value)
        ]
    return value


def _copy_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _copy_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_json(item) for item in value]
    return value
