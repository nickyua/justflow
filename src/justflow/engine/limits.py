"""Pure guards for bounded workflow runtime data."""

from __future__ import annotations

from enum import Enum
from typing import Any

from justflow.engine.serialization import (
    StrictJsonError,
    StrictJsonLayout,
)
from justflow.engine.serialization import (
    strict_json_bytes as serialize_strict_json_bytes,
)


class LimitKind(str, Enum):
    PAYLOAD_BYTES = "payload_bytes"
    COLLECTION_ITEMS = "collection_items"
    FANOUT_ITEMS = "fanout_items"
    PARALLELISM = "parallelism"
    LOOP_ATTEMPTS = "loop_attempts"
    TOTAL_INVOCATIONS = "total_invocations"
    QUEUED_MESSAGES = "queued_messages"
    QUEUED_MESSAGE_BYTES = "queued_message_bytes"


class LimitExceededError(Exception):
    def __init__(
        self,
        *,
        kind: LimitKind,
        boundary: str,
        actual: int,
        limit: int,
    ) -> None:
        self.kind = kind
        self.boundary = boundary
        self.actual = actual
        self.limit = limit
        super().__init__(
            f"Runtime limit exceeded at '{boundary}': {kind.value} is {actual}, maximum is {limit}"
        )


class PayloadSerializationError(Exception):
    def __init__(self, boundary: str, cause: Exception) -> None:
        self.boundary = boundary
        self.cause = cause
        super().__init__(f"Payload at '{boundary}' is not strict JSON: {cause}")


def strict_json_bytes(value: Any) -> bytes:
    return serialize_strict_json_bytes(value, layout=StrictJsonLayout.CANONICAL)


def enforce_payload_bytes(value: Any, *, boundary: str, limit: int) -> None:
    try:
        actual = len(strict_json_bytes(value))
    except StrictJsonError as exc:
        raise PayloadSerializationError(boundary, exc) from exc
    enforce_limit(
        actual,
        limit=limit,
        kind=LimitKind.PAYLOAD_BYTES,
        boundary=boundary,
    )


def enforce_utf8_bytes(value: str, *, boundary: str, limit: int) -> None:
    try:
        actual = len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise LimitExceededError(
            kind=LimitKind.PAYLOAD_BYTES,
            boundary=boundary,
            actual=limit + 1,
            limit=limit,
        ) from exc
    enforce_limit(
        actual,
        limit=limit,
        kind=LimitKind.PAYLOAD_BYTES,
        boundary=boundary,
    )


def enforce_limit(
    actual: int,
    *,
    limit: int,
    kind: LimitKind,
    boundary: str,
) -> None:
    if actual > limit:
        raise LimitExceededError(
            kind=kind,
            boundary=boundary,
            actual=actual,
            limit=limit,
        )
