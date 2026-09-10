"""Tests for deterministic runtime limit guards."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeAlias

import pytest

from justflow.engine.limits import (
    LimitExceededError,
    PayloadSerializationError,
    enforce_payload_bytes,
    strict_json_bytes,
)


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: None


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


PayloadOutcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class PayloadLimitCase:
    id: str
    value: Any
    limit: int
    outcome: PayloadOutcome


UNICODE_PAYLOAD = {"value": "é"}
UNICODE_PAYLOAD_BYTES = len(strict_json_bytes(UNICODE_PAYLOAD))

PAYLOAD_LIMIT_CASES = [
    PayloadLimitCase(
        id="exact-byte-boundary",
        value=UNICODE_PAYLOAD,
        limit=UNICODE_PAYLOAD_BYTES,
        outcome=Returns(value=None),
    ),
    PayloadLimitCase(
        id="multibyte-over-boundary",
        value=UNICODE_PAYLOAD,
        limit=UNICODE_PAYLOAD_BYTES - 1,
        outcome=Raises(exc=LimitExceededError, match="payload_bytes"),
    ),
    PayloadLimitCase(
        id="non-finite-is-not-serialized",
        value={"value": float("nan")},
        limit=UNICODE_PAYLOAD_BYTES,
        outcome=Raises(exc=PayloadSerializationError, match="not strict JSON"),
    ),
]


@pytest.mark.parametrize(
    "case",
    PAYLOAD_LIMIT_CASES,
    ids=lambda case: case.id,
)
def test_payload_byte_limit(case: PayloadLimitCase) -> None:
    if isinstance(case.outcome, Returns):
        assert (
            enforce_payload_bytes(case.value, boundary="test.payload", limit=case.limit)
            is case.outcome.value
        )
        return

    with pytest.raises(case.outcome.exc, match=case.outcome.match):
        enforce_payload_bytes(case.value, boundary="test.payload", limit=case.limit)


def test_strict_json_bytes_are_canonical() -> None:
    assert strict_json_bytes({"z": 1, "a": "é"}) == b'{"a":"\xc3\xa9","z":1}'
