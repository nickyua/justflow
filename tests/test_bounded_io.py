"""Read limits apply while consuming data, and the owned stream always closes."""

from __future__ import annotations

import io
from dataclasses import dataclass

import pytest

from justflow.bounded_io import BoundedReadError, read_bounded_body


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: bytes


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


@dataclass(frozen=True, kw_only=True)
class ReadCase:
    id: str
    payload: bytes
    limit: int
    outcome: Returns | Raises


CASES = [
    ReadCase(id="empty", payload=b"", limit=1, outcome=Returns(value=b"")),
    ReadCase(id="exact", payload=b"ok", limit=2, outcome=Returns(value=b"ok")),
    ReadCase(
        id="oversized",
        payload=b"too long",
        limit=2,
        outcome=Raises(exc=BoundedReadError, match="byte limit"),
    ),
]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.id)
def test_bounded_response_consumption(case: ReadCase) -> None:
    stream = io.BytesIO(case.payload)
    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            read_bounded_body(stream, limit=case.limit)
    else:
        assert read_bounded_body(stream, limit=case.limit) == case.outcome.value
    assert stream.closed
