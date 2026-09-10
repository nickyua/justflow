"""Tests for JSON-compatible data normalization and path traversal."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeAlias

import pytest
from pydantic import BaseModel

from justflow.engine.data import (
    DataNormalizationError,
    DataPathError,
    JSONValue,
    normalize_json_value,
    resolve_json_path,
)
from justflow.engine.limits import LimitExceededError, LimitKind


class PayloadModel(BaseModel):
    name: str
    values: list[int]


@dataclass(frozen=True, kw_only=True)
class PayloadRecord:
    name: str
    active: bool


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: JSONValue


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


NormalizationOutcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class NormalizationCase:
    id: str
    value: Any
    outcome: NormalizationOutcome


NORMALIZATION_CASES = [
    NormalizationCase(
        id="recursive-json",
        value={"items": [1, True, None, {"ratio": 0.5}]},
        outcome=Returns(value={"items": [1, True, None, {"ratio": 0.5}]}),
    ),
    NormalizationCase(
        id="pydantic-model",
        value=PayloadModel(name="sample", values=[1, 2]),
        outcome=Returns(value={"name": "sample", "values": [1, 2]}),
    ),
    NormalizationCase(
        id="dataclass",
        value=PayloadRecord(name="sample", active=True),
        outcome=Returns(value={"name": "sample", "active": True}),
    ),
    NormalizationCase(
        id="non-string-key",
        value={1: "value"},
        outcome=Raises(exc=DataNormalizationError, match="keys must be strings"),
    ),
    NormalizationCase(
        id="non-finite-number",
        value=float("inf"),
        outcome=Raises(exc=DataNormalizationError, match="non-finite"),
    ),
    NormalizationCase(
        id="unsupported-object",
        value=object(),
        outcome=Raises(exc=DataNormalizationError, match="unsupported type object"),
    ),
]


@pytest.mark.parametrize("case", NORMALIZATION_CASES, ids=lambda case: case.id)
def test_normalize_json_value(case: NormalizationCase) -> None:
    if isinstance(case.outcome, Returns):
        assert normalize_json_value(case.value) == case.outcome.value
        return

    with pytest.raises(case.outcome.exc, match=case.outcome.match):
        normalize_json_value(case.value)


@dataclass(frozen=True, kw_only=True)
class PathCase:
    id: str
    value: Any
    tokens: list[str | int]
    outcome: NormalizationOutcome


PATH_CASES = [
    PathCase(
        id="ordinary-special-looking-keys",
        value={"_id": {"__proto__": "value"}},
        tokens=["_id", "__proto__"],
        outcome=Returns(value="value"),
    ),
    PathCase(
        id="bounded-list-index",
        value={"items": ["first", "second"]},
        tokens=["items", 1],
        outcome=Returns(value="second"),
    ),
    PathCase(
        id="missing-key",
        value={"present": True},
        tokens=["missing"],
        outcome=Raises(exc=DataPathError, match="key is not present"),
    ),
    PathCase(
        id="negative-index",
        value=["value"],
        tokens=[-1],
        outcome=Raises(exc=DataPathError, match="out of range"),
    ),
    PathCase(
        id="string-list-index",
        value=["value"],
        tokens=["0"],
        outcome=Raises(exc=DataPathError, match="integer index"),
    ),
]


@pytest.mark.parametrize("case", PATH_CASES, ids=lambda case: case.id)
def test_resolve_json_path(case: PathCase) -> None:
    if isinstance(case.outcome, Returns):
        assert resolve_json_path(case.value, case.tokens) == case.outcome.value
        return

    with pytest.raises(case.outcome.exc, match=case.outcome.match):
        resolve_json_path(case.value, case.tokens)


class AdversarialValue:
    @property
    def secret(self) -> str:
        raise AssertionError("property access must not run")

    def __getitem__(self, key: str) -> str:
        raise AssertionError(f"custom indexing must not run: {key}")


def test_rejects_objects_without_invoking_properties_or_custom_indexing() -> None:
    with pytest.raises(DataNormalizationError, match="unsupported type"):
        resolve_json_path(AdversarialValue(), ["secret"])


def test_enforces_depth_and_path_token_bounds() -> None:
    with pytest.raises(DataNormalizationError, match="depth exceeds 1"):
        normalize_json_value({"one": {"two": True}}, max_depth=1)

    with pytest.raises(DataPathError, match="more than 1 tokens"):
        resolve_json_path({"one": {"two": True}}, ["one", "two"], max_tokens=1)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param({"one": 1, "two": 2}, id="object"),
        pytest.param([1, 2], id="list"),
        pytest.param((1, 2), id="tuple"),
    ],
)
def test_collection_growth_raises_typed_limit(value: Any) -> None:
    with pytest.raises(LimitExceededError) as exc_info:
        normalize_json_value(value, path="payload", max_collection_items=1)

    assert exc_info.value.kind is LimitKind.COLLECTION_ITEMS
    assert exc_info.value.boundary == "payload"
