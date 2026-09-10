"""JSON-compatible value normalization and safe path traversal."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import fields, is_dataclass
from typing import Any, TypeAlias

from pydantic import BaseModel

from justflow.config.grammar import MAX_REFERENCE_COMPONENTS
from justflow.config.runtime_limits import DEFAULT_COLLECTION_ITEMS
from justflow.engine.limits import LimitExceededError, LimitKind

MAX_JSON_DEPTH = 32
MAX_PATH_TOKENS = MAX_REFERENCE_COMPONENTS - 1

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
PathToken: TypeAlias = str | int


class DataNormalizationError(Exception):
    def __init__(self, path: str, detail: str) -> None:
        self.path = path
        super().__init__(f"Invalid JSON-compatible value at '{path}': {detail}")


class DataPathError(Exception):
    def __init__(self, path: str, detail: str) -> None:
        self.path = path
        super().__init__(f"Cannot resolve data path '{path}': {detail}")


def normalize_json_value(
    value: Any,
    *,
    path: str = "$",
    max_depth: int = MAX_JSON_DEPTH,
    max_collection_items: int = DEFAULT_COLLECTION_ITEMS,
    _depth: int = 0,
) -> JSONValue:
    if _depth > max_depth:
        raise DataNormalizationError(path, f"depth exceeds {max_depth}")

    if value is None or type(value) in {str, int, bool}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise DataNormalizationError(path, "non-finite numbers are not supported")
        return value
    if isinstance(value, BaseModel):
        return normalize_json_value(
            value.model_dump(mode="json"),
            path=path,
            max_depth=max_depth,
            max_collection_items=max_collection_items,
            _depth=_depth,
        )
    if is_dataclass(value) and not isinstance(value, type):
        return normalize_json_value(
            {field.name: getattr(value, field.name) for field in fields(value)},
            path=path,
            max_depth=max_depth,
            max_collection_items=max_collection_items,
            _depth=_depth,
        )
    if type(value) is dict:
        if len(value) > max_collection_items:
            raise LimitExceededError(
                kind=LimitKind.COLLECTION_ITEMS,
                boundary=path,
                actual=len(value),
                limit=max_collection_items,
            )
        normalized: dict[str, JSONValue] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise DataNormalizationError(path, "mapping keys must be strings")
            normalized[key] = normalize_json_value(
                item,
                path=f"{path}.{key}",
                max_depth=max_depth,
                max_collection_items=max_collection_items,
                _depth=_depth + 1,
            )
        return normalized
    if type(value) in {list, tuple}:
        if len(value) > max_collection_items:
            raise LimitExceededError(
                kind=LimitKind.COLLECTION_ITEMS,
                boundary=path,
                actual=len(value),
                limit=max_collection_items,
            )
        return [
            normalize_json_value(
                item,
                path=f"{path}[{index}]",
                max_depth=max_depth,
                max_collection_items=max_collection_items,
                _depth=_depth + 1,
            )
            for index, item in enumerate(value)
        ]
    raise DataNormalizationError(
        path,
        f"unsupported type {type(value).__name__}",
    )


def normalize_json_object(
    value: Any,
    *,
    path: str = "$",
    max_depth: int = MAX_JSON_DEPTH,
    max_collection_items: int = DEFAULT_COLLECTION_ITEMS,
) -> dict[str, JSONValue]:
    normalized = normalize_json_value(
        value,
        path=path,
        max_depth=max_depth,
        max_collection_items=max_collection_items,
    )
    if not isinstance(normalized, dict):
        raise DataNormalizationError(path, "expected an object")
    return normalized


def resolve_json_path(
    value: Any,
    tokens: Sequence[PathToken],
    *,
    root: str = "$",
    max_tokens: int = MAX_PATH_TOKENS,
) -> JSONValue:
    if len(tokens) > max_tokens:
        raise DataPathError(root, f"path contains more than {max_tokens} tokens")
    current = normalize_json_value(value, path=root)
    path = root
    for token in tokens:
        current, path = _resolve_token(current, token, path)
    return current


def _resolve_token(
    value: JSONValue,
    token: PathToken,
    path: str,
) -> tuple[JSONValue, str]:
    if isinstance(value, dict):
        if type(token) is not str:
            raise DataPathError(path, "object access requires a string key")
        next_path = f"{path}.{token}"
        if token not in value:
            raise DataPathError(next_path, "key is not present")
        return value[token], next_path
    if isinstance(value, list):
        if type(token) is not int:
            raise DataPathError(path, "array access requires an integer index")
        next_path = f"{path}[{token}]"
        if token < 0 or token >= len(value):
            raise DataPathError(next_path, "index is out of range")
        return value[token], next_path
    raise DataPathError(path, f"cannot traverse {type(value).__name__}")
