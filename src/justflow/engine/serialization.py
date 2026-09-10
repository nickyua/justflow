"""Versioned strict JSON serialization for durable engine boundaries."""

from __future__ import annotations

import json
import math
from enum import Enum
from typing import Any

STRICT_JSON_VERSION = 1


class StrictJsonLayout(str, Enum):
    CANONICAL = "canonical"
    CACHE_V1 = "cache-v1"
    PRETTY = "pretty"
    SCRIPT = "script"


class StrictJsonError(ValueError):
    pass


def validate_json_value(value: Any, *, path: str = "$") -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise StrictJsonError(f"{path}: non-finite numbers are not supported")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            validate_json_value(item, path=f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise StrictJsonError(
                    f"{path}: object keys must be strings, got {type(key).__name__}"
                )
            validate_json_value(item, path=f"{path}.{key}")
        return
    raise StrictJsonError(f"{path}: unsupported JSON type {type(value).__name__}")


def dumps_strict_json(value: Any, *, layout: StrictJsonLayout) -> str:
    validate_json_value(value)
    options: dict[str, Any] = {"allow_nan": False}
    match layout:
        case StrictJsonLayout.CANONICAL:
            options.update(ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        case StrictJsonLayout.CACHE_V1:
            options.update(sort_keys=True)
        case StrictJsonLayout.PRETTY:
            options.update(ensure_ascii=False, indent=2)
        case StrictJsonLayout.SCRIPT:
            options.update(ensure_ascii=True, separators=(",", ":"))
    try:
        return json.dumps(value, **options)
    except (TypeError, ValueError, UnicodeError) as exc:
        raise StrictJsonError(str(exc)) from exc


def strict_json_bytes(value: Any, *, layout: StrictJsonLayout) -> bytes:
    try:
        return dumps_strict_json(value, layout=layout).encode("utf-8")
    except UnicodeEncodeError as exc:
        raise StrictJsonError(str(exc)) from exc


def loads_strict_json(serialized: str | bytes | bytearray) -> Any:
    try:
        value = json.loads(
            serialized,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_non_finite_constant,
        )
    except (json.JSONDecodeError, TypeError, ValueError, UnicodeError) as exc:
        raise StrictJsonError(str(exc)) from exc
    validate_json_value(value)
    return value


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise StrictJsonError(f"duplicate object key '{key}'")
        value[key] = item
    return value


def _reject_non_finite_constant(value: str) -> None:
    raise StrictJsonError(f"non-finite number '{value}'")
