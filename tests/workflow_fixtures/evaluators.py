"""Condition evaluators used by resource tests."""

from __future__ import annotations

from typing import Any

ALLOWED_ITEMS_KEY = "allowed_items"
CONFIG_RESOURCE = "runtime_config"
NOT_CALLABLE = "not-callable"


def has_allowed_item(data: dict[str, Any], resources: dict[str, Any]) -> bool:
    if CONFIG_RESOURCE not in resources:
        raise KeyError(
            f"has_allowed_item requires the '{CONFIG_RESOURCE}' resource; "
            f"got: {list(resources.keys())}"
        )
    allowed = set(resources[CONFIG_RESOURCE].get(ALLOWED_ITEMS_KEY, []))
    return any(item in allowed for item in data.get("items", []))


def missing_resources(data: dict[str, Any]) -> bool:
    return bool(data)


def extra_required_argument(
    data: dict[str, Any],
    required: str,
    resources: dict[str, Any],
) -> bool:
    return bool(data) and bool(required) and bool(resources)
