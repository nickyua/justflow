"""Authored-style YAML rendering shared by the draft editor and operations views.

Declaration key order, defaults omitted, multi-line strings as block literals —
so rendered documents read like the files tenants actually write.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Literal, get_origin

import yaml
from pydantic import BaseModel, ValidationError


class AuthoredStyleDumper(yaml.SafeDumper):
    """Renders multi-line strings as block literals so descriptions stay readable."""


def _represent_multiline_str(dumper: yaml.SafeDumper, value: str) -> yaml.ScalarNode:
    style = "|" if "\n" in value else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)


AuthoredStyleDumper.add_representer(str, _represent_multiline_str)


def authored_dump(model: BaseModel) -> dict[str, object]:
    """Dump a config model in authored style: declaration key order, defaults omitted.

    Literal fields are retained because they identify tagged-union variants even
    when their model supplies the value as a default.
    """
    pruned = model.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
        exclude_defaults=True,
    )
    restored = _restore_literal_fields(model, pruned)
    if not isinstance(restored, dict):
        raise TypeError("An authored model must serialize to an object")
    try:
        if type(model).model_validate(restored) == model:
            return restored
    except ValidationError:
        pass
    return model.model_dump(mode="json", by_alias=True, exclude_none=True)


def _restore_literal_fields(source: object, dumped: object) -> object:
    if isinstance(source, BaseModel) and isinstance(dumped, dict):
        restored: dict[str, object] = {}
        for field_name, field in type(source).model_fields.items():
            key = field.serialization_alias or field.alias or field_name
            if key in dumped:
                restored[key] = _restore_literal_fields(getattr(source, field_name), dumped[key])
            elif get_origin(field.annotation) is Literal:
                literal = source.model_dump(mode="json", by_alias=True, include={field_name})
                restored[key] = literal[key]
        return restored
    if isinstance(source, Mapping) and isinstance(dumped, dict):
        return {
            key: _restore_literal_fields(source[key], value)
            for key, value in dumped.items()
            if key in source
        }
    if (
        isinstance(source, Sequence)
        and not isinstance(source, (str, bytes))
        and isinstance(dumped, list)
        and len(source) == len(dumped)
    ):
        return [
            _restore_literal_fields(source_value, dumped_value)
            for source_value, dumped_value in zip(source, dumped, strict=True)
        ]
    return dumped


def render_authored_yaml(payload: object) -> str:
    """Render to YAML preserving key order; raises yaml.YAMLError on unrepresentable input."""
    return yaml.dump(
        payload,
        Dumper=AuthoredStyleDumper,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=False,
    )
