"""Reference resolution - resolves dot-notation references and param interpolation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from justflow.config.grammar import (
    PlaceholderSyntaxError,
    ReferencePath,
    ReferenceSyntaxError,
    parse_placeholders,
)
from justflow.engine.data import (
    DataNormalizationError,
    DataPathError,
    resolve_json_path,
)


def resolve_reference(
    ref: ReferencePath | str,
    step_outputs: dict[str, Any],
    params: dict[str, Any],
) -> Any:
    """Resolve a dot-notation reference against step outputs and params.

    Supports:
      - "step_name.field.nested" -> step_outputs["step_name"]["field"]["nested"]
      - "shared_output_name" -> step_outputs["shared_output_name"] (convergence alias)
      - "input" / "input.field" -> handled by caller (passed as step_outputs key)
    """
    try:
        reference = ReferencePath.parse(ref)
    except ReferenceSyntaxError as exc:
        raise ReferenceResolutionError(f"Invalid reference '{ref}': {exc}") from exc
    root = reference.root

    if root in step_outputs or root in params:
        value = step_outputs[root] if root in step_outputs else params[root]
        try:
            return resolve_json_path(value, reference.path, root=root)
        except (DataNormalizationError, DataPathError) as exc:
            raise ReferenceResolutionError(str(exc)) from exc

    raise ReferenceResolutionError(
        f"Cannot resolve reference '{reference.value}': '{root}' not found"
    )


def resolve_input(
    input_spec: (
        ReferencePath
        | str
        | Mapping[str, ReferencePath | str]
        | Sequence[ReferencePath | str]
        | None
    ),
    step_outputs: dict[str, Any],
    params: dict[str, Any],
) -> Any:
    """Resolve the input field of a flow step.

    Handles:
      - None -> None
      - str -> single reference resolution
      - dict -> named inputs {key: reference}
      - list -> list of resolved references
    """
    if input_spec is None:
        return None

    if isinstance(input_spec, (ReferencePath, str)):
        return resolve_reference(input_spec, step_outputs, params)

    if isinstance(input_spec, Mapping):
        return {
            key: resolve_reference(ref, step_outputs, params) for key, ref in input_spec.items()
        }

    if isinstance(input_spec, Sequence):
        return [resolve_reference(ref, step_outputs, params) for ref in input_spec]

    return input_spec


def find_unresolved_placeholders(value: Any) -> list[str]:
    """Collect ${...} placeholders still present after interpolation.

    Self-referencing params (e.g. ``source_id: "${source_id}"`` with no trigger
    value) survive interpolation as literals; callers use this to fail fast
    instead of leaking placeholders into paths and payloads.
    """
    if isinstance(value, str):
        try:
            return [placeholder.name for placeholder in parse_placeholders(value)]
        except PlaceholderSyntaxError as exc:
            raise ReferenceResolutionError(f"Invalid placeholder syntax: {exc}") from exc
    if isinstance(value, dict):
        return [p for v in value.values() for p in find_unresolved_placeholders(v)]
    if isinstance(value, list):
        return [p for item in value for p in find_unresolved_placeholders(item)]
    return []


def interpolate_params(value: Any, params: dict[str, Any]) -> Any:
    """Recursively interpolate ${param_name} in strings, dicts, and lists."""
    if isinstance(value, str):
        return _interpolate_string(value, params)
    if isinstance(value, dict):
        return {k: interpolate_params(v, params) for k, v in value.items()}
    if isinstance(value, list):
        return [interpolate_params(item, params) for item in value]
    return value


def _interpolate_string(s: str, params: dict[str, Any]) -> Any:
    """Interpolate ${...} placeholders in a string.

    If the entire string is a single placeholder, return the raw value (preserving type).
    Otherwise, substitute as string.
    """
    try:
        placeholders = parse_placeholders(s)
    except PlaceholderSyntaxError as exc:
        raise ReferenceResolutionError(f"Invalid placeholder syntax: {exc}") from exc
    if len(placeholders) == 1 and placeholders[0].start == 0 and placeholders[0].end == len(s):
        key = placeholders[0].name
        if key in params:
            return params[key]
        raise ReferenceResolutionError(f"Param '{key}' not found for interpolation")

    if not placeholders:
        return s

    interpolated: list[str] = []
    position = 0
    for placeholder in placeholders:
        interpolated.append(s[position : placeholder.start])
        key = placeholder.name
        if key in params:
            interpolated.append(str(params[key]))
        else:
            raise ReferenceResolutionError(f"Param '{key}' not found for interpolation")
        position = placeholder.end
    interpolated.append(s[position:])
    return "".join(interpolated)


class ReferenceResolutionError(Exception):
    """Raised when a reference cannot be resolved."""
