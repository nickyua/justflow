"""Step I/O contract validation (G4).

Step definitions may declare input_schema/output_schema as an inline JSON
Schema dict or a dotted path to a pydantic BaseModel. Payloads are validated
at the activity boundary; the config validator inspects schema-known reference
paths without executing business code.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, NewType
from urllib.parse import unquote, urljoin

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError
from referencing import Registry, Resource
from referencing.exceptions import Unresolvable
from referencing.jsonschema import DRAFT202012

SchemaSpec = dict[str, Any] | str
ContractIdentity = NewType("ContractIdentity", str)
DEFAULT_JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
SUPPORTED_JSON_SCHEMA_DIALECTS = frozenset(
    {
        DEFAULT_JSON_SCHEMA_DIALECT,
        f"{DEFAULT_JSON_SCHEMA_DIALECT}#",
    }
)
MAX_SCHEMA_PATH_DEPTH = 32
JSON_SCHEMA_REFERENCE_KEYWORDS = ("$ref", "$dynamicRef")


class SchemaValueType(str, Enum):
    OBJECT = "object"
    ARRAY = "array"
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    NULL = "null"


@dataclass(frozen=True, slots=True)
class SchemaPathResult:
    exists: bool | None
    value_types: frozenset[SchemaValueType] | None = None
    detail: str | None = None


class ContractViolation(Exception):
    """A step's input or output does not match its declared schema."""

    def __init__(self, direction: str, step_name: str, detail: str):
        self.direction = direction
        self.step_name = step_name
        super().__init__(f"Step '{step_name}' {direction} violates its schema: {detail}")


class SchemaLoadError(Exception):
    """A dotted schema path does not resolve to a pydantic model."""


class SchemaDeclarationError(Exception):
    """A contract declaration is unsupported or malformed."""


def load_model(dotted_path: str) -> type[BaseModel]:
    try:
        module_path, attr_name = dotted_path.rsplit(".", 1)
    except ValueError as exc:
        raise SchemaLoadError(
            f"Schema path '{dotted_path}' must contain a module and model name"
        ) from exc
    try:
        module = importlib.import_module(module_path)
        model = getattr(module, attr_name)
    except (ImportError, AttributeError) as e:
        raise SchemaLoadError(f"Cannot import schema '{dotted_path}': {e}") from e
    if not (isinstance(model, type) and issubclass(model, BaseModel)):
        raise SchemaLoadError(f"Schema '{dotted_path}' is not a pydantic BaseModel subclass")
    return model


def validate_contract_declaration(schema: SchemaSpec) -> ContractIdentity:
    if isinstance(schema, str):
        model = load_model(schema)
        return _contract_identity(
            {
                "kind": "pydantic",
                "path": schema,
                "schema": model.model_json_schema(),
            }
        )

    dialect = schema.get("$schema", DEFAULT_JSON_SCHEMA_DIALECT)
    if not isinstance(dialect, str) or dialect not in SUPPORTED_JSON_SCHEMA_DIALECTS:
        raise SchemaDeclarationError(
            f"Unsupported JSON Schema dialect '{dialect}'; supported dialects: "
            f"{sorted(SUPPORTED_JSON_SCHEMA_DIALECTS)}"
        )

    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise SchemaDeclarationError(f"Invalid JSON Schema: {exc.message}") from exc
    resource = Resource.from_contents(schema, default_specification=DRAFT202012)
    registry = _offline_registry().with_resource("", resource).crawl()
    _validate_schema_references(resource, registry)
    normalized_schema = dict(schema)
    normalized_schema.setdefault("$schema", DEFAULT_JSON_SCHEMA_DIALECT)
    return _contract_identity(
        {
            "kind": "json-schema",
            "schema": normalized_schema,
        }
    )


def _contract_identity(value: dict[str, Any]) -> ContractIdentity:
    try:
        canonical = json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise SchemaDeclarationError(f"Contract declaration is not canonical JSON: {exc}") from exc
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    return ContractIdentity(f"sha256:{digest}")


def _offline_registry() -> Registry[Any]:
    """An explicit registry disables jsonschema's implicit urllib retrieval."""
    return Registry()


def _validate_schema_references(
    resource: Resource[Any], registry: Registry[Any], base_uri: str = ""
) -> None:
    contents = resource.contents
    if not isinstance(contents, dict):
        return
    dialect = contents.get("$schema", DEFAULT_JSON_SCHEMA_DIALECT)
    if not isinstance(dialect, str) or dialect not in SUPPORTED_JSON_SCHEMA_DIALECTS:
        raise SchemaDeclarationError("Unsupported JSON Schema dialect in nested resource")
    base_uri = urljoin(base_uri, resource.id() or "")
    resolver = registry.resolver(base_uri)
    for keyword in JSON_SCHEMA_REFERENCE_KEYWORDS:
        reference = contents.get(keyword)
        if not isinstance(reference, str):
            continue
        if reference and not reference.startswith("#"):
            raise SchemaDeclarationError("Remote JSON Schema references are not supported")
        try:
            resolver.lookup(reference)
        except Unresolvable:
            raise SchemaDeclarationError("Unresolved local JSON Schema references") from None
    for nested in resource.subresources():
        _validate_schema_references(nested, registry, base_uri)


def validate_payload(schema: SchemaSpec, payload: Any, *, direction: str, step_name: str) -> None:
    """Validate a payload against an inline JSON Schema or pydantic model path."""
    validate_contract_declaration(schema)
    if isinstance(schema, str):
        model = load_model(schema)
        try:
            model.model_validate(payload)
        except PydanticValidationError as e:
            raise ContractViolation(direction, step_name, str(e)) from e
        return

    try:
        validation_error = next(
            Draft202012Validator(schema, registry=_offline_registry()).iter_errors(payload), None
        )
    except (Unresolvable, RecursionError):
        raise SchemaDeclarationError(
            "Unresolvable JSON Schema reference during validation"
        ) from None
    if validation_error is not None:
        location = ".".join(str(part) for part in validation_error.absolute_path)
        detail = validation_error.message
        if location:
            detail = f"{location}: {detail}"
        raise ContractViolation(direction, step_name, detail)


def schema_properties(schema: SchemaSpec) -> set[str] | None:
    """Top-level field names a schema declares; None when unknowable.

    Used for config-time field-level reference checking — a None result means
    'don't check' (e.g. a non-object schema or an unimportable path, which the
    import checks report separately).
    """
    if isinstance(schema, str):
        try:
            return set(load_model(schema).model_fields.keys())
        except SchemaLoadError:
            return None
    properties = schema.get("properties")
    if isinstance(properties, dict) and properties:
        return set(properties.keys())
    return None


def inspect_schema_path(
    schema: SchemaSpec,
    tokens: tuple[str | int, ...],
) -> SchemaPathResult:
    """Return only schema facts that can be established without executing code."""
    if len(tokens) > MAX_SCHEMA_PATH_DEPTH:
        return SchemaPathResult(
            exists=False,
            detail=f"path exceeds {MAX_SCHEMA_PATH_DEPTH} components",
        )
    if isinstance(schema, str):
        try:
            root_schema = load_model(schema).model_json_schema()
        except SchemaLoadError:
            return SchemaPathResult(exists=None)
    else:
        root_schema = schema
    return _inspect_schema_node(root_schema, root_schema, tokens, frozenset())


def _inspect_schema_node(
    root: dict[str, Any],
    node: Any,
    tokens: tuple[str | int, ...],
    references: frozenset[str],
) -> SchemaPathResult:
    if isinstance(node, bool):
        if not node:
            return SchemaPathResult(exists=False)
        return SchemaPathResult(exists=True if not tokens else None)
    if not isinstance(node, dict):
        return SchemaPathResult(exists=None)

    reference = node.get("$ref")
    if isinstance(reference, str) and reference.startswith("#"):
        if reference in references:
            return SchemaPathResult(exists=None)
        target = _local_reference_target(root, reference)
        if target is None:
            return SchemaPathResult(exists=None)
        return _inspect_schema_node(root, target, tokens, references | {reference})

    alternatives = _schema_alternatives(node)
    if alternatives is not None:
        return _combine_schema_results(
            [
                _inspect_schema_node(root, alternative, tokens, references)
                for alternative in alternatives
            ]
        )

    if not tokens:
        return SchemaPathResult(exists=True, value_types=_schema_value_types(node))

    token, *remaining = tokens
    value_types = _schema_value_types(node)
    object_schema = SchemaValueType.OBJECT in value_types if value_types is not None else False
    array_schema = SchemaValueType.ARRAY in value_types if value_types is not None else False
    if value_types is not None and not object_schema and not array_schema:
        return SchemaPathResult(
            exists=False,
            detail=f"cannot traverse schema-known {', '.join(sorted(value_types))} value",
        )

    properties = node.get("properties")
    if isinstance(properties, dict) or object_schema:
        if not isinstance(token, str):
            return SchemaPathResult(
                exists=False,
                detail="object path requires a named field",
            )
        if isinstance(properties, dict) and token in properties:
            return _inspect_schema_node(
                root,
                properties[token],
                tuple(remaining),
                references,
            )
        additional = node.get("additionalProperties")
        if isinstance(additional, (dict, bool)):
            if additional is False:
                return SchemaPathResult(exists=False, detail=f"field {token!r} is not declared")
            return _inspect_schema_node(root, additional, tuple(remaining), references)
        if isinstance(properties, dict):
            return SchemaPathResult(exists=False, detail=f"field {token!r} is not declared")
        return SchemaPathResult(exists=None)

    if array_schema or "items" in node or "prefixItems" in node:
        if not isinstance(token, int):
            return SchemaPathResult(
                exists=False,
                detail="array path requires a numeric index",
            )
        prefix_items = node.get("prefixItems")
        if isinstance(prefix_items, list) and token < len(prefix_items):
            return _inspect_schema_node(
                root,
                prefix_items[token],
                tuple(remaining),
                references,
            )
        items = node.get("items")
        if items is False:
            return SchemaPathResult(exists=False, detail=f"array index {token} is not declared")
        if isinstance(items, (dict, bool)):
            return _inspect_schema_node(root, items, tuple(remaining), references)
        return SchemaPathResult(exists=None)

    return SchemaPathResult(exists=None)


def _schema_alternatives(node: dict[str, Any]) -> list[Any] | None:
    for keyword in ("anyOf", "oneOf"):
        alternatives = node.get(keyword)
        if isinstance(alternatives, list) and alternatives:
            return alternatives
    return None


def _combine_schema_results(results: list[SchemaPathResult]) -> SchemaPathResult:
    if all(result.exists is False for result in results):
        detail = next((result.detail for result in results if result.detail), None)
        return SchemaPathResult(exists=False, detail=detail)
    if any(result.exists is None for result in results):
        return SchemaPathResult(exists=None)
    existing = [result for result in results if result.exists]
    if not existing:
        return SchemaPathResult(exists=None)
    if any(result.value_types is None for result in existing):
        return SchemaPathResult(exists=True)
    value_types = frozenset(
        value_type for result in existing for value_type in result.value_types or ()
    )
    return SchemaPathResult(exists=True, value_types=value_types)


def _schema_value_types(node: dict[str, Any]) -> frozenset[SchemaValueType] | None:
    declared = node.get("type")
    if isinstance(declared, str):
        try:
            return frozenset({SchemaValueType(declared)})
        except ValueError:
            return None
    if isinstance(declared, list):
        values: set[SchemaValueType] = set()
        for value in declared:
            if not isinstance(value, str):
                return None
            try:
                values.add(SchemaValueType(value))
            except ValueError:
                return None
        return frozenset(values)
    if "properties" in node or "additionalProperties" in node:
        return frozenset({SchemaValueType.OBJECT})
    if "items" in node or "prefixItems" in node:
        return frozenset({SchemaValueType.ARRAY})
    return None


def _local_reference_target(root: dict[str, Any], reference: str) -> Any | None:
    if reference in {"", "#"}:
        return root
    fragment = unquote(reference[1:])
    if not fragment.startswith("/"):
        return None
    current: Any = root
    for encoded_part in fragment[1:].split("/"):
        part = encoded_part.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            return None
    return current
