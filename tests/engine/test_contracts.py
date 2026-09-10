"""Tests for step I/O contract validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from pydantic import BaseModel

from justflow.engine.contracts import (
    ContractViolation,
    SchemaDeclarationError,
    SchemaLoadError,
    SchemaValueType,
    inspect_schema_path,
    load_model,
    schema_properties,
    validate_contract_declaration,
    validate_payload,
)

RECORD_JSON_SCHEMA = {
    "type": "object",
    "properties": {"record_id": {"type": "string"}, "value": {"type": "integer"}},
    "required": ["record_id"],
}


class RecordModel(BaseModel):
    record_id: str
    value: int | None = None


RECORD_MODEL_PATH = f"{__name__}.RecordModel"


class TestValidatePayload:
    @pytest.mark.parametrize(
        "schema", [RECORD_JSON_SCHEMA, RECORD_MODEL_PATH], ids=["json-schema", "pydantic-path"]
    )
    def test_valid_payload_passes(self, schema):
        validate_payload(schema, {"record_id": "R1", "value": 40}, direction="input", step_name="s")

    @pytest.mark.parametrize(
        "schema", [RECORD_JSON_SCHEMA, RECORD_MODEL_PATH], ids=["json-schema", "pydantic-path"]
    )
    @pytest.mark.parametrize(
        "payload", [{}, {"record_id": 5}], ids=["missing-required", "wrong-type"]
    )
    def test_invalid_payload_raises(self, schema, payload):
        with pytest.raises(ContractViolation, match="violates its schema"):
            validate_payload(schema, payload, direction="output", step_name="s")

    def test_unimportable_model_path(self):
        with pytest.raises(SchemaLoadError, match="Cannot import"):
            validate_payload("no.such.Model", {}, direction="input", step_name="s")

    def test_non_model_path(self):
        with pytest.raises(SchemaLoadError, match="not a pydantic BaseModel"):
            validate_payload(f"{__name__}.RECORD_JSON_SCHEMA", {}, direction="input", step_name="s")


class TestSchemaProperties:
    def test_json_schema_properties(self):
        assert schema_properties(RECORD_JSON_SCHEMA) == {"record_id", "value"}

    def test_pydantic_model_fields(self):
        assert schema_properties(RECORD_MODEL_PATH) == {"record_id", "value"}

    @pytest.mark.parametrize(
        "schema",
        [{"type": "array"}, {"type": "object"}, "no.such.Model"],
        ids=["non-object", "no-properties", "unimportable"],
    )
    def test_unknowable_returns_none(self, schema):
        assert schema_properties(schema) is None


class TestLoadModel:
    def test_loads_model(self):
        assert load_model(RECORD_MODEL_PATH) is RecordModel


@dataclass(frozen=True, kw_only=True)
class DeclarationReturns:
    value_prefix: str = "sha256:"


@dataclass(frozen=True, kw_only=True)
class DeclarationRaises:
    exc: type[BaseException]
    match: str


DeclarationOutcome = DeclarationReturns | DeclarationRaises


@dataclass(frozen=True, kw_only=True)
class SchemaDeclarationCase:
    id: str
    schema: dict[str, Any]
    outcome: DeclarationOutcome


SCHEMA_DECLARATION_CASES = [
    SchemaDeclarationCase(
        id="non-string-dialect",
        schema={"$schema": []},
        outcome=DeclarationRaises(exc=SchemaDeclarationError, match="dialect"),
    ),
    SchemaDeclarationCase(
        id="unknown-nested-dialect",
        schema={"$defs": {"value": {"$schema": "https://schemas.example/unknown"}}},
        outcome=DeclarationRaises(exc=SchemaDeclarationError, match="dialect"),
    ),
    SchemaDeclarationCase(
        id="default-draft-2020-12",
        schema={"type": "object"},
        outcome=DeclarationReturns(),
    ),
    SchemaDeclarationCase(
        id="explicit-supported-dialect",
        schema={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
        },
        outcome=DeclarationReturns(),
    ),
    SchemaDeclarationCase(
        id="local-reference",
        schema={
            "$defs": {"record": {"type": "object"}},
            "$ref": "#/$defs/record",
        },
        outcome=DeclarationReturns(),
    ),
    SchemaDeclarationCase(
        id="local-anchor",
        schema={
            "$defs": {
                "record": {
                    "$anchor": "record",
                    "type": "object",
                }
            },
            "$ref": "#record",
        },
        outcome=DeclarationReturns(),
    ),
    SchemaDeclarationCase(
        id="unsupported-dialect",
        schema={
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
        },
        outcome=DeclarationRaises(
            exc=SchemaDeclarationError,
            match="Unsupported JSON Schema dialect",
        ),
    ),
    SchemaDeclarationCase(
        id="remote-reference",
        schema={"$ref": "https://schemas.example/record.json"},
        outcome=DeclarationRaises(
            exc=SchemaDeclarationError,
            match="Remote JSON Schema references are not supported",
        ),
    ),
    SchemaDeclarationCase(
        id="unresolved-local-reference",
        schema={"$ref": "#/$defs/missing"},
        outcome=DeclarationRaises(
            exc=SchemaDeclarationError,
            match="Unresolved local JSON Schema references",
        ),
    ),
    SchemaDeclarationCase(
        id="invalid-schema",
        schema={"type": "not-a-json-schema-type"},
        outcome=DeclarationRaises(
            exc=SchemaDeclarationError,
            match="Invalid JSON Schema",
        ),
    ),
    SchemaDeclarationCase(
        id="remote-dynamic-reference",
        schema={"$dynamicRef": "https://schemas.example/private"},
        outcome=DeclarationRaises(exc=SchemaDeclarationError, match="Remote JSON Schema"),
    ),
    SchemaDeclarationCase(
        id="local-dynamic-anchor",
        schema={
            "$defs": {"record": {"$dynamicAnchor": "record", "type": "object"}},
            "$dynamicRef": "#record",
        },
        outcome=DeclarationReturns(),
    ),
    SchemaDeclarationCase(
        id="unresolved-dynamic-reference",
        schema={"$dynamicRef": "#missing"},
        outcome=DeclarationRaises(exc=SchemaDeclarationError, match="Unresolved local JSON Schema"),
    ),
    SchemaDeclarationCase(
        id="https-identifier-local-reference",
        schema={
            "$id": "https://schemas.example/local",
            "$defs": {"record": {"type": "object"}},
            "$ref": "#/$defs/record",
        },
        outcome=DeclarationReturns(),
    ),
    SchemaDeclarationCase(
        id="nested-resource-local-reference",
        schema={
            "$defs": {
                "record": {
                    "$id": "https://schemas.example/nested",
                    "$defs": {"value": {"type": "string"}},
                    "$ref": "#/$defs/value",
                }
            }
        },
        outcome=DeclarationReturns(),
    ),
    SchemaDeclarationCase(
        id="nested-reference-cannot-use-parent-anchor",
        schema={
            "$defs": {
                "value": {"$anchor": "parent", "type": "string"},
                "record": {"$id": "https://schemas.example/nested", "$ref": "#parent"},
            }
        },
        outcome=DeclarationRaises(exc=SchemaDeclarationError, match="Unresolved local JSON Schema"),
    ),
    SchemaDeclarationCase(
        id="reference-like-instance-data",
        schema={"const": {"$ref": "https://schemas.example/instance-data"}},
        outcome=DeclarationReturns(),
    ),
]


class TestSchemaDeclarations:
    @pytest.mark.parametrize(
        "case",
        SCHEMA_DECLARATION_CASES,
        ids=lambda case: case.id,
    )
    def test_declaration(self, case: SchemaDeclarationCase):
        if isinstance(case.outcome, DeclarationRaises):
            with pytest.raises(case.outcome.exc, match=case.outcome.match):
                validate_contract_declaration(case.schema)
        else:
            identity = validate_contract_declaration(case.schema)
            assert identity.startswith(case.outcome.value_prefix)

    def test_identity_is_canonical_and_content_sensitive(self):
        left = validate_contract_declaration(
            {
                "type": "object",
                "properties": {"id": {"type": "string"}},
            }
        )
        reordered = validate_contract_declaration(
            {
                "properties": {"id": {"type": "string"}},
                "type": "object",
            }
        )
        changed = validate_contract_declaration(
            {
                "type": "object",
                "properties": {"id": {"type": "integer"}},
            }
        )

        assert left == reordered
        assert left != changed

    def test_runtime_never_retrieves_a_remote_dynamic_reference(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import urllib.request

        attempts: list[object] = []

        def deny_network(*args: object, **kwargs: object) -> None:
            attempts.append(args)
            raise AssertionError("Schema validation attempted network access")

        monkeypatch.setattr(urllib.request, "urlopen", deny_network)
        try:
            with pytest.raises(SchemaDeclarationError, match="Remote JSON Schema"):
                validate_payload(
                    {"$dynamicRef": "https://schemas.example/private"},
                    {},
                    direction="input",
                    step_name="offline",
                )
        finally:
            assert attempts == []


@dataclass(frozen=True, kw_only=True)
class SchemaPathCase:
    id: str
    path: tuple[str | int, ...]
    exists: bool | None
    value_types: frozenset[SchemaValueType] | None = None


NESTED_SCHEMA = {
    "$defs": {
        "record": {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                    },
                },
                "count": {"type": "integer"},
            },
        }
    },
    "$ref": "#/$defs/record",
}


SCHEMA_PATH_CASES = [
    SchemaPathCase(
        id="nested-array-field",
        path=("items", 0, "id"),
        exists=True,
        value_types=frozenset({SchemaValueType.STRING}),
    ),
    SchemaPathCase(
        id="missing-nested-field",
        path=("items", 0, "missing"),
        exists=False,
    ),
    SchemaPathCase(
        id="traverse-scalar",
        path=("count", "value"),
        exists=False,
    ),
    SchemaPathCase(
        id="named-array-index",
        path=("items", "first"),
        exists=False,
    ),
    SchemaPathCase(
        id="array-value",
        path=("items",),
        exists=True,
        value_types=frozenset({SchemaValueType.ARRAY}),
    ),
]


@pytest.mark.parametrize("case", SCHEMA_PATH_CASES, ids=lambda case: case.id)
def test_inspect_nested_schema_paths(case: SchemaPathCase) -> None:
    result = inspect_schema_path(NESTED_SCHEMA, case.path)

    assert result.exists is case.exists
    if case.value_types is not None:
        assert result.value_types == case.value_types
