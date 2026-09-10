"""Shared grammar for authored identifiers, references, and placeholders."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Annotated, Any, TypeAlias

from pydantic import Field, GetCoreSchemaHandler, StrictStr
from pydantic_core import CoreSchema, PydanticCustomError, core_schema

from justflow.config.provider_names import (
    PROVIDER_NAME_MAX_LENGTH,
    PROVIDER_NAME_PATTERN_TEXT,
)

MAX_IDENTIFIER_LENGTH = 128
MAX_REFERENCE_COMPONENTS = 33
MAX_REFERENCE_COMPONENT_LENGTH = 128
IDENTIFIER_PATTERN_TEXT = r"^[A-Za-z_][A-Za-z0-9_]*$"
REFERENCE_PATH_PATTERN_TEXT = (
    r"^[A-Za-z_][A-Za-z0-9_]{0,127}"
    r"(?:\.(?:[A-Za-z_][A-Za-z0-9_]{0,127}|0|[1-9][0-9]{0,127})){0,32}$"
)
REFERENCE_COMPONENT_PATTERN = re.compile(IDENTIFIER_PATTERN_TEXT)
RESERVED_DATA_ROOTS = frozenset({"error", "input", "request_id"})

Identifier = Annotated[
    StrictStr,
    Field(
        min_length=1,
        max_length=MAX_IDENTIFIER_LENGTH,
        pattern=IDENTIFIER_PATTERN_TEXT,
    ),
]
WorkflowName = Identifier
StepName = Identifier
OperationName = Identifier
AliasName = Identifier
ParameterName = Identifier
ResourceName = Identifier
ServiceName = Identifier
TriggerName = Identifier
SignalName = Identifier
ProviderName = Annotated[
    StrictStr,
    Field(
        min_length=1,
        max_length=PROVIDER_NAME_MAX_LENGTH,
        pattern=PROVIDER_NAME_PATTERN_TEXT,
    ),
]
ReferenceToken: TypeAlias = str | int


class ReferenceSyntaxError(ValueError):
    """A reference path cannot be parsed without ambiguity."""


class PlaceholderSyntaxError(ValueError):
    """A placeholder expression is malformed or has an invalid name."""


@dataclass(frozen=True, slots=True)
class ReferencePath:
    value: str
    components: tuple[ReferenceToken, ...]

    @property
    def root(self) -> str:
        root = self.components[0]
        if not isinstance(root, str):
            raise ReferenceSyntaxError("reference root must be an identifier")
        return root

    @property
    def path(self) -> tuple[ReferenceToken, ...]:
        return self.components[1:]

    def __str__(self) -> str:
        return self.value

    @classmethod
    def parse(cls, value: object) -> ReferencePath:
        if isinstance(value, cls):
            return value
        if type(value) is not str:
            raise ReferenceSyntaxError("reference must be a string")
        parts = value.split(".")
        if len(parts) > MAX_REFERENCE_COMPONENTS:
            raise ReferenceSyntaxError(
                f"reference contains more than {MAX_REFERENCE_COMPONENTS} components"
            )
        components: list[ReferenceToken] = []
        for index, part in enumerate(parts):
            if not part:
                raise ReferenceSyntaxError("reference components must not be empty")
            if len(part) > MAX_REFERENCE_COMPONENT_LENGTH:
                raise ReferenceSyntaxError(
                    f"reference component exceeds {MAX_REFERENCE_COMPONENT_LENGTH} characters"
                )
            if index > 0 and part.isdecimal():
                if len(part) > 1 and part.startswith("0"):
                    raise ReferenceSyntaxError(
                        "numeric reference components must not contain leading zeroes"
                    )
                components.append(int(part))
            elif REFERENCE_COMPONENT_PATTERN.fullmatch(part) is not None:
                components.append(part)
            else:
                raise ReferenceSyntaxError(
                    f"reference component {part!r} must be an identifier or array index"
                )
        return cls(value=value, components=tuple(components))

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        _handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        string_schema = core_schema.str_schema(
            strict=True,
            min_length=1,
            max_length=(MAX_REFERENCE_COMPONENT_LENGTH + 1) * MAX_REFERENCE_COMPONENTS,
            pattern=REFERENCE_PATH_PATTERN_TEXT,
        )

        def validate(value: object) -> ReferencePath:
            try:
                return cls.parse(value)
            except ReferenceSyntaxError as exc:
                raise PydanticCustomError(
                    "reference_path",
                    "invalid reference path: {detail}",
                    {"detail": str(exc)},
                ) from exc

        return core_schema.no_info_plain_validator_function(
            validate,
            json_schema_input_schema=string_schema,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda reference: reference.value,
                return_schema=string_schema,
            ),
        )


@dataclass(frozen=True, slots=True)
class Placeholder:
    name: str
    start: int
    end: int


def parse_placeholders(value: str) -> tuple[Placeholder, ...]:
    placeholders: list[Placeholder] = []
    index = 0
    while index < len(value):
        start = value.find("${", index)
        if start < 0:
            break
        end = value.find("}", start + 2)
        if end < 0:
            raise PlaceholderSyntaxError(f"placeholder beginning at character {start} is unclosed")
        name = value[start + 2 : end]
        if REFERENCE_COMPONENT_PATTERN.fullmatch(name) is None:
            raise PlaceholderSyntaxError(
                f"placeholder at character {start} has invalid name {name!r}"
            )
        placeholders.append(Placeholder(name=name, start=start, end=end + 1))
        index = end + 1
    return tuple(placeholders)


def validate_placeholders(value: Any, *, path: str) -> None:
    if isinstance(value, str):
        try:
            parse_placeholders(value)
        except PlaceholderSyntaxError as exc:
            raise ValueError(f"{path}: {exc}") from exc
        return
    if isinstance(value, dict):
        for key, item in value.items():
            validate_placeholders(item, path=f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            validate_placeholders(item, path=f"{path}.{index}")


def placeholder_names(value: Any) -> frozenset[str]:
    if isinstance(value, str):
        return frozenset(placeholder.name for placeholder in parse_placeholders(value))
    if isinstance(value, dict):
        return frozenset(name for item in value.values() for name in placeholder_names(item))
    if isinstance(value, list):
        return frozenset(name for item in value for name in placeholder_names(item))
    return frozenset()


def validate_data_roots(names: list[str], *, kind: str) -> None:
    collisions = sorted(RESERVED_DATA_ROOTS.intersection(names))
    if collisions:
        raise ValueError(f"{kind} names collide with reserved engine roots: {collisions}")
