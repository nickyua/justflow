"""Stable public diagnostics for authored configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TypeAlias

MAX_DIAGNOSTICS = 1_000
MAX_DIAGNOSTIC_MESSAGE_LENGTH = 2_048
MAX_DIAGNOSTIC_JSON_BYTES = 3 * 1024 * 1024
LocationComponent: TypeAlias = str | int


class DiagnosticSeverity(str, Enum):
    ERROR = "error"
    WARNING = "warning"


class DiagnosticCategory(str, Enum):
    DECLARATION = "declaration"
    IMPORT = "import"
    LIMIT = "limit"
    LINT = "lint"
    REFERENCE = "reference"
    SEMANTIC = "semantic"
    PROVIDER = "provider"


@dataclass(frozen=True, slots=True)
class ValidationDiagnostic:
    source_file: str
    location: tuple[LocationComponent, ...]
    category: DiagnosticCategory
    message: str
    severity: DiagnosticSeverity = DiagnosticSeverity.ERROR
    cause: BaseException | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if len(self.message) > MAX_DIAGNOSTIC_MESSAGE_LENGTH:
            raise ValueError(
                f"Diagnostic message exceeds {MAX_DIAGNOSTIC_MESSAGE_LENGTH} characters"
            )

    @property
    def location_text(self) -> str:
        return ".".join(str(component) for component in self.location)

    @property
    def sort_key(self) -> tuple[str, tuple[str, ...], str, str, str]:
        return (
            self.source_file,
            tuple(str(component) for component in self.location),
            self.severity.value,
            self.category.value,
            self.message,
        )

    def __str__(self) -> str:
        return f"[{self.source_file}:{self.location_text}] {self.category.value}: {self.message}"

    def as_dict(self) -> dict[str, object]:
        return {
            "category": self.category.value,
            "location": list(self.location),
            "message": self.message,
            "severity": self.severity.value,
            "source_file": self.source_file,
        }
