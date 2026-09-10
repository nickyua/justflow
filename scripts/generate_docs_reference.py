"""Generate public reference pages from the shipped interfaces."""

from __future__ import annotations

import argparse
import inspect
import json
import os
import re
import subprocess
import sys
import types
from collections.abc import Iterable
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import Annotated, Union, get_args, get_origin

from pydantic import BaseModel, SecretStr
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from justflow.config.diagnostics import DiagnosticCategory, DiagnosticSeverity
from justflow.config.settings import ENV_NESTED_DELIMITER, ENV_PREFIX, Settings
from justflow.openapi import OPENAPI_FILE_NAME, OPENAPI_VERSION, public_api_routes
from justflow.provenance import installed_engine_version
from justflow.runtime.auth import AuthorizationAction
from justflow.runtime.cloud_events import CloudEventErrorCode
from justflow.runtime.operations import ControlErrorCode
from justflow.runtime.operations_query import OperationsQueryErrorCode
from justflow.runtime.schedule_operations import ScheduleOperationErrorCode
from justflow.runtime.schedule_reconciler import (
    ScheduleApplyErrorCode,
    ScheduleReconciliationErrorCode,
)
from justflow.runtime.scheduled_starts import ScheduledStartErrorCode
from justflow.runtime.starter import StartErrorCode
from justflow.runtime.webhooks import WebhookErrorCode
from justflow.schemas import (
    AUTHORING_SCHEMA_VERSION,
    SCHEMA_FILE_NAMES,
    load_bundled_schemas,
)

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
GENERATED_DIRECTORY = REPOSITORY_ROOT / "docs" / "reference" / "generated"
COMMAND_TIMEOUT_SECONDS = 30
CLI_TERMINAL_COLUMNS = 80
CLI_HELP_WIDTH = CLI_TERMINAL_COLUMNS - 2
INSTALLED_PACKAGE_VERSION_DEFAULT = "installed version or null"
UNION_TYPE_SUMMARY = "Represent a union type"
CLI_COMPACT_OPTION_PATTERN = re.compile(
    r"^(?P<indent>\s+)(?P<short>-[^\s,]+), "
    r"(?P<long>--[^\s]+) (?P<metavar>[A-Z][A-Z0-9_-]*)"
    r"(?P<spacing>\s{2,})(?P<description>\S.*)$"
)
CLI_SUBCOMMAND_USAGE_PATTERN = re.compile(r"^(?P<indent>\s+)(?P<choices>\{[^}]+\}) \.\.\.$")
CLI_REQUIRED_OPTION_USAGE_PATTERN = re.compile(
    r"^(?P<indent>\s+)(?P<option>--[^\s]+) "
    r"(?P<metavar>[A-Z][A-Z0-9_-]*)(?P<suffix> .*)?$"
)
CLI_HELP_COMMANDS = (
    ("justflow --help", ("--help",)),
    ("justflow worker --help", ("worker", "--help")),
    ("justflow worker-server --help", ("worker-server", "--help")),
    ("justflow api --help", ("api", "--help")),
    ("justflow api schema --help", ("api", "schema", "--help")),
    ("justflow api schema export --help", ("api", "schema", "export", "--help")),
    ("justflow serve --help", ("serve", "--help")),
    ("justflow validate --help", ("validate", "--help")),
    ("justflow graph --help", ("graph", "--help")),
    ("justflow run --help", ("run", "--help")),
    ("justflow definitions --help", ("definitions", "--help")),
    ("justflow definitions publish --help", ("definitions", "publish", "--help")),
    ("justflow definitions export --help", ("definitions", "export", "--help")),
    ("justflow definitions import --help", ("definitions", "import", "--help")),
    ("justflow definitions migrate --help", ("definitions", "migrate", "--help")),
    (
        "justflow definitions migrate-scope --help",
        ("definitions", "migrate-scope", "--help"),
    ),
    ("justflow triggers --help", ("triggers", "--help")),
    ("justflow triggers plan --help", ("triggers", "plan", "--help")),
    ("justflow triggers apply --help", ("triggers", "apply", "--help")),
    ("justflow triggers list --help", ("triggers", "list", "--help")),
    ("justflow triggers describe --help", ("triggers", "describe", "--help")),
    ("justflow triggers pause --help", ("triggers", "pause", "--help")),
    ("justflow triggers resume --help", ("triggers", "resume", "--help")),
    (
        "justflow triggers trigger-now --help",
        ("triggers", "trigger-now", "--help"),
    ),
    ("justflow triggers backfill --help", ("triggers", "backfill", "--help")),
    ("justflow triggers delete --help", ("triggers", "delete", "--help")),
    ("justflow schema --help", ("schema", "--help")),
    ("justflow schema export --help", ("schema", "export", "--help")),
)
PUBLIC_FACADE_MODULES = (
    "justflow.sdk",
    "justflow.runtime",
    "justflow.resources",
    "justflow.transports",
    "justflow.brokers",
    "justflow.configuration",
    "justflow.definitions",
    "justflow.schemas",
    "justflow.openapi",
    "justflow.visualization",
)
ERROR_ENUMS = (
    ("Workflow start", StartErrorCode),
    ("Workflow control", ControlErrorCode),
    ("Operations query", OperationsQueryErrorCode),
    ("Schedule operation", ScheduleOperationErrorCode),
    ("Schedule reconciliation", ScheduleReconciliationErrorCode),
    ("Schedule apply", ScheduleApplyErrorCode),
    ("Scheduled start", ScheduledStartErrorCode),
    ("Webhook", WebhookErrorCode),
    ("CloudEvent", CloudEventErrorCode),
)


def _clean_environment() -> dict[str, str]:
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith(ENV_PREFIX)
    }
    environment["COLUMNS"] = str(CLI_TERMINAL_COLUMNS)
    return environment


def _normalize_cli_help(output: str) -> str:
    lines = output.rstrip().splitlines()
    normalized: list[str] = []
    in_usage = False
    for line in lines:
        if line.startswith("usage: "):
            in_usage = True
        elif not line:
            in_usage = False

        subcommand_usage = CLI_SUBCOMMAND_USAGE_PATTERN.fullmatch(line)
        if in_usage and subcommand_usage is not None and len(line) > CLI_HELP_WIDTH:
            normalized.extend(
                (
                    f"{subcommand_usage['indent']}{subcommand_usage['choices']}",
                    f"{subcommand_usage['indent']}...",
                )
            )
            continue

        required_option = CLI_REQUIRED_OPTION_USAGE_PATTERN.fullmatch(line)
        if in_usage and required_option is not None and normalized:
            option_on_previous_line = f"{normalized[-1]} {required_option['option']}"
            complete_option = f"{option_on_previous_line} {required_option['metavar']}"
            if len(option_on_previous_line) <= CLI_HELP_WIDTH < len(complete_option):
                normalized[-1] = option_on_previous_line
                line = (
                    f"{required_option['indent']}{required_option['metavar']}"
                    f"{required_option['suffix'] or ''}"
                )

        compact_option = CLI_COMPACT_OPTION_PATTERN.fullmatch(line)
        if compact_option is not None:
            declaration = (
                f"{compact_option['indent']}{compact_option['short']} "
                f"{compact_option['metavar']}, {compact_option['long']} "
                f"{compact_option['metavar']}"
            )
            description_column = line.index(compact_option["description"])
            if len(declaration) < description_column:
                line = f"{declaration:<{description_column}}{compact_option['description']}"
            else:
                normalized.append(declaration)
                line = f"{'':<{description_column}}{compact_option['description']}"
        normalized.append(line)
    return "\n".join(normalized)


def _render_cli_reference() -> str:
    sections = [
        "# CLI reference",
        "",
        "This page is generated by executing every public help path offline.",
        "Environment defaults are documented in [Settings and environment](settings.md).",
        "",
    ]
    environment = _clean_environment()
    for title, arguments in CLI_HELP_COMMANDS:
        result = subprocess.run(
            [sys.executable, "-m", "justflow", *arguments],
            cwd=REPOSITORY_ROOT,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
        sections.extend(
            (
                f"## `{title}`",
                "",
                "```text",
                _normalize_cli_help(result.stdout),
                "```",
                "",
            )
        )
    return "\n".join(sections)


def _unwrap(annotation: object) -> object:
    if get_origin(annotation) is Annotated:
        return get_args(annotation)[0]
    return annotation


def _model_variants(annotation: object) -> tuple[type[BaseModel], ...]:
    annotation = _unwrap(annotation)
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return (annotation,)
    if get_origin(annotation) in (types.UnionType, Union):
        return tuple(
            item
            for item in get_args(annotation)
            if isinstance(item, type) and issubclass(item, BaseModel)
        )
    return ()


def _field_default(field: FieldInfo) -> object:
    if field.default is not PydanticUndefined:
        return field.default
    if field.default_factory is not None:
        return field.get_default(call_default_factory=True)
    return PydanticUndefined


def _settings_field_default(
    field: FieldInfo,
    default_model: BaseModel | None,
    name: str,
) -> object:
    if field.default_factory is installed_engine_version:
        return INSTALLED_PACKAGE_VERSION_DEFAULT
    return getattr(default_model, name) if default_model is not None else _field_default(field)


def _display_type(annotation: object) -> str:
    annotation = _unwrap(annotation)
    if isinstance(annotation, type):
        return annotation.__name__
    origin = get_origin(annotation)
    if origin in (types.UnionType, Union):
        return " | ".join(_display_type(item) for item in get_args(annotation))
    if origin is not None:
        name = getattr(origin, "__name__", str(origin).replace("typing.", ""))
        arguments = ", ".join(_display_type(item) for item in get_args(annotation))
        return f"{name}[{arguments}]"
    return str(annotation).replace("typing.", "")


def _display_default(value: object) -> str:
    if value is PydanticUndefined:
        return "required"
    if isinstance(value, SecretStr):
        return "unset" if not value.get_secret_value() else "redacted"
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, Path):
        return str(value)
    if value is None:
        return "null"
    if isinstance(value, (set, frozenset, tuple, list)):
        normalized = sorted(item.value if isinstance(item, Enum) else item for item in value)
        return json.dumps(normalized, separators=(",", ":"))
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value).lower() if isinstance(value, bool) else str(value)


def _escape_cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _settings_rows(
    model: type[BaseModel],
    prefix: tuple[str, ...],
    default_model: BaseModel | None,
    variant_path: tuple[str, ...],
) -> list[tuple[str, str, str, str]]:
    rows: list[tuple[str, str, str, str]] = []
    for name, field in model.model_fields.items():
        environment_name = ENV_PREFIX + ENV_NESTED_DELIMITER.join(
            part.upper() for part in (*prefix, name)
        )
        default = _settings_field_default(field, default_model, name)
        variants = _model_variants(field.annotation)
        if variants:
            for variant in variants:
                variant_default = default if isinstance(default, variant) else None
                rows.extend(
                    _settings_rows(
                        variant,
                        (*prefix, name),
                        variant_default,
                        (*variant_path, variant.__name__),
                    )
                )
            continue
        rows.append(
            (
                environment_name,
                _display_type(field.annotation),
                _display_default(default),
                " → ".join(variant_path) if variant_path else "all modes",
            )
        )
    return rows


def render_settings_reference() -> str:
    setting_defaults = {
        name: _field_default(field) for name, field in Settings.model_fields.items()
    }
    lines = [
        "# Settings and environment",
        "",
        f"Justflow loads one typed settings tree. Environment names start with `{ENV_PREFIX}` ",
        f"and use `{ENV_NESTED_DELIMITER}` between nested fields. Invalid values fail startup.",
        "Secret values are never rendered here; structured dictionaries and lists use JSON.",
        "",
    ]
    for group_name, field in Settings.model_fields.items():
        group_default = setting_defaults[group_name]
        variants = _model_variants(field.annotation)
        lines.extend((f"## `{group_name}`", ""))
        if not variants:
            lines.extend(
                (
                    "This is a structured or provider-defined mapping. Supply it as JSON at ",
                    f"`{ENV_PREFIX}{group_name.upper()}` or use nested fields where supported.",
                    "",
                )
            )
            continue
        rows: list[tuple[str, str, str, str]] = []
        for variant in variants:
            default_model = group_default if isinstance(group_default, variant) else None
            rows.extend(
                _settings_rows(
                    variant,
                    (group_name,),
                    default_model,
                    (variant.__name__,) if len(variants) > 1 else (),
                )
            )
        lines.extend(
            (
                "| Environment variable | Type | Default | Applies to |",
                "| --- | --- | --- | --- |",
            )
        )
        lines.extend(
            "| `{}` | `{}` | `{}` | {} |".format(*(_escape_cell(value) for value in row))
            for row in sorted(set(rows))
        )
        lines.append("")
    return "\n".join(lines)


def _render_error_reference() -> str:
    route_codes = sorted({code for route in public_api_routes() for code in route.error_codes})
    lines = [
        "# Error taxonomy",
        "",
        'HTTP errors use `{"error": {"code": string, "message": string}}`. Treat the ',
        "bounded `code` as machine-readable and the message as operator-facing context. Validation ",
        "diagnostics instead carry source, location, severity, category, and message.",
        "",
        "## Public HTTP codes",
        "",
    ]
    lines.extend(f"- `{code}`" for code in route_codes)
    lines.extend(("", "## Typed runtime codes", ""))
    for title, enum_type in ERROR_ENUMS:
        lines.extend((f"### {title}", "", ", ".join(f"`{item.value}`" for item in enum_type), ""))
    lines.extend(
        (
            "## Validation diagnostics",
            "",
            "Severities: " + ", ".join(f"`{item.value}`" for item in DiagnosticSeverity) + ".",
            "",
            "Categories: " + ", ".join(f"`{item.value}`" for item in DiagnosticCategory) + ".",
            "",
        )
    )
    return "\n".join(lines)


def _symbol_kind(value: object) -> str:
    if inspect.isclass(value):
        return "exception" if issubclass(value, BaseException) else "class"
    if inspect.isfunction(value):
        return "function"
    return "constant"


def _symbol_summary(value: object) -> str:
    if get_origin(value) in (types.UnionType, Union):
        return UNION_TYPE_SUMMARY
    doc = inspect.getdoc(value)
    return doc.splitlines()[0] if doc else "Public symbol."


def _render_python_api_reference() -> str:
    lines = [
        "# Public Python facades",
        "",
        "Only the symbols exported by these facade modules are part of the documented Python ",
        "surface. Import through the facade rather than an implementation module.",
        "",
    ]
    for module_name in PUBLIC_FACADE_MODULES:
        module = import_module(module_name)
        exported: Iterable[str] = getattr(module, "__all__", ())
        lines.extend(
            (
                f"## `{module_name}`",
                "",
                "| Symbol | Kind | Summary |",
                "| --- | --- | --- |",
            )
        )
        for name in exported:
            value = getattr(module, name)
            lines.append(
                f"| `{name}` | {_symbol_kind(value)} | {_escape_cell(_symbol_summary(value))} |"
            )
        lines.append("")
    return "\n".join(lines)


def _render_schema_reference() -> str:
    schemas = load_bundled_schemas()
    lines = [
        "# Schemas",
        "",
        f"Authoring schemas are versioned as `{AUTHORING_SCHEMA_VERSION}` and ship in the wheel. ",
        "Export them without network access with:",
        "",
        "<!-- tested: tests/test_schemas.py -->",
        "```console",
        "justflow schema export --output .justflow/schemas",
        "```",
        "",
        "| Declaration | Schema identifier | Installed resource |",
        "| --- | --- | --- |",
    ]
    for file_name in SCHEMA_FILE_NAMES:
        lines.append(
            f"| `{file_name}` | `{schemas[file_name]['$id']}` | "
            f"`justflow.schemas.bundled/{file_name}` |"
        )
    lines.extend(
        (
            "",
            "## HTTP API",
            "",
            (
                f"OpenAPI `{OPENAPI_VERSION}` ships as "
                f"`justflow.openapi.bundled/{OPENAPI_FILE_NAME}`. Export it with:"
            ),
            "",
            "<!-- tested: tests/test_openapi.py -->",
            "```console",
            "justflow api schema export --output .justflow/api",
            "```",
            "",
            "Configure an editor to use the exported file that matches the declaration being ",
            "edited. The export is conflict-safe: it refuses to overwrite different content.",
            "",
        )
    )
    return "\n".join(lines)


def generated_pages() -> dict[Path, str]:
    return {
        GENERATED_DIRECTORY / "cli.md": _render_cli_reference(),
        GENERATED_DIRECTORY / "settings.md": render_settings_reference(),
        GENERATED_DIRECTORY / "errors.md": _render_error_reference(),
        GENERATED_DIRECTORY / "python-api.md": _render_python_api_reference(),
        GENERATED_DIRECTORY / "schemas.md": _render_schema_reference(),
        GENERATED_DIRECTORY / "authorization-actions.md": (
            "# Authorization actions\n\n"
            "Generated from `justflow.runtime.auth.AuthorizationAction`. "
            "Hosts explicitly grant actions within a trusted runtime scope.\n\n"
            "| Enum member | Wire value |\n| --- | --- |\n"
            + "".join(f"| `{action.name}` | `{action.value}` |\n" for action in AuthorizationAction)
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    stale: list[Path] = []
    for path, content in generated_pages().items():
        rendered = content.rstrip() + "\n"
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != rendered:
                stale.append(path)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    if stale:
        joined = ", ".join(str(path.relative_to(REPOSITORY_ROOT)) for path in stale)
        raise SystemExit(f"Generated documentation is stale: {joined}")


if __name__ == "__main__":
    main()
