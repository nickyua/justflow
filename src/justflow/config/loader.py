"""Bounded, lossless loading for authored configuration."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeVar

import yaml
from pydantic import ValidationError
from yaml.composer import ComposerError
from yaml.constructor import ConstructorError
from yaml.error import MarkedYAMLError
from yaml.events import AliasEvent
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode

from justflow.config.diagnostics import (
    MAX_DIAGNOSTIC_MESSAGE_LENGTH,
    DiagnosticCategory,
    ValidationDiagnostic,
)
from justflow.config.models import ResourcesConfig, ServicesConfig, WorkflowConfig
from justflow.config.triggers import TriggersConfig
from justflow.engine.data import DataNormalizationError, normalize_json_object
from justflow.engine.limits import LimitExceededError

SUPPORTED_WORKFLOW_SUFFIX = ".yaml"
MAX_CONFIG_FILE_BYTES = 1024 * 1024
MAX_YAML_ALIASES = 100
MAX_YAML_DEPTH = 32
MAX_YAML_COLLECTION_ITEMS = 10_000
MAX_YAML_EXPANDED_NODES = 50_000
MAX_REPORTED_KEY_LENGTH = 128
MAX_DECLARATION_ERRORS = 20
CONFIG_FILE_NAMES = frozenset({"resources.yaml", "services.yaml", "triggers.yaml"})
CONFIG_DIRECTORY_NAMES = frozenset({"definitions", "workflows"})
LEGACY_SCHEDULES_FILENAME = "schedules.yaml"
TRIGGERS_FILENAME = "triggers.yaml"
ConfigModel = TypeVar(
    "ConfigModel",
    ResourcesConfig,
    TriggersConfig,
    ServicesConfig,
    WorkflowConfig,
)


class ConfigLoadError(Exception):
    """Authored configuration cannot be loaded without losing intent."""

    def __init__(
        self,
        source: Path,
        detail: str,
        *,
        location: tuple[str | int, ...] = (),
        cause: BaseException | None = None,
    ) -> None:
        self.source = source
        self.detail = detail[:MAX_DIAGNOSTIC_MESSAGE_LENGTH]
        self.location = location
        self.cause = cause
        super().__init__(f"{source}: {self.detail}")

    def as_diagnostic(self) -> ValidationDiagnostic:
        return ValidationDiagnostic(
            source_file=str(self.source),
            location=self.location,
            category=DiagnosticCategory.DECLARATION,
            message=self.detail,
            cause=self.cause,
        )


class _LosslessSafeLoader(yaml.SafeLoader):
    def __init__(self, stream: str) -> None:
        super().__init__(stream)
        self.alias_count = 0
        self.compose_depth = 0

    def compose_node(self, parent: Node | None, index: int | None) -> Node:
        self.compose_depth += 1
        try:
            if self.compose_depth > MAX_YAML_DEPTH:
                event = self.peek_event()
                raise ComposerError(
                    "while composing configuration",
                    None,
                    f"YAML nesting exceeds {MAX_YAML_DEPTH}",
                    event.start_mark,
                )
            if self.check_event(AliasEvent):
                self.alias_count += 1
                if self.alias_count > MAX_YAML_ALIASES:
                    event = self.peek_event()
                    raise ComposerError(
                        "while composing configuration",
                        None,
                        f"YAML alias count exceeds {MAX_YAML_ALIASES}",
                        event.start_mark,
                    )
            return super().compose_node(parent, index)
        finally:
            self.compose_depth -= 1

    def construct_mapping(self, node: MappingNode, deep: bool = False) -> dict[Any, Any]:
        if not isinstance(node, MappingNode):
            raise ConstructorError(
                None,
                None,
                f"expected a mapping node, found {node.id}",
                node.start_mark,
            )
        mapping: dict[Any, Any] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                duplicate = key in mapping
            except TypeError as exc:
                raise ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    "found an unhashable mapping key",
                    key_node.start_mark,
                ) from exc
            if duplicate:
                key_text = str(key)
                if len(key_text) > MAX_REPORTED_KEY_LENGTH:
                    key_text = f"{key_text[:MAX_REPORTED_KEY_LENGTH]}..."
                raise ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"duplicate key {key_text!r}",
                    key_node.start_mark,
                )
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


class ConfigLoader:
    """Loads resources, services, and workflow declarations from one directory."""

    def __init__(self, config_dir: str | Path):
        self.config_dir = Path(config_dir)
        self._workflow_sources: dict[str, Path] = {}

    @property
    def workflow_sources(self) -> Mapping[str, Path]:
        return MappingProxyType(self._workflow_sources)

    def load_resources(self) -> ResourcesConfig:
        self._validate_config_layout()
        path = self.config_dir / "resources.yaml"
        return self._validate_model(path, ResourcesConfig, self._load_yaml(path))

    def load_services(self) -> ServicesConfig:
        self._validate_config_layout()
        path = self.config_dir / "services.yaml"
        return self._validate_model(path, ServicesConfig, self._load_yaml(path))

    def load_workflows(self) -> dict[str, WorkflowConfig]:
        return self._load_workflows(require_non_empty=True)

    def load_triggers(self) -> TriggersConfig:
        self._validate_config_layout()
        path = self.config_dir / TRIGGERS_FILENAME
        return self._validate_model(path, TriggersConfig, self._load_yaml(path))

    def inspect_workflows(self) -> dict[str, WorkflowConfig]:
        """Load workflow declarations while allowing an empty directory."""
        return self._load_workflows(require_non_empty=False)

    @classmethod
    def inspect_workflow_file(cls, path: str | Path) -> WorkflowConfig:
        source = Path(path)
        return cls._validate_model(source, WorkflowConfig, cls._load_yaml(source))

    def load_all(self) -> tuple[ResourcesConfig, ServicesConfig, dict[str, WorkflowConfig]]:
        self._validate_config_layout()
        resources_path = self.config_dir / "resources.yaml"
        services_path = self.config_dir / "services.yaml"
        resources = self._validate_model(
            resources_path,
            ResourcesConfig,
            self._load_yaml(resources_path),
        )
        services = self._validate_model(
            services_path,
            ServicesConfig,
            self._load_yaml(services_path),
        )
        workflows = self._load_workflows(require_non_empty=True, layout_validated=True)
        return resources, services, workflows

    def _load_workflows(
        self,
        *,
        require_non_empty: bool,
        layout_validated: bool = False,
    ) -> dict[str, WorkflowConfig]:
        if not layout_validated:
            self._validate_config_layout()
        workflows_dir = self.config_dir / "workflows"
        if not workflows_dir.exists():
            if require_non_empty:
                raise ConfigLoadError(workflows_dir, "workflow directory is required")
            return {}
        if not workflows_dir.is_dir():
            raise ConfigLoadError(workflows_dir, "workflow path must be a directory")

        workflow_files = sorted(workflows_dir.iterdir())
        unsupported_files = [
            path
            for path in workflow_files
            if not path.is_file() or path.suffix != SUPPORTED_WORKFLOW_SUFFIX
        ]
        if unsupported_files:
            paths = ", ".join(str(path) for path in unsupported_files)
            raise ConfigLoadError(
                workflows_dir,
                f"workflow directory contains unsupported entries: {paths}; "
                f"workflow files must use '{SUPPORTED_WORKFLOW_SUFFIX}'",
            )
        if require_non_empty and not workflow_files:
            raise ConfigLoadError(workflows_dir, "at least one workflow file is required")

        workflows: dict[str, WorkflowConfig] = {}
        workflow_sources: dict[str, Path] = {}
        for yaml_file in workflow_files:
            raw = self._load_yaml(yaml_file)
            workflow = self._validate_model(yaml_file, WorkflowConfig, raw)
            if workflow.workflow in workflows:
                raise ConfigLoadError(
                    yaml_file,
                    f"duplicate workflow name '{workflow.workflow}' also declared in "
                    f"'{workflow_sources[workflow.workflow]}'",
                )
            workflows[workflow.workflow] = workflow
            workflow_sources[workflow.workflow] = yaml_file
        self._workflow_sources = workflow_sources
        return workflows

    def _validate_config_layout(self) -> None:
        if not self.config_dir.exists():
            raise ConfigLoadError(self.config_dir, "configuration directory does not exist")
        if not self.config_dir.is_dir():
            raise ConfigLoadError(self.config_dir, "configuration path must be a directory")

        legacy_schedules = self.config_dir / LEGACY_SCHEDULES_FILENAME
        if legacy_schedules.exists():
            raise ConfigLoadError(
                legacy_schedules,
                "schedules.yaml was replaced by triggers.yaml; rename the file, rename its "
                "top-level schedules key to triggers, and add 'kind: schedule' to every entry",
            )

        unsupported = sorted(
            path
            for path in self.config_dir.iterdir()
            if path.name not in CONFIG_FILE_NAMES | CONFIG_DIRECTORY_NAMES
            or path.name in CONFIG_FILE_NAMES
            and not path.is_file()
            or path.name in CONFIG_DIRECTORY_NAMES
            and not path.is_dir()
        )
        if unsupported:
            paths = ", ".join(str(path) for path in unsupported)
            raise ConfigLoadError(
                self.config_dir,
                f"configuration directory contains unsupported entries: {paths}",
            )

    @staticmethod
    def _validate_model(
        path: Path,
        model: type[ConfigModel],
        raw: dict[str, Any],
    ) -> ConfigModel:
        try:
            return model.model_validate(raw)
        except ValidationError as exc:
            errors = exc.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            )
            rendered = [
                f"{'.'.join(str(component) for component in error['loc'])}: {error['msg']}"
                for error in errors[:MAX_DECLARATION_ERRORS]
            ]
            if len(errors) > MAX_DECLARATION_ERRORS:
                rendered.append(
                    f"{len(errors) - MAX_DECLARATION_ERRORS} additional declaration errors"
                )
            first_location = tuple(errors[0]["loc"]) if errors else ()
            raise ConfigLoadError(
                path,
                "declaration is invalid: " + "; ".join(rendered),
                location=first_location,
                cause=exc,
            ) from exc

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        if not path.exists():
            raise ConfigLoadError(path, "configuration file is required")
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise ConfigLoadError(
                path,
                f"cannot read configuration file: {exc}",
                cause=exc,
            ) from exc

        return load_bounded_yaml(payload, source=path)


def load_bounded_yaml(
    payload: bytes,
    *,
    source: str | Path,
) -> dict[str, Any]:
    path = Path(source)
    if len(payload) > MAX_CONFIG_FILE_BYTES:
        raise ConfigLoadError(
            path,
            f"configuration file exceeds {MAX_CONFIG_FILE_BYTES} bytes",
        )
    try:
        yaml_source = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigLoadError(
            path,
            "configuration file must be UTF-8",
            cause=exc,
        ) from exc

    loader = _LosslessSafeLoader(yaml_source)
    try:
        node = loader.get_single_node()
        if node is None:
            raise ConfigLoadError(path, "configuration file must not be empty")
        _validate_yaml_node(node)
        loaded = loader.construct_document(node)
        return normalize_json_object(
            loaded,
            path=str(path),
            max_depth=MAX_YAML_DEPTH,
            max_collection_items=MAX_YAML_COLLECTION_ITEMS,
        )
    except ConfigLoadError:
        raise
    except yaml.YAMLError as exc:
        detail, location = _safe_yaml_error(exc)
        raise ConfigLoadError(
            path,
            detail,
            location=location,
            cause=exc,
        ) from exc
    except (DataNormalizationError, LimitExceededError) as exc:
        raise ConfigLoadError(path, str(exc), cause=exc) from exc
    finally:
        loader.dispose()


def _validate_yaml_node(node: Node) -> None:
    expanded_nodes = 0

    def visit(current: Node, *, depth: int, ancestors: frozenset[int]) -> None:
        nonlocal expanded_nodes
        if depth > MAX_YAML_DEPTH:
            raise ConstructorError(
                "while validating configuration",
                None,
                f"YAML nesting exceeds {MAX_YAML_DEPTH}",
                current.start_mark,
            )
        if id(current) in ancestors:
            raise ConstructorError(
                "while validating configuration",
                None,
                "recursive YAML aliases are not supported",
                current.start_mark,
            )
        expanded_nodes += 1
        if expanded_nodes > MAX_YAML_EXPANDED_NODES:
            raise ConstructorError(
                "while validating configuration",
                None,
                f"expanded YAML node count exceeds {MAX_YAML_EXPANDED_NODES}",
                current.start_mark,
            )

        next_ancestors = ancestors | {id(current)}
        if isinstance(current, MappingNode):
            if len(current.value) > MAX_YAML_COLLECTION_ITEMS:
                raise ConstructorError(
                    "while validating configuration",
                    None,
                    f"mapping contains more than {MAX_YAML_COLLECTION_ITEMS} items",
                    current.start_mark,
                )
            for key, value in current.value:
                visit(key, depth=depth + 1, ancestors=next_ancestors)
                visit(value, depth=depth + 1, ancestors=next_ancestors)
        elif isinstance(current, SequenceNode):
            if len(current.value) > MAX_YAML_COLLECTION_ITEMS:
                raise ConstructorError(
                    "while validating configuration",
                    None,
                    f"sequence contains more than {MAX_YAML_COLLECTION_ITEMS} items",
                    current.start_mark,
                )
            for value in current.value:
                visit(value, depth=depth + 1, ancestors=next_ancestors)
        elif not isinstance(current, ScalarNode):
            raise ConstructorError(
                "while validating configuration",
                None,
                f"unsupported YAML node {type(current).__name__}",
                current.start_mark,
            )

    visit(node, depth=0, ancestors=frozenset())


def _safe_yaml_error(error: yaml.YAMLError) -> tuple[str, tuple[str | int, ...]]:
    if isinstance(error, MarkedYAMLError):
        problem = error.problem or "invalid YAML"
        if error.problem_mark is not None:
            line = error.problem_mark.line + 1
            column = error.problem_mark.column + 1
            return f"{problem} at line {line}, column {column}", ("yaml", line, column)
        return problem, ("yaml",)
    return "invalid YAML", ("yaml",)
