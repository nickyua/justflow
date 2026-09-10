"""Config validator - cross-validates authored configuration layers at startup."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

from justflow.config._workflow_validation import (
    COLLECTION_SCHEMA_TYPES as _COLLECTION_SCHEMA_TYPES,
)
from justflow.config._workflow_validation import (
    DEADLINE_SCHEMA_TYPES as _DEADLINE_SCHEMA_TYPES,
)
from justflow.config._workflow_validation import INPUT_ROOT as _INPUT_ROOT
from justflow.config._workflow_validation import _WorkflowValidator
from justflow.config.diagnostics import (
    MAX_DIAGNOSTIC_MESSAGE_LENGTH,
    MAX_DIAGNOSTICS,
    DiagnosticCategory,
    DiagnosticSeverity,
    ValidationDiagnostic,
)
from justflow.config.models import (
    ChildWorkflowTarget,
    ResourcesConfig,
    ServicesConfig,
    WorkflowConfig,
)
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.config.triggers import TriggersConfig
from justflow.engine.contracts import ContractIdentity
from justflow.resources.builtins import builtin_resource_registry
from justflow.resources.registry import (
    ResolvedResource,
    ResourceRegistry,
    ResourceRegistryError,
)
from justflow.transports.builtins import builtin_transport_registry
from justflow.transports.registry import (
    ResolvedService,
    TransportRegistry,
    TransportRegistryError,
)


@dataclass
class ValidationResult:
    source_files: Mapping[tuple[str, ...], str] = field(default_factory=dict)
    _diagnostics: list[ValidationDiagnostic] = field(default_factory=list)
    _overflowed: bool = False

    @property
    def diagnostics(self) -> list[ValidationDiagnostic]:
        return sorted(self._diagnostics, key=lambda diagnostic: diagnostic.sort_key)

    @property
    def errors(self) -> list[ValidationDiagnostic]:
        return [
            diagnostic
            for diagnostic in self.diagnostics
            if diagnostic.severity is DiagnosticSeverity.ERROR
        ]

    @property
    def warnings(self) -> list[ValidationDiagnostic]:
        return [
            diagnostic
            for diagnostic in self.diagnostics
            if diagnostic.severity is DiagnosticSeverity.WARNING
        ]

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def add(
        self,
        location: str,
        message: str,
        *,
        category: DiagnosticCategory = DiagnosticCategory.SEMANTIC,
        severity: DiagnosticSeverity = DiagnosticSeverity.ERROR,
        cause: BaseException | None = None,
    ) -> None:
        if len(self._diagnostics) >= MAX_DIAGNOSTICS:
            if not self._overflowed:
                self._overflowed = True
                self._diagnostics[-1] = ValidationDiagnostic(
                    source_file=self._source_for(tuple(location.split("."))),
                    location=("diagnostics",),
                    category=DiagnosticCategory.LIMIT,
                    severity=DiagnosticSeverity.ERROR,
                    message="Additional configuration diagnostics were omitted",
                )
            return
        structured_location = tuple(location.split("."))
        bounded_message = message[:MAX_DIAGNOSTIC_MESSAGE_LENGTH]
        self._diagnostics.append(
            ValidationDiagnostic(
                source_file=self._source_for(structured_location),
                location=structured_location,
                category=category,
                severity=severity,
                message=bounded_message,
                cause=cause,
            )
        )

    def _source_for(self, location: tuple[str, ...]) -> str:
        if len(location) >= 2 and location[0] == "workflows":
            key = location[:2]
        else:
            key = location[:1]
        return self.source_files.get(key, "/".join((*key, "declaration.yaml")))

    def raise_if_invalid(self) -> None:
        if not self.is_valid:
            msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in self.errors)
            raise ConfigValidationError(msg, self.errors)


class ConfigValidationError(Exception):
    def __init__(self, message: str, errors: list[ValidationDiagnostic]):
        super().__init__(message)
        self.errors = errors


class ServicesNotResolvedError(RuntimeError):
    pass


class ResourcesNotResolvedError(RuntimeError):
    pass


INPUT_ROOT = _INPUT_ROOT
COLLECTION_SCHEMA_TYPES = _COLLECTION_SCHEMA_TYPES
DEADLINE_SCHEMA_TYPES = _DEADLINE_SCHEMA_TYPES


class ConfigValidator:
    """Cross-validates resources, services, and workflow configs."""

    def __init__(
        self,
        resources: ResourcesConfig,
        services: ServicesConfig,
        workflows: dict[str, WorkflowConfig],
        check_imports: bool = True,
        transport_registry: TransportRegistry | None = None,
        resource_registry: ResourceRegistry | None = None,
        limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
        config_dir: str | Path | None = None,
        workflow_sources: Mapping[str, Path] | None = None,
        triggers: TriggersConfig | None = None,
    ):
        self.resources = resources
        self.services = services
        self.workflows = workflows
        self.triggers = triggers or TriggersConfig(triggers={})
        self._validate_reachability = triggers is not None
        self.check_imports = check_imports
        self.transport_registry = transport_registry or builtin_transport_registry()
        self.resource_registry = resource_registry or builtin_resource_registry()
        self.limits = limits
        root = Path(config_dir) if config_dir is not None else Path(".")
        self._source_files: dict[tuple[str, ...], str] = {
            ("resources",): str(root / "resources.yaml"),
            ("triggers",): str(root / "triggers.yaml"),
            ("services",): str(root / "services.yaml"),
        }
        for name in workflows:
            source = (
                workflow_sources[name]
                if workflow_sources is not None and name in workflow_sources
                else root / "workflows" / f"{name}.yaml"
            )
            self._source_files[("workflows", name)] = str(source)
        self._contract_identities: dict[str, ContractIdentity] = {}
        self._resolved_resources: dict[str, ResolvedResource] = {}
        self._resolved_services: dict[str, ResolvedService] = {}
        self._resources_resolved = False
        self._services_resolved = False

    @property
    def contract_identities(self) -> Mapping[str, ContractIdentity]:
        return MappingProxyType(self._contract_identities)

    @property
    def resolved_services(self) -> Mapping[str, ResolvedService]:
        if not self._services_resolved:
            raise ServicesNotResolvedError(
                "Services are available only after configuration validation"
            )
        return MappingProxyType(self._resolved_services)

    @property
    def resolved_resources(self) -> Mapping[str, ResolvedResource]:
        if not self._resources_resolved:
            raise ResourcesNotResolvedError(
                "Resources are available only after configuration validation"
            )
        return MappingProxyType(self._resolved_resources)

    def validate(self) -> ValidationResult:
        self._contract_identities.clear()
        self._resolved_resources.clear()
        self._resolved_services.clear()
        self._resources_resolved = False
        self._services_resolved = False
        result = ValidationResult(source_files=self._source_files)
        self._validate_resources(result)
        self._resources_resolved = True
        self._validate_services(result)
        self._services_resolved = True
        workflow_validator = _WorkflowValidator(
            resources=self.resources,
            services=self.services,
            workflows=self.workflows,
            resolved_resources=self._resolved_resources,
            resolved_services=self._resolved_services,
            check_imports=self.check_imports,
            transport_registry=self.transport_registry,
            resource_registry=self.resource_registry,
            limits=self.limits,
            contract_identities=self._contract_identities,
        )
        for name, workflow in self.workflows.items():
            workflow_validator.validate(name, workflow, result)
        self._validate_triggers(result)
        if self._validate_reachability:
            self._validate_workflow_reachability(result)
        self._check_composition_cycles(result)
        return result

    def _validate_triggers(self, result: ValidationResult) -> None:
        for trigger_name, declaration in self.triggers.triggers.items():
            if declaration.workflow not in self.workflows:
                result.add(
                    f"triggers.{trigger_name}.workflow",
                    f"Trigger references unknown workflow '{declaration.workflow}'",
                    category=DiagnosticCategory.REFERENCE,
                )

    def _validate_workflow_reachability(self, result: ValidationResult) -> None:
        triggered = {declaration.workflow for declaration in self.triggers.triggers.values()}
        children = {
            step.target.workflow
            for workflow in self.workflows.values()
            for step in workflow.steps.values()
            if isinstance(step.target, ChildWorkflowTarget)
        }
        for workflow_name in sorted(set(self.workflows) - triggered - children):
            result.add(
                f"workflows.{workflow_name}",
                "Workflow is internal-only and is not referenced by another workflow",
                category=DiagnosticCategory.LINT,
                severity=DiagnosticSeverity.WARNING,
            )

    def _check_composition_cycles(self, result: ValidationResult) -> None:
        """Sub-workflow composition must be acyclic (A using B using A)."""
        edges = {
            name: sorted(
                {
                    step_def.target.workflow
                    for step_def in wf.steps.values()
                    if isinstance(step_def.target, ChildWorkflowTarget)
                    and step_def.target.workflow in self.workflows
                }
            )
            for name, wf in self.workflows.items()
        }

        WHITE, GRAY, BLACK = 0, 1, 2
        color = dict.fromkeys(edges, WHITE)
        stack: list[str] = []
        cycles: list[list[str]] = []

        def visit(name: str) -> None:
            color[name] = GRAY
            stack.append(name)
            for target in edges[name]:
                if color[target] == GRAY:
                    cycles.append(stack[stack.index(target) :] + [target])
                elif color[target] == WHITE:
                    visit(target)
            stack.pop()
            color[name] = BLACK

        for name in edges:
            if color[name] == WHITE:
                visit(name)

        for cycle in cycles:
            result.add(
                "workflows",
                f"Sub-workflow composition cycle: {' -> '.join(cycle)}",
            )

    def _validate_resources(self, result: ValidationResult) -> None:
        for name, declaration in self.resources.resources.items():
            try:
                resource = self.resource_registry.resolve_resource(name, declaration)
            except ResourceRegistryError as exc:
                result.add(f"resources.{name}", str(exc), cause=exc)
                continue
            self._resolved_resources[name] = resource
        if len(self._resolved_resources) == len(self.resources.resources):
            try:
                self.resource_registry.resource_load_order(self._resolved_resources)
            except ResourceRegistryError as exc:
                result.add("resources", str(exc), cause=exc)

    def _validate_services(self, result: ValidationResult) -> None:
        for name, declaration in self.services.services.items():
            try:
                service = self.transport_registry.resolve_service(name, declaration)
            except TransportRegistryError as exc:
                result.add(f"services.{name}", str(exc))
                continue
            self._resolved_services[name] = service
            if self.check_imports:
                try:
                    startup_error = self.transport_registry.validate_startup(service)
                except TransportRegistryError as exc:
                    result.add(f"services.{name}", str(exc))
                else:
                    if startup_error is not None:
                        result.add(f"services.{name}", startup_error)
