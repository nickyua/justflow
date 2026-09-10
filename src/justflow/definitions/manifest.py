"""Canonical, content-addressed workflow definition manifests."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import asdict
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from justflow.config.models import (
    ChildWorkflowTarget,
    EvaluatorCondition,
    ServiceOperationTarget,
    WorkflowConfig,
)
from justflow.config.runtime_limits import RuntimeLimits
from justflow.engine.contracts import (
    SchemaDeclarationError,
    SchemaLoadError,
    SchemaSpec,
    validate_contract_declaration,
)
from justflow.engine.limits import PayloadSerializationError, strict_json_bytes
from justflow.resources.registry import ResolvedResource
from justflow.transports.registry import ResolvedService

MANIFEST_FORMAT_VERSION = 2
ENGINE_WORKFLOW_ABI = "justflow.workflow.v1"
SHA256_HEX_LENGTH = 64
SHA256_HEX_PATTERN = re.compile(rf"^[0-9a-f]{{{SHA256_HEX_LENGTH}}}$")
WORKFLOW_TYPE_SEPARATOR = "__"


class DefinitionManifestError(Exception):
    """A workflow cannot be represented by a stable definition identity."""


class FrozenManifestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ProviderContractIdentity(FrozenManifestModel):
    name: str
    version: str


class ContractReference(FrozenManifestModel):
    location: str
    identity: str


class EvaluatorIdentity(FrozenManifestModel):
    path: str


class ChildDefinitionIdentity(FrozenManifestModel):
    workflow: str
    definition_digest: str = Field(
        min_length=SHA256_HEX_LENGTH,
        max_length=SHA256_HEX_LENGTH,
        pattern=SHA256_HEX_PATTERN,
    )


class ServiceSchedulingPolicy(FrozenManifestModel):
    service: str
    provider: str
    provider_contract_version: str
    connect_timeout_sec: int | None
    dispatch_timeout_sec: int
    response_timeout_sec: int | None
    retries: int
    params: dict[str, Any]


class DeterministicPolicy(FrozenManifestModel):
    services: tuple[ServiceSchedulingPolicy, ...]
    runtime_limits: dict[str, int]


class DefinitionManifestContent(FrozenManifestModel):
    format_version: int = MANIFEST_FORMAT_VERSION
    logical_name: str
    required_engine_workflow_abi: str
    workflow: dict[str, Any]
    deterministic_policy: DeterministicPolicy
    provider_contracts: tuple[ProviderContractIdentity, ...]
    contracts: tuple[ContractReference, ...]
    evaluators: tuple[EvaluatorIdentity, ...]
    children: tuple[ChildDefinitionIdentity, ...]


class DefinitionManifest(DefinitionManifestContent):
    definition_digest: str = Field(
        min_length=SHA256_HEX_LENGTH,
        max_length=SHA256_HEX_LENGTH,
        pattern=SHA256_HEX_PATTERN,
    )

    @model_validator(mode="after")
    def verify_identity(self) -> Self:
        if self.format_version != MANIFEST_FORMAT_VERSION:
            raise ValueError(
                f"Unsupported definition manifest format version {self.format_version}"
            )
        if SHA256_HEX_PATTERN.fullmatch(self.definition_digest) is None:
            raise ValueError("Definition digest must be a lowercase full SHA-256 digest")
        expected = definition_digest(self.model_dump(mode="json", exclude={"definition_digest"}))
        if self.definition_digest != expected:
            raise ValueError(
                f"Definition digest mismatch for workflow '{self.logical_name}': "
                f"expected {expected}, found {self.definition_digest}"
            )
        return self

    @classmethod
    def create(cls, **content: Any) -> DefinitionManifest:
        try:
            normalized = DefinitionManifestContent.model_validate(content).model_dump(mode="json")
            digest = definition_digest(normalized)
            return cls(**normalized, definition_digest=digest)
        except ValidationError as exc:
            raise DefinitionManifestError("Definition manifest content is invalid") from exc

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))

    def matches_workflow(self, workflow: WorkflowConfig) -> bool:
        return self.workflow == _manifest_workflow_content(workflow)


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return strict_json_bytes(value)
    except (PayloadSerializationError, TypeError, ValueError, UnicodeError) as exc:
        raise DefinitionManifestError(f"Definition manifest is not canonical JSON: {exc}") from exc


def definition_digest(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def workflow_type_name(logical_name: str, digest: str) -> str:
    if SHA256_HEX_PATTERN.fullmatch(digest) is None:
        raise DefinitionManifestError("Workflow type requires a full SHA-256 definition digest")
    return f"{logical_name}{WORKFLOW_TYPE_SEPARATOR}{digest}"


def build_definition_manifests(
    workflows: Mapping[str, WorkflowConfig],
    services: Mapping[str, ResolvedService],
    limits: RuntimeLimits,
    *,
    resources: Mapping[str, ResolvedResource] | None = None,
    required_engine_workflow_abi: str = ENGINE_WORKFLOW_ABI,
) -> dict[str, DefinitionManifest]:
    resolved_resources = resources or {}
    built: dict[str, DefinitionManifest] = {}
    visiting: list[str] = []

    def build(logical_name: str) -> DefinitionManifest:
        if logical_name in built:
            return built[logical_name]
        if logical_name in visiting:
            cycle = " -> ".join([*visiting, logical_name])
            raise DefinitionManifestError(
                f"Workflow dependency cycle cannot be manifested: {cycle}"
            )
        try:
            workflow = workflows[logical_name]
        except KeyError as exc:
            raise DefinitionManifestError(
                f"Workflow definition '{logical_name}' is not available"
            ) from exc

        visiting.append(logical_name)
        child_names = sorted(
            {
                step.target.workflow
                for step in workflow.steps.values()
                if isinstance(step.target, ChildWorkflowTarget)
            }
        )
        children = tuple(
            ChildDefinitionIdentity(
                workflow=child_name,
                definition_digest=build(child_name).definition_digest,
            )
            for child_name in child_names
        )
        visiting.pop()

        used_service_names = sorted(
            {
                step.target.service
                for step in workflow.steps.values()
                if isinstance(step.target, ServiceOperationTarget)
            }
        )
        scheduling: list[ServiceSchedulingPolicy] = []
        provider_contracts: set[tuple[str, str]] = set()
        for service_name in used_service_names:
            try:
                service = services[service_name]
            except KeyError as exc:
                raise DefinitionManifestError(
                    f"Workflow '{logical_name}' uses unresolved service '{service_name}'"
                ) from exc
            scheduling.append(
                ServiceSchedulingPolicy(
                    service=service.name,
                    provider=service.provider_name,
                    provider_contract_version=service.provider_contract_version,
                    connect_timeout_sec=service.connect_timeout_sec,
                    dispatch_timeout_sec=service.dispatch_timeout_sec,
                    response_timeout_sec=service.response_timeout_sec,
                    retries=service.retries,
                    params=dict(service.params),
                )
            )
            provider_contracts.add((service.provider_name, service.provider_contract_version))

        pending_resource_names = list(_resource_names(workflow))
        included_resource_names: set[str] = set()
        while pending_resource_names:
            resource_name = pending_resource_names.pop()
            if resource_name in included_resource_names:
                continue
            try:
                resource = resolved_resources[resource_name]
            except KeyError as exc:
                raise DefinitionManifestError(
                    f"Workflow '{logical_name}' uses unresolved resource '{resource_name}'"
                ) from exc
            included_resource_names.add(resource_name)
            provider_contracts.add((resource.provider_name, resource.provider_contract_version))
            pending_resource_names.extend(
                dependency.resource_name for dependency in resource.dependencies
            )

        try:
            manifest = DefinitionManifest.create(
                format_version=MANIFEST_FORMAT_VERSION,
                logical_name=logical_name,
                required_engine_workflow_abi=required_engine_workflow_abi,
                workflow=_manifest_workflow_content(workflow),
                deterministic_policy=DeterministicPolicy(
                    services=tuple(scheduling),
                    runtime_limits=asdict(limits),
                ),
                provider_contracts=tuple(
                    ProviderContractIdentity(name=name, version=version)
                    for name, version in sorted(provider_contracts)
                ),
                contracts=_contract_references(workflow),
                evaluators=_evaluator_identities(workflow),
                children=children,
            )
        except (SchemaDeclarationError, SchemaLoadError, ValidationError) as exc:
            raise DefinitionManifestError(
                f"Workflow '{logical_name}' cannot be represented by a definition manifest"
            ) from exc
        built[logical_name] = manifest
        return manifest

    for name in sorted(workflows):
        build(name)
    return built


def _resource_names(workflow: WorkflowConfig) -> tuple[str, ...]:
    names: set[str] = set()
    if workflow.on_complete is not None:
        names.add(workflow.on_complete.resource)
    for step in workflow.steps.values():
        names.update(step.required_resources)
        if step.cache is not None:
            names.add(step.cache.resource)
    for flow_step in workflow.flow:
        for branch in flow_step.on_result or ():
            if isinstance(branch.when, EvaluatorCondition):
                names.update(branch.when.resources)
    return tuple(sorted(names))


def _manifest_workflow_content(workflow: WorkflowConfig) -> dict[str, Any]:
    """Serialize the typed authoring model into the stable manifest-v2 workflow shape."""
    content = workflow.model_dump(mode="json", by_alias=True)
    steps = content["steps"]
    if not isinstance(steps, dict):
        raise DefinitionManifestError("Definition manifest workflow steps are invalid")
    for step_name, definition in workflow.steps.items():
        step = steps.get(step_name)
        if not isinstance(step, dict):
            raise DefinitionManifestError("Definition manifest workflow step is invalid")
        if step.pop("target", None) is None:
            raise DefinitionManifestError("Definition manifest workflow target is invalid")
        if isinstance(definition.target, ServiceOperationTarget):
            step["service"] = definition.target.service
            step["action"] = definition.target.action
            step["workflow"] = None
        elif isinstance(definition.target, ChildWorkflowTarget):
            step["service"] = None
            step["action"] = None
            step["workflow"] = definition.target.workflow
        else:
            raise DefinitionManifestError("Definition manifest workflow target kind is invalid")
    return content


def _contract_references(workflow: WorkflowConfig) -> tuple[ContractReference, ...]:
    declarations: list[tuple[str, SchemaSpec]] = []
    if workflow.input_schema is not None:
        declarations.append(("workflow.input", workflow.input_schema))
    if workflow.output_schema is not None:
        declarations.append(("workflow.output", workflow.output_schema))
    for step_name, step in sorted(workflow.steps.items()):
        if step.input_schema is not None:
            declarations.append((f"steps.{step_name}.input", step.input_schema))
        if step.output_schema is not None:
            declarations.append((f"steps.{step_name}.output", step.output_schema))
    return tuple(
        ContractReference(location=location, identity=str(validate_contract_declaration(schema)))
        for location, schema in declarations
    )


def _evaluator_identities(workflow: WorkflowConfig) -> tuple[EvaluatorIdentity, ...]:
    paths = {
        branch.when.evaluator
        for step in workflow.flow
        for branch in (step.on_result or ())
        if isinstance(branch.when, EvaluatorCondition)
    }
    return tuple(EvaluatorIdentity(path=path) for path in sorted(paths))
