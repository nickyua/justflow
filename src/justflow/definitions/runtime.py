"""Validated assembly of catalog definitions into Temporal workflow classes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from justflow.config.models import WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.definitions.catalog import DefinitionCatalog
from justflow.definitions.manifest import DefinitionManifest, build_definition_manifests
from justflow.definitions.routing import (
    DefinitionStartTarget,
    WorkerDeploymentRouter,
)
from justflow.engine.compiler import compile_workflow
from justflow.provenance import ExecutionConfigurationIdentity
from justflow.resources.registry import ResolvedResource
from justflow.scope import RuntimeScope
from justflow.transports.registry import ResolvedService


@dataclass(frozen=True, kw_only=True)
class PreparedDefinitions:
    manifests: Mapping[str, DefinitionManifest]
    workflow_classes: Mapping[str, type]
    start_targets: Mapping[str, DefinitionStartTarget]


def prepare_definitions(
    workflows: Mapping[str, WorkflowConfig],
    services: Mapping[str, ResolvedService],
    limits: RuntimeLimits,
    catalog: DefinitionCatalog,
    router: WorkerDeploymentRouter,
    environment_snapshot_digests: Mapping[str, str],
    *,
    resources: Mapping[str, ResolvedResource] | None = None,
    runtime_scope: RuntimeScope | None = None,
    execution_configuration: ExecutionConfigurationIdentity | None = None,
) -> PreparedDefinitions:
    authored = build_definition_manifests(
        workflows,
        services,
        limits,
        resources=resources,
    )
    catalog.verify_authored(authored)
    if set(environment_snapshot_digests) != set(workflows):
        raise ValueError(
            "Execution environment snapshots do not match workflow definitions: "
            f"snapshots={sorted(environment_snapshot_digests)}, workflows={sorted(workflows)}"
        )
    workflow_classes: dict[str, type] = {}
    start_targets: dict[str, DefinitionStartTarget] = {}
    for logical_name in sorted(workflows):
        manifest = catalog.resolve(logical_name)
        deployment = router.select(manifest)
        children = {
            child.workflow: catalog.get(child.workflow, child.definition_digest)
            for child in manifest.children
        }
        workflow_class = compile_workflow(
            workflows[logical_name],
            services,
            limits=limits,
            manifest=manifest,
            deployment=deployment,
            child_manifests=children,
            environment_snapshot_digest=environment_snapshot_digests[logical_name],
            child_environment_snapshot_digests={
                child_name: environment_snapshot_digests[child_name] for child_name in children
            },
            runtime_scope_digest=runtime_scope.digest if runtime_scope is not None else None,
            execution_configuration=execution_configuration,
        )
        target = DefinitionStartTarget(
            manifest=manifest,
            workflow_class=workflow_class,
            deployment=deployment,
            environment_snapshot_digest=environment_snapshot_digests[logical_name],
            scope_digest=runtime_scope.digest if runtime_scope is not None else None,
            execution_configuration=execution_configuration,
        )
        workflow_classes[target.workflow_type] = workflow_class
        start_targets[logical_name] = target
    return PreparedDefinitions(
        manifests=MappingProxyType(authored),
        workflow_classes=MappingProxyType(workflow_classes),
        start_targets=MappingProxyType(start_targets),
    )
