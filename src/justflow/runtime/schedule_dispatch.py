"""Temporal workflow and activity for idempotent scheduled starts."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from pydantic import ValidationError
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.definitions.catalog import CatalogError, DefinitionCatalog, DefinitionCatalogStore
from justflow.definitions.routing import WorkerDeployment, WorkflowStartTarget
from justflow.engine.limits import LimitExceededError, enforce_payload_bytes
from justflow.runtime.schedules import (
    SCHEDULE_DISPATCH_WORKFLOW_TYPE,
    ScheduleDispatchPlan,
    make_schedule_occurrence_id,
)
from justflow.runtime.starter import (
    ScheduleSourceIdentity,
    StartWorkflowRequest,
    WorkflowStarter,
    WorkflowStartError,
)
from justflow.scope import (
    LEGACY_LOCAL_UNSCOPED_POLICY,
    LOCAL_RUNTIME_SCOPE,
    RuntimeScope,
    ScopeBindingKind,
    TrustedScopeBinding,
)

SCHEDULE_DISPATCH_ACTIVITY_TYPE = "justflow.start-scheduled-workflow.v1"
SCHEDULE_DISPATCH_INVALID_INPUT = "SCHEDULE_DISPATCH_INVALID_INPUT"
SCHEDULE_TARGET_UNAVAILABLE = "SCHEDULE_TARGET_UNAVAILABLE"
SCHEDULE_START_REJECTED = "SCHEDULE_START_REJECTED"


@workflow.defn(name=SCHEDULE_DISPATCH_WORKFLOW_TYPE)
class ScheduleDispatchWorkflow:
    @workflow.run
    async def run(self, raw_plan: dict[str, Any]) -> dict[str, Any]:
        try:
            enforce_payload_bytes(
                raw_plan,
                boundary="schedule.dispatch.input",
                limit=DEFAULT_RUNTIME_LIMITS.activity_input_bytes,
            )
            plan = ScheduleDispatchPlan.model_validate(raw_plan)
        except (LimitExceededError, ValidationError, TypeError, ValueError) as exc:
            raise ApplicationError(
                "Scheduled dispatch input is invalid",
                type=SCHEDULE_DISPATCH_INVALID_INPUT,
                non_retryable=True,
            ) from exc
        occurrence_id = make_schedule_occurrence_id(
            plan.schedule_name,
            workflow.info().workflow_id,
            scope_digest=plan.target.scope_digest,
        )
        result: dict[str, Any] = await workflow.execute_activity(
            SCHEDULE_DISPATCH_ACTIVITY_TYPE,
            arg={
                "plan": plan.model_dump(mode="json"),
                "occurrence_id": occurrence_id,
            },
            result_type=dict,
            start_to_close_timeout=timedelta(seconds=plan.dispatch_timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=plan.dispatch_attempts),
        )
        return result


class ScheduleDispatchActivity:
    def __init__(
        self,
        *,
        catalog: DefinitionCatalog,
        catalog_store: DefinitionCatalogStore,
        starter: WorkflowStarter,
        scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
        limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
    ) -> None:
        self._catalog = catalog
        self._catalog_store = catalog_store
        self._starter = starter
        self._scope = scope
        self._limits = limits

    @activity.defn(name=SCHEDULE_DISPATCH_ACTIVITY_TYPE)
    async def start_scheduled_workflow(self, raw_request: dict[str, Any]) -> dict[str, Any]:
        try:
            enforce_payload_bytes(
                raw_request,
                boundary="schedule.dispatch.activity_input",
                limit=self._limits.activity_input_bytes,
            )
            plan = ScheduleDispatchPlan.model_validate(raw_request.get("plan"))
            occurrence_id = _required_occurrence_id(raw_request.get("occurrence_id"))
            target = self._resolve_target(plan)
        except (CatalogError, LimitExceededError, ValidationError, TypeError, ValueError) as exc:
            raise ApplicationError(
                "Scheduled workflow target is unavailable",
                type=SCHEDULE_TARGET_UNAVAILABLE,
                non_retryable=True,
            ) from exc

        try:
            result = await self._starter.start_resolved(
                StartWorkflowRequest(
                    workflow_name=plan.target.workflow_name,
                    definition_digest=plan.target.definition_digest,
                    business_request_id=occurrence_id,
                    input=dict(plan.input),
                    source=ScheduleSourceIdentity(
                        schedule=plan.schedule_name,
                        occurrence_id=occurrence_id,
                    ),
                ),
                target,
                trigger_name=plan.schedule_name,
                scope_binding=TrustedScopeBinding.create(
                    kind=ScopeBindingKind.SCHEDULE,
                    scope=self._scope,
                    binding_id=plan.schedule_name,
                ),
            )
        except WorkflowStartError as exc:
            raise ApplicationError(
                "Scheduled workflow start was rejected",
                type=SCHEDULE_START_REJECTED,
                non_retryable=not exc.retryable,
            ) from exc
        return result.model_dump(mode="json")

    def _resolve_target(self, plan: ScheduleDispatchPlan) -> WorkflowStartTarget:
        identity = plan.target
        if not LEGACY_LOCAL_UNSCOPED_POLICY.owns(identity.scope_digest, self._scope):
            raise ValueError("Schedule target scope does not match the trusted worker scope")
        manifest = self._catalog.get(identity.workflow_name, identity.definition_digest)
        snapshot = self._catalog_store.load_environment_snapshot(
            identity.environment_snapshot_digest
        )
        if snapshot.definition_digest != manifest.definition_digest:
            raise ValueError("Schedule environment snapshot definition does not match its target")
        if snapshot.worker_artifact != identity.artifact_identity:
            raise ValueError("Schedule environment snapshot artifact does not match its target")
        if snapshot.execution_configuration != identity.execution_configuration:
            raise ValueError(
                "Schedule environment snapshot configuration does not match its target"
            )
        deployment = WorkerDeployment(
            artifact_identity=identity.artifact_identity,
            compatible_engine_workflow_abis=frozenset({manifest.required_engine_workflow_abi}),
        )
        return WorkflowStartTarget(
            manifest=manifest,
            deployment=deployment,
            environment_snapshot_digest=snapshot.snapshot_digest,
            scope_digest=identity.scope_digest,
            execution_configuration=identity.execution_configuration,
        )


def _required_occurrence_id(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("Schedule occurrence identity is missing")
    return value
