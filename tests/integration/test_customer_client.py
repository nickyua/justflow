"""The example HTTP client drives real scheduled-start arbitration through the ASGI API."""

from __future__ import annotations

from datetime import timedelta

import httpx
import pytest
from scheduled_reporting.customer_client import CustomerClientError, CustomerWorkflowClient
from temporalio.worker import Worker

from justflow.config.settings import ControlSettings, ScheduledStartSettings
from justflow.definitions.routing import wait_for_worker_deployment, worker_deployment_config
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.provenance import RuntimeProfile
from justflow.runtime import (
    ScheduledStartCancelRequest,
    ScheduledStartCreateRequest,
    ScheduledStartRescheduleRequest,
)
from justflow.runtime.control_api import ControlApi
from justflow.runtime.health import HealthRegistry
from justflow.runtime.metrics import MetricsRegistry
from justflow.runtime.operations import WorkflowControlService
from justflow.runtime.scheduled_start_dispatch import (
    ScheduledStartArbiterWorkflow,
    ScheduledStartDispatchActivities,
    ScheduledStartDueWorkflow,
)
from justflow.runtime.scheduled_start_service import (
    BestEffortLocalScheduledStartQuotaController,
    ScheduledStartService,
)
from justflow.runtime.scheduled_starts import ScheduledStartMutationStatus, ScheduledStartState
from justflow.runtime.starter import WorkflowStarter
from tests.integration.test_scheduled_starts import (
    BUSINESS_TASK_QUEUE,
    DEPLOYMENT,
    DISPATCH_TASK_QUEUE,
    FIRST_DUE_TIME,
    FIRST_TARGET,
    LONG_TEST_HORIZON_SECONDS,
    NOW,
    RESCHEDULED_DUE_TIME,
    WORKFLOW_NAME,
    FirstScheduledEntityWorkflow,
    MutableTargetResolver,
)

CREATE_KEY = "customer-create"
RESCHEDULE_KEY = "customer-reschedule"
CANCEL_KEY = "customer-cancel"


async def test_customer_client_appointment_lifecycle_and_accepted_retry() -> None:
    current_time = NOW
    async with await start_local_environment() as environment:
        starter = WorkflowStarter(
            environment.client,
            BUSINESS_TASK_QUEUE,
            target_resolver=MutableTargetResolver(FIRST_TARGET),
        )
        service = ScheduledStartService(
            environment.client,
            starter,
            ScheduledStartSettings(max_horizon_seconds=LONG_TEST_HORIZON_SECONDS),
            task_queue=DISPATCH_TASK_QUEUE,
            quota_controller=BestEffortLocalScheduledStartQuotaController(),
            clock=lambda: current_time,
        )
        activities = ScheduledStartDispatchActivities(service)
        api = ControlApi(
            settings=ControlSettings(),
            runtime_profile=RuntimeProfile.LOCAL,
            starter=starter,
            controls=WorkflowControlService(
                environment.client, max_payload_bytes=ControlSettings().max_request_body_bytes
            ),
            health=HealthRegistry(frozenset()),
            metrics=MetricsRegistry(),
            scheduled_starts=service,
        )
        async with (
            Worker(
                environment.client,
                task_queue=BUSINESS_TASK_QUEUE,
                workflows=[FirstScheduledEntityWorkflow],
                deployment_config=worker_deployment_config(DEPLOYMENT),
            ),
            Worker(
                environment.client,
                task_queue=DISPATCH_TASK_QUEUE,
                workflows=[ScheduledStartDueWorkflow, ScheduledStartArbiterWorkflow],
                activities=[
                    activities.claim,
                    activities.prepare,
                    activities.attempt,
                    activities.commit,
                ],
                workflow_runner=workflow_sandbox_runner(),
            ),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api), base_url="http://127.0.0.1"
            ) as http,
        ):
            await wait_for_worker_deployment(environment.client, DEPLOYMENT, BUSINESS_TASK_QUEUE)
            client = CustomerWorkflowClient(http)
            capabilities = await client.capabilities()
            assert capabilities.scheduled_start_create
            request = ScheduledStartCreateRequest(
                workflow_name=WORKFLOW_NAME,
                business_request_id="appointment-reference",
                input={"reference": "customer-reference"},
                start_at=FIRST_DUE_TIME,
                workload_class="standard",
            )
            accepted = await client.schedule(request, key=CREATE_KEY)
            assert accepted.status is ScheduledStartMutationStatus.ACCEPTED
            identity = accepted.scheduled_start.scheduled_start_id
            current_time = FIRST_DUE_TIME + timedelta(seconds=1)
            duplicate = await client.schedule(request, key=CREATE_KEY)
            assert duplicate.status is ScheduledStartMutationStatus.DUPLICATE
            assert duplicate.scheduled_start.scheduled_start_id == identity
            reschedule = ScheduledStartRescheduleRequest(
                expected_version=accepted.scheduled_start.version, start_at=RESCHEDULED_DUE_TIME
            )
            changed = await client.reschedule(identity, reschedule, key=RESCHEDULE_KEY)
            assert changed.status is ScheduledStartMutationStatus.RESCHEDULED
            with pytest.raises(CustomerClientError, match="conflict"):
                await client.reschedule(identity, reschedule, key="different-reschedule")
            cancel = ScheduledStartCancelRequest(expected_version=changed.scheduled_start.version)
            canceled = await client.cancel(identity, cancel, key=CANCEL_KEY)
            assert canceled.scheduled_start.state is ScheduledStartState.CANCELED
            recovered = await client.reschedule(identity, reschedule, key=RESCHEDULE_KEY)
            assert recovered.status is ScheduledStartMutationStatus.DUPLICATE
            assert (await client.cancel(identity, cancel, key=CANCEL_KEY)).status is (
                ScheduledStartMutationStatus.DUPLICATE
            )
