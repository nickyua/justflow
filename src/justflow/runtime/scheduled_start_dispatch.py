"""Temporal arbitration for one-off scheduled starts."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, timedelta
from typing import Any

from pydantic import ValidationError
from temporalio import activity, workflow
from temporalio.common import Priority, RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.engine.limits import LimitExceededError, enforce_payload_bytes
from justflow.runtime.scheduled_start_service import ScheduledStartService
from justflow.runtime.scheduled_starts import (
    SCHEDULED_START_ARBITER_INPUT_ADAPTER,
    SCHEDULED_START_ARBITER_PREPARATION_ADAPTER,
    SCHEDULED_START_ARBITER_WORKFLOW_TYPE,
    SCHEDULED_START_ATTEMPT_ACTIVITY_TYPE,
    SCHEDULED_START_ATTEMPT_RESULT_ADAPTER,
    SCHEDULED_START_CLAIM_ACTIVITY_TYPE,
    SCHEDULED_START_COMMIT_ACTIVITY_TYPE,
    SCHEDULED_START_DUE_CLAIM_RESULT_ADAPTER,
    SCHEDULED_START_DUE_WORKFLOW_TYPE,
    SCHEDULED_START_PREPARE_ACTIVITY_TYPE,
    PendingScheduledStartRecord,
    ScheduledStartArbiterCommandKind,
    ScheduledStartArbiterInitialInput,
    ScheduledStartArbiterPreparedDue,
    ScheduledStartArbiterPreparedProjection,
    ScheduledStartArbiterPreparedStale,
    ScheduledStartArbiterResolvedDueInput,
    ScheduledStartArbiterStale,
    ScheduledStartArbiterTerminalInput,
    ScheduledStartAttemptAccepted,
    ScheduledStartAttemptAmbiguous,
    ScheduledStartAttemptRequest,
    ScheduledStartAttemptResult,
    ScheduledStartDueInput,
    ScheduledStartDueSkipped,
    ScheduledStartError,
    ScheduledStartFailureCode,
    complete_scheduled_start,
    describe_scheduled_start,
    fail_scheduled_start,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope

SCHEDULED_START_ACTIVITY_INVALID_INPUT = "SCHEDULED_START_ACTIVITY_INVALID_INPUT"
SCHEDULED_START_ACTIVITY_REJECTED = "SCHEDULED_START_ACTIVITY_REJECTED"
TENANT_START_ACTIVITY_ATTEMPTS = 1


@workflow.defn(name=SCHEDULED_START_DUE_WORKFLOW_TYPE)
class ScheduledStartDueWorkflow:
    @workflow.run
    async def run(self, raw_due: dict[str, Any]) -> dict[str, Any]:
        due = ScheduledStartDueInput.model_validate(raw_due)
        record = due.record
        if not isinstance(record, PendingScheduledStartRecord):
            return ScheduledStartDueSkipped(current=describe_scheduled_start(record)).model_dump(
                mode="json"
            )
        raw_result: dict[str, Any] = await workflow.execute_activity(
            SCHEDULED_START_CLAIM_ACTIVITY_TYPE,
            arg=due.model_dump(mode="json"),
            result_type=dict,
            start_to_close_timeout=timedelta(seconds=record.dispatch_timeout_seconds),
            retry_policy=RetryPolicy(
                maximum_interval=timedelta(seconds=record.dispatch_timeout_seconds)
            ),
            priority=workflow.info().priority,
        )
        result = SCHEDULED_START_DUE_CLAIM_RESULT_ADAPTER.validate_python(raw_result)
        return result.model_dump(mode="json")


@workflow.defn(name=SCHEDULED_START_ARBITER_WORKFLOW_TYPE)
class ScheduledStartArbiterWorkflow:
    @workflow.run
    async def run(self, raw_input: dict[str, Any]) -> dict[str, Any]:
        arbiter_input = SCHEDULED_START_ARBITER_INPUT_ADAPTER.validate_python(raw_input)
        if isinstance(arbiter_input, ScheduledStartArbiterInitialInput):
            return await self._prepare(arbiter_input)
        if isinstance(arbiter_input, ScheduledStartArbiterResolvedDueInput):
            return await self._dispatch(arbiter_input)
        return await self._commit(arbiter_input)

    async def _prepare(
        self,
        initial: ScheduledStartArbiterInitialInput,
    ) -> dict[str, Any]:
        raw_preparation: dict[str, Any] = await self._execute_persistent_activity(
            SCHEDULED_START_PREPARE_ACTIVITY_TYPE,
            initial.model_dump(mode="json"),
            timeout_seconds=initial.record.dispatch_timeout_seconds,
        )
        preparation = SCHEDULED_START_ARBITER_PREPARATION_ADAPTER.validate_python(raw_preparation)
        if isinstance(preparation, ScheduledStartArbiterPreparedStale):
            return ScheduledStartArbiterStale(
                consumed_version=initial.record.version,
                current=preparation.current,
            ).model_dump(mode="json")
        if isinstance(preparation, ScheduledStartArbiterPreparedProjection):
            workflow.continue_as_new(preparation.terminal.model_dump(mode="json"))
        if not isinstance(preparation, ScheduledStartArbiterPreparedDue):
            raise TypeError("Scheduled-start preparation returned an unknown state")
        workflow.continue_as_new(
            ScheduledStartArbiterResolvedDueInput(
                record=preparation.record,
                target=preparation.target,
            ).model_dump(mode="json")
        )

    async def _dispatch(
        self,
        progress: ScheduledStartArbiterResolvedDueInput,
    ) -> dict[str, Any]:
        saw_ambiguous = progress.saw_ambiguous
        authoritative_rejections = progress.authoritative_rejections
        timeout = timedelta(seconds=progress.record.dispatch_timeout_seconds)
        request = ScheduledStartAttemptRequest(
            record=progress.record,
            target=progress.target,
        )
        for _ in range(progress.record.dispatch_attempts):
            attempt: ScheduledStartAttemptResult
            try:
                raw_attempt: dict[str, Any] = await workflow.execute_activity(
                    SCHEDULED_START_ATTEMPT_ACTIVITY_TYPE,
                    arg=request.model_dump(mode="json"),
                    result_type=dict,
                    start_to_close_timeout=timeout,
                    retry_policy=RetryPolicy(maximum_attempts=TENANT_START_ACTIVITY_ATTEMPTS),
                    priority=workflow.info().priority,
                )
            except ActivityError:
                attempt = ScheduledStartAttemptAmbiguous()
            else:
                attempt = SCHEDULED_START_ATTEMPT_RESULT_ADAPTER.validate_python(raw_attempt)
            if isinstance(attempt, ScheduledStartAttemptAccepted):
                started_projection = complete_scheduled_start(
                    progress.record,
                    attempt.run,
                    completed_at=workflow.now().astimezone(UTC).replace(microsecond=0),
                )
                workflow.continue_as_new(
                    ScheduledStartArbiterTerminalInput(
                        projection=started_projection,
                        consumed_version=progress.record.claimed_version,
                        winner=ScheduledStartArbiterCommandKind.DUE,
                    ).model_dump(mode="json")
                )
            if isinstance(attempt, ScheduledStartAttemptAmbiguous):
                saw_ambiguous = True
            else:
                authoritative_rejections += 1
                if (
                    not saw_ambiguous
                    and authoritative_rejections >= progress.record.dispatch_attempts
                ):
                    failed_projection = fail_scheduled_start(
                        progress.record,
                        ScheduledStartFailureCode.DISPATCH_EXHAUSTED,
                        completed_at=(workflow.now().astimezone(UTC).replace(microsecond=0)),
                    )
                    workflow.continue_as_new(
                        ScheduledStartArbiterTerminalInput(
                            projection=failed_projection,
                            consumed_version=progress.record.claimed_version,
                            winner=ScheduledStartArbiterCommandKind.DUE,
                        ).model_dump(mode="json")
                    )
        workflow.continue_as_new(
            ScheduledStartArbiterResolvedDueInput(
                record=progress.record,
                target=progress.target,
                saw_ambiguous=saw_ambiguous,
                authoritative_rejections=authoritative_rejections,
            ).model_dump(mode="json")
        )

    async def _commit(
        self,
        terminal: ScheduledStartArbiterTerminalInput,
    ) -> dict[str, Any]:
        raw_result: dict[str, Any] = await self._execute_persistent_activity(
            SCHEDULED_START_COMMIT_ACTIVITY_TYPE,
            terminal.model_dump(mode="json"),
            timeout_seconds=terminal.projection.dispatch_timeout_seconds,
        )
        return raw_result

    @staticmethod
    async def _execute_persistent_activity(
        activity_type: str,
        argument: dict[str, Any],
        *,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        try:
            result: dict[str, Any] = await workflow.execute_activity(
                activity_type,
                arg=argument,
                result_type=dict,
                start_to_close_timeout=timedelta(seconds=timeout_seconds),
                retry_policy=RetryPolicy(maximum_interval=timedelta(seconds=timeout_seconds)),
                priority=workflow.info().priority,
            )
        except ActivityError as exc:
            raise RuntimeError("Scheduled-start arbiter activity failed permanently") from exc
        return result


class ScheduledStartDispatchActivities:
    def __init__(
        self,
        service: ScheduledStartService,
        *,
        scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
        limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
        priority_provider: Callable[[], Priority] | None = None,
    ) -> None:
        self._service = service
        self._scope = scope
        self._limits = limits
        self._priority_provider = priority_provider or (lambda: activity.info().priority)

    @activity.defn(name=SCHEDULED_START_CLAIM_ACTIVITY_TYPE)
    async def claim(self, raw_due: dict[str, Any]) -> dict[str, Any]:
        try:
            self._enforce_input(raw_due, "scheduled_start.claim_activity.input")
            due = ScheduledStartDueInput.model_validate(raw_due)
            if isinstance(due.record, PendingScheduledStartRecord):
                result = await self._service.claim_due(
                    due.record,
                    scope=self._scope,
                    priority=self._priority_provider(),
                )
            else:
                result = ScheduledStartDueSkipped(current=describe_scheduled_start(due.record))
        except (LimitExceededError, ValidationError, TypeError, ValueError) as exc:
            raise _invalid_activity_input("Scheduled-start claim input is invalid") from exc
        except ScheduledStartError as exc:
            raise _activity_error("Scheduled-start claim failed", exc) from exc
        return result.model_dump(mode="json")

    @activity.defn(name=SCHEDULED_START_PREPARE_ACTIVITY_TYPE)
    async def prepare(self, raw_initial: dict[str, Any]) -> dict[str, Any]:
        try:
            self._enforce_input(raw_initial, "scheduled_start.prepare_activity.input")
            initial = ScheduledStartArbiterInitialInput.model_validate(raw_initial)
            result = await self._service.prepare_arbiter(initial, scope=self._scope)
        except (LimitExceededError, ValidationError, TypeError, ValueError) as exc:
            raise _invalid_activity_input("Scheduled-start preparation input is invalid") from exc
        except ScheduledStartError as exc:
            raise _activity_error("Scheduled-start preparation failed", exc) from exc
        return result.model_dump(mode="json")

    @activity.defn(name=SCHEDULED_START_ATTEMPT_ACTIVITY_TYPE)
    async def attempt(self, raw_attempt: dict[str, Any]) -> dict[str, Any]:
        try:
            self._enforce_input(raw_attempt, "scheduled_start.attempt_activity.input")
            attempt = ScheduledStartAttemptRequest.model_validate(raw_attempt)
            result = await self._service.attempt_dispatch(
                attempt,
                scope=self._scope,
                priority=self._priority_provider(),
            )
        except (LimitExceededError, ValidationError, TypeError, ValueError) as exc:
            raise _invalid_activity_input("Scheduled-start attempt input is invalid") from exc
        except ScheduledStartError as exc:
            raise _activity_error("Scheduled-start attempt failed", exc) from exc
        return result.model_dump(mode="json")

    @activity.defn(name=SCHEDULED_START_COMMIT_ACTIVITY_TYPE)
    async def commit(self, raw_terminal: dict[str, Any]) -> dict[str, Any]:
        try:
            self._enforce_input(raw_terminal, "scheduled_start.commit_activity.input")
            terminal = ScheduledStartArbiterTerminalInput.model_validate(raw_terminal)
            result = await self._service.commit_arbiter(terminal, scope=self._scope)
        except (LimitExceededError, ValidationError, TypeError, ValueError) as exc:
            raise _invalid_activity_input("Scheduled-start commit input is invalid") from exc
        except ScheduledStartError as exc:
            raise _activity_error("Scheduled-start commit failed", exc) from exc
        return result.model_dump(mode="json")

    def _enforce_input(self, value: object, boundary: str) -> None:
        enforce_payload_bytes(
            value,
            boundary=boundary,
            limit=self._limits.activity_input_bytes,
        )


def _invalid_activity_input(message: str) -> ApplicationError:
    return ApplicationError(
        message,
        type=SCHEDULED_START_ACTIVITY_INVALID_INPUT,
        non_retryable=True,
    )


def _activity_error(message: str, error: ScheduledStartError) -> ApplicationError:
    return ApplicationError(
        message,
        type=SCHEDULED_START_ACTIVITY_REJECTED,
        non_retryable=not error.retryable,
    )
