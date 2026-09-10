"""Bounded cleanup for terminal one-off scheduled starts."""

from __future__ import annotations

import base64
import binascii
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import Field, ValidationError
from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleDescription,
    ScheduleHandle,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode

from justflow.config.settings import ScheduledStartSettings
from justflow.definitions.routing import MEMO_SCOPE_DIGEST
from justflow.engine.limits import LimitExceededError, enforce_payload_bytes
from justflow.runtime.scheduled_starts import (
    MAX_SCHEDULED_START_CURSOR_LENGTH,
    MEMO_SCHEDULED_START_ID,
    MEMO_SCHEDULED_START_OWNER,
    SCHEDULED_START_CLEANUP_ACTIVITY_TYPE,
    SCHEDULED_START_CLEANUP_WORKFLOW_TYPE,
    SCHEDULED_START_DUE_WORKFLOW_TYPE,
    SCHEDULED_START_MAINTENANCE_ID_PREFIX,
    SCHEDULED_START_MAINTENANCE_OWNER,
    SCHEDULED_START_OWNER,
    SCHEDULED_START_SCHEDULE_ID_PREFIX,
    CanceledScheduledStartRecord,
    FailedScheduledStartRecord,
    ScheduledStartDueInput,
    StartedScheduledStartRecord,
    StrictScheduledStartModel,
)
from justflow.scope import safe_identity_digest, scoped_identity_from_digest

SCHEDULED_START_CLEANUP_INVALID_INPUT = "SCHEDULED_START_CLEANUP_INVALID_INPUT"
SCHEDULED_START_CLEANUP_FAILED = "SCHEDULED_START_CLEANUP_FAILED"
SCHEDULED_START_CLEANUP_ACTIVITY_TIMEOUT_SECONDS = 30
SCHEDULED_START_CLEANUP_ACTIVITY_ATTEMPTS = 3


class ScheduledStartCleanupPage(StrictScheduledStartModel):
    cursor: str | None = Field(
        default=None,
        max_length=MAX_SCHEDULED_START_CURSOR_LENGTH,
        repr=False,
    )


class ScheduledStartCleanupResult(StrictScheduledStartModel):
    cursor: str | None = Field(
        default=None,
        max_length=MAX_SCHEDULED_START_CURSOR_LENGTH,
        repr=False,
    )
    scanned: int = Field(ge=0)
    deleted: int = Field(ge=0)


@workflow.defn(name=SCHEDULED_START_CLEANUP_WORKFLOW_TYPE)
class ScheduledStartCleanupWorkflow:
    @workflow.run
    async def run(self, raw_page: dict[str, Any]) -> dict[str, Any]:
        try:
            page = ScheduledStartCleanupPage.model_validate(raw_page)
        except (ValidationError, TypeError, ValueError) as exc:
            raise ApplicationError(
                "Scheduled-start cleanup input is invalid",
                type=SCHEDULED_START_CLEANUP_INVALID_INPUT,
                non_retryable=True,
            ) from exc
        raw_result: dict[str, Any] = await workflow.execute_activity(
            SCHEDULED_START_CLEANUP_ACTIVITY_TYPE,
            arg=page.model_dump(mode="json"),
            result_type=dict,
            start_to_close_timeout=timedelta(
                seconds=SCHEDULED_START_CLEANUP_ACTIVITY_TIMEOUT_SECONDS
            ),
            retry_policy=RetryPolicy(maximum_attempts=SCHEDULED_START_CLEANUP_ACTIVITY_ATTEMPTS),
        )
        result = ScheduledStartCleanupResult.model_validate(raw_result)
        if result.cursor is not None:
            workflow.continue_as_new(
                ScheduledStartCleanupPage(cursor=result.cursor).model_dump(mode="json")
            )
        return result.model_dump(mode="json")


class ScheduledStartCleanupActivity:
    def __init__(
        self,
        client: Client,
        settings: ScheduledStartSettings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._client = client
        self._settings = settings
        self._clock = clock or (lambda: datetime.now(UTC))
        self._rpc_timeout = timedelta(seconds=settings.dispatch_timeout_seconds)

    @activity.defn(name=SCHEDULED_START_CLEANUP_ACTIVITY_TYPE)
    async def cleanup(self, raw_page: dict[str, Any]) -> dict[str, Any]:
        try:
            enforce_payload_bytes(
                raw_page,
                boundary="scheduled_start.cleanup_activity.input",
                limit=MAX_SCHEDULED_START_CURSOR_LENGTH,
            )
            page = ScheduledStartCleanupPage.model_validate(raw_page)
            iterator = await self._client.list_schedules(
                query=f'ScheduleId STARTS_WITH "{SCHEDULED_START_SCHEDULE_ID_PREFIX}"',
                page_size=self._settings.cleanup_page_size,
                next_page_token=_decode_cursor(page.cursor),
                rpc_timeout=self._rpc_timeout,
            )
            await iterator.fetch_next_page()
            entries = iterator.current_page
            if entries is None:
                raise RuntimeError("Temporal did not publish the fetched cleanup page")
            if len(entries) > self._settings.cleanup_page_size:
                raise RuntimeError("Temporal exceeded the scheduled-start cleanup page bound")
            cutoff = self._now() - timedelta(seconds=self._settings.terminal_retention_seconds)
            scanned = len(entries)
            deleted = 0
            for entry in entries:
                handle = self._client.get_schedule_handle(entry.id)
                description = await _describe_if_present(handle, self._rpc_timeout)
                if description is None:
                    continue
                if await _deletable_terminal_record(description, cutoff):
                    await _delete_if_present(handle, self._rpc_timeout)
                    deleted += 1
            result = ScheduledStartCleanupResult(
                cursor=_encode_cursor(iterator.next_page_token),
                scanned=scanned,
                deleted=deleted,
            )
        except (LimitExceededError, ValidationError, TypeError, ValueError) as exc:
            raise ApplicationError(
                "Scheduled-start cleanup input is invalid",
                type=SCHEDULED_START_CLEANUP_INVALID_INPUT,
                non_retryable=True,
            ) from exc
        except ApplicationError:
            raise
        except Exception as exc:
            raise ApplicationError(
                "Scheduled-start cleanup is unavailable",
                type=SCHEDULED_START_CLEANUP_FAILED,
            ) from exc
        return result.model_dump(mode="json")

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise RuntimeError("Scheduled-start cleanup clock returned a naive value")
        return value.astimezone(UTC).replace(microsecond=0)


class ScheduledStartMaintenance:
    def __init__(
        self,
        client: Client,
        settings: ScheduledStartSettings,
        *,
        task_queue: str,
    ) -> None:
        if not task_queue:
            raise ValueError("Scheduled-start maintenance task queue must not be empty")
        self._client = client
        self._settings = settings
        self._task_queue = task_queue
        self._schedule_id = SCHEDULED_START_MAINTENANCE_ID_PREFIX + safe_identity_digest(
            "scheduled-start-maintenance", task_queue
        )
        self._rpc_timeout = timedelta(seconds=settings.dispatch_timeout_seconds)

    async def ensure_schedule(self) -> None:
        desired = self._schedule()
        memo = {MEMO_SCHEDULED_START_OWNER: SCHEDULED_START_MAINTENANCE_OWNER}
        try:
            await self._client.create_schedule(
                self._schedule_id,
                desired,
                memo=memo,
                rpc_timeout=self._rpc_timeout,
            )
            return
        except ScheduleAlreadyRunningError:
            pass
        except Exception as exc:
            raise RuntimeError("Temporal did not accept scheduled-start maintenance") from exc

        handle = self._client.get_schedule_handle(self._schedule_id)

        async def update(input: ScheduleUpdateInput) -> ScheduleUpdate:
            owner = await input.description.memo_value(
                MEMO_SCHEDULED_START_OWNER,
                None,
                type_hint=str,
            )
            action = input.description.schedule.action
            if (
                owner != SCHEDULED_START_MAINTENANCE_OWNER
                or not isinstance(action, ScheduleActionStartWorkflow)
                or action.workflow != SCHEDULED_START_CLEANUP_WORKFLOW_TYPE
                or action.task_queue != self._task_queue
            ):
                raise RuntimeError("Maintenance schedule identity is owned by another resource")
            return ScheduleUpdate(schedule=desired)

        await handle.update(update, rpc_timeout=self._rpc_timeout)

    def _schedule(self) -> Schedule:
        return Schedule(
            action=ScheduleActionStartWorkflow(
                SCHEDULED_START_CLEANUP_WORKFLOW_TYPE,
                ScheduledStartCleanupPage().model_dump(mode="json"),
                id=self._schedule_id,
                task_queue=self._task_queue,
                memo={MEMO_SCHEDULED_START_OWNER: SCHEDULED_START_MAINTENANCE_OWNER},
            ),
            spec=ScheduleSpec(
                intervals=(
                    ScheduleIntervalSpec(
                        every=timedelta(seconds=self._settings.cleanup_interval_seconds)
                    ),
                )
            ),
            policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            state=ScheduleState(note="scheduled-start-maintenance"),
        )


async def _deletable_terminal_record(
    description: ScheduleDescription,
    cutoff: datetime,
) -> bool:
    owner = await description.memo_value(
        MEMO_SCHEDULED_START_OWNER,
        None,
        type_hint=str,
    )
    memo_id = await description.memo_value(
        MEMO_SCHEDULED_START_ID,
        None,
        type_hint=str,
    )
    memo_scope = await description.memo_value(
        MEMO_SCOPE_DIGEST,
        None,
        type_hint=str,
    )
    action = description.schedule.action
    if (
        owner != SCHEDULED_START_OWNER
        or not isinstance(memo_id, str)
        or not isinstance(memo_scope, str)
        or not isinstance(action, ScheduleActionStartWorkflow)
        or action.workflow != SCHEDULED_START_DUE_WORKFLOW_TYPE
        or len(action.args) != 1
    ):
        return False
    raw_action = await _decode_action_argument(description, action.args[0])
    record = ScheduledStartDueInput.model_validate(raw_action).record
    if not isinstance(
        record,
        (StartedScheduledStartRecord, CanceledScheduledStartRecord, FailedScheduledStartRecord),
    ):
        return False
    return (
        record.scheduled_start_id == memo_id
        and record.scope_digest == memo_scope
        and description.id
        == scoped_identity_from_digest(
            "scheduled-start-schedule",
            record.scope_digest,
            record.scheduled_start_id,
        )
        and action.id == make_scheduled_start_due_id_from_record(record)
        and record.completed_at <= cutoff
    )


def make_scheduled_start_due_id_from_record(
    record: (
        StartedScheduledStartRecord | CanceledScheduledStartRecord | FailedScheduledStartRecord
    ),
) -> str:
    return scoped_identity_from_digest(
        "scheduled-start-due",
        record.scope_digest,
        record.scheduled_start_id,
        str(record.version),
    )


async def _decode_action_argument(
    description: ScheduleDescription,
    value: object,
) -> object:
    if not isinstance(value, Payload):
        return value
    decoded = await description.data_converter.decode([value], [dict])
    if len(decoded) != 1:
        raise ValueError("Scheduled-start action payload could not be decoded")
    return decoded[0]


async def _delete_if_present(handle: ScheduleHandle, rpc_timeout: timedelta) -> None:
    try:
        await handle.delete(rpc_timeout=rpc_timeout)
    except RPCError as exc:
        if exc.status is not RPCStatusCode.NOT_FOUND:
            raise


async def _describe_if_present(
    handle: ScheduleHandle,
    rpc_timeout: timedelta,
) -> ScheduleDescription | None:
    try:
        return await handle.describe(rpc_timeout=rpc_timeout)
    except RPCError as exc:
        if exc.status is not RPCStatusCode.NOT_FOUND:
            raise
        return None


def _encode_cursor(value: bytes | None) -> str | None:
    if not value:
        return None
    encoded = base64.urlsafe_b64encode(value).decode("ascii")
    if len(encoded) > MAX_SCHEDULED_START_CURSOR_LENGTH:
        raise ValueError("Scheduled-start cleanup cursor exceeds its bound")
    return encoded


def _decode_cursor(value: str | None) -> bytes | None:
    if value is None:
        return None
    try:
        return base64.b64decode(value.encode("ascii"), altchars=b"-_", validate=True)
    except (UnicodeEncodeError, binascii.Error) as exc:
        raise ValueError("Scheduled-start cleanup cursor is invalid") from exc
