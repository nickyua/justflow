"""Bounded scheduled-start cleanup and maintenance tests."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
)
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode

from justflow.config.settings import ScheduledStartSettings, ScheduledStartWorkloadClass
from justflow.definitions.routing import MEMO_SCOPE_DIGEST
from justflow.runtime.scheduled_start_cleanup import (
    SCHEDULED_START_CLEANUP_FAILED,
    SCHEDULED_START_CLEANUP_INVALID_INPUT,
    ScheduledStartCleanupActivity,
    ScheduledStartCleanupResult,
    ScheduledStartMaintenance,
    make_scheduled_start_due_id_from_record,
)
from justflow.runtime.scheduled_starts import (
    MEMO_SCHEDULED_START_ID,
    MEMO_SCHEDULED_START_OWNER,
    SCHEDULED_START_DUE_WORKFLOW_TYPE,
    SCHEDULED_START_MAINTENANCE_ID_PREFIX,
    SCHEDULED_START_MAINTENANCE_OWNER,
    SCHEDULED_START_OWNER,
    CanceledScheduledStartRecord,
    PendingScheduledStartRecord,
    ScheduledStartCancelRequest,
    ScheduledStartCreateRequest,
    ScheduledStartDueInput,
    cancel_scheduled_start,
    create_scheduled_start_record,
    make_scheduled_start_id,
    make_scheduled_start_schedule_id,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE

WORKFLOW_NAME = "reporting"
TRIGGER_NAME = "reporting_api"
NOW = datetime(2026, 8, 11, 8, 0, tzinfo=UTC)
RETENTION_SECONDS = 100
CLEANUP_INTERVAL_SECONDS = 120


class FakeScheduleAsyncIterator:
    def __init__(
        self,
        pages: tuple[tuple[object, ...], ...],
        *,
        next_page_token: bytes | None,
    ) -> None:
        self._pages = pages
        self._next_page_index = _decode_page_index(next_page_token)
        self.current_page: tuple[object, ...] | None = None
        self.current_page_index = 0
        self.next_page_token = next_page_token
        self.fetch_count = 0

    async def fetch_next_page(self, *, page_size: int | None = None) -> None:
        del page_size
        self.fetch_count += 1
        if self._next_page_index >= len(self._pages):
            self.current_page = ()
            self.next_page_token = None
            return
        self.current_page = self._pages[self._next_page_index]
        self.current_page_index = 0
        self._next_page_index += 1
        self.next_page_token = (
            _encode_page_index(self._next_page_index)
            if self._next_page_index < len(self._pages)
            else None
        )

    def __aiter__(self) -> FakeScheduleAsyncIterator:
        return self

    async def __anext__(self) -> object:
        while True:
            if self.current_page is None:
                await self.fetch_next_page()
                continue
            if self.current_page_index < len(self.current_page):
                value = self.current_page[self.current_page_index]
                self.current_page_index += 1
                return value
            if self.next_page_token is None:
                raise StopAsyncIteration
            await self.fetch_next_page()


def _encode_page_index(page_index: int) -> bytes:
    return str(page_index).encode("ascii")


def _decode_page_index(page_token: bytes | None) -> int:
    return int(page_token.decode("ascii")) if page_token is not None else 0


def _cleanup_cursor(page_index: int | None) -> str | None:
    if page_index is None or page_index == 0:
        return None
    return base64.urlsafe_b64encode(_encode_page_index(page_index)).decode("ascii")


class FakeDescription:
    def __init__(self, schedule_id: str, schedule: Schedule, memo: dict[str, str]) -> None:
        self.id = schedule_id
        self.schedule = schedule
        self._memo = memo

    async def memo_value(self, key: str, default: object, *, type_hint: type) -> object:
        del type_hint
        return self._memo.get(key, default)


class FakeHandle:
    def __init__(
        self,
        description: FakeDescription,
        *,
        describe_error: Exception | None = None,
        delete_error: Exception | None = None,
    ) -> None:
        self.description = description
        self.deleted = False
        self.describe_error = describe_error
        self.delete_error = delete_error
        self.update_count = 0
        self.delete_attempts = 0

    async def describe(self, **_: object) -> FakeDescription:
        if self.describe_error is not None:
            raise self.describe_error
        return self.description

    async def delete(self, **_: object) -> None:
        self.delete_attempts += 1
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted = True

    async def update(self, updater, **_: object) -> None:
        result = await updater(SimpleNamespace(description=self.description))
        if isinstance(result, ScheduleUpdate):
            self.update_count += 1
            self.description = FakeDescription(
                self.description.id,
                result.schedule,
                self.description._memo,
            )


def scheduled_record(
    business_request_id: str,
    *,
    completed_at: datetime | None = None,
) -> PendingScheduledStartRecord | CanceledScheduledStartRecord:
    request = ScheduledStartCreateRequest(
        workflow_name=WORKFLOW_NAME,
        input={"reference": "private-reference"},
        business_request_id=business_request_id,
        start_at=NOW + timedelta(hours=1),
        workload_class=ScheduledStartWorkloadClass.STANDARD,
    )
    record = create_scheduled_start_record(
        request,
        scheduled_start_id=make_scheduled_start_id(
            LOCAL_RUNTIME_SCOPE,
            WORKFLOW_NAME,
            business_request_id,
        ),
        scope=LOCAL_RUNTIME_SCOPE,
        trigger_name=TRIGGER_NAME,
        request_digest="d" * 64,
        accepted_at=NOW - timedelta(hours=1),
        normalized_input=dict(request.input),
    )
    if completed_at is None:
        return record
    canceled, _ = cancel_scheduled_start(
        record,
        ScheduledStartCancelRequest(expected_version=1),
        request_digest="e" * 64,
        updated_at=completed_at,
    )
    return canceled


def description_for(
    record: PendingScheduledStartRecord | CanceledScheduledStartRecord,
    *,
    owner: str = SCHEDULED_START_OWNER,
) -> FakeDescription:
    action_id = (
        make_scheduled_start_due_id_from_record(record)
        if isinstance(record, CanceledScheduledStartRecord)
        else "unused-pending-action"
    )
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            SCHEDULED_START_DUE_WORKFLOW_TYPE,
            ScheduledStartDueInput(record=record).model_dump(mode="json"),
            id=action_id,
            task_queue="scheduled-start-dispatch",
        ),
        spec=ScheduleSpec(),
        state=ScheduleState(),
    )
    return FakeDescription(
        make_scheduled_start_schedule_id(LOCAL_RUNTIME_SCOPE, record.scheduled_start_id),
        schedule,
        {
            MEMO_SCHEDULED_START_OWNER: owner,
            MEMO_SCHEDULED_START_ID: record.scheduled_start_id,
            MEMO_SCOPE_DIGEST: LOCAL_RUNTIME_SCOPE.digest,
        },
    )


class CleanupRecordKind(str, Enum):
    EXPIRED = "expired"
    RECENT = "recent"
    PENDING = "pending"
    FOREIGN = "foreign"


@dataclass(frozen=True, kw_only=True)
class CleanupRecordSpec:
    id: str
    kind: CleanupRecordKind


@dataclass(frozen=True, kw_only=True)
class CleanupPageCase:
    id: str
    pages: tuple[tuple[CleanupRecordSpec, ...], ...]
    page_size: int
    starting_page: int
    expected_scanned_ids: tuple[str, ...]
    expected_deleted_ids: tuple[str, ...]
    expected_next_page: int | None


PAGED_RECORDS = (
    CleanupRecordSpec(id="expired-first", kind=CleanupRecordKind.EXPIRED),
    CleanupRecordSpec(id="pending-first", kind=CleanupRecordKind.PENDING),
    CleanupRecordSpec(id="recent-middle", kind=CleanupRecordKind.RECENT),
    CleanupRecordSpec(id="expired-middle", kind=CleanupRecordKind.EXPIRED),
    CleanupRecordSpec(id="foreign-final", kind=CleanupRecordKind.FOREIGN),
)
PAGED_RECORD_PAGES = (PAGED_RECORDS[:2], PAGED_RECORDS[2:4], PAGED_RECORDS[4:])
CLEANUP_PAGE_CASES = [
    CleanupPageCase(
        id="first",
        pages=PAGED_RECORD_PAGES,
        page_size=2,
        starting_page=0,
        expected_scanned_ids=("expired-first", "pending-first"),
        expected_deleted_ids=("expired-first",),
        expected_next_page=1,
    ),
    CleanupPageCase(
        id="middle",
        pages=PAGED_RECORD_PAGES,
        page_size=2,
        starting_page=1,
        expected_scanned_ids=("recent-middle", "expired-middle"),
        expected_deleted_ids=("expired-middle",),
        expected_next_page=2,
    ),
    CleanupPageCase(
        id="final",
        pages=PAGED_RECORD_PAGES,
        page_size=2,
        starting_page=2,
        expected_scanned_ids=("foreign-final",),
        expected_deleted_ids=(),
        expected_next_page=None,
    ),
    CleanupPageCase(
        id="empty",
        pages=(
            (CleanupRecordSpec(id="pending-before-empty", kind=CleanupRecordKind.PENDING),),
            (),
            (CleanupRecordSpec(id="expired-after-empty", kind=CleanupRecordKind.EXPIRED),),
        ),
        page_size=1,
        starting_page=1,
        expected_scanned_ids=(),
        expected_deleted_ids=(),
        expected_next_page=2,
    ),
    CleanupPageCase(
        id="mixed",
        pages=(
            (
                CleanupRecordSpec(id="mixed-expired", kind=CleanupRecordKind.EXPIRED),
                CleanupRecordSpec(id="mixed-recent", kind=CleanupRecordKind.RECENT),
                CleanupRecordSpec(id="mixed-pending", kind=CleanupRecordKind.PENDING),
                CleanupRecordSpec(id="mixed-foreign", kind=CleanupRecordKind.FOREIGN),
            ),
        ),
        page_size=4,
        starting_page=0,
        expected_scanned_ids=(
            "mixed-expired",
            "mixed-recent",
            "mixed-pending",
            "mixed-foreign",
        ),
        expected_deleted_ids=("mixed-expired",),
        expected_next_page=None,
    ),
]


def _description_for_spec(spec: CleanupRecordSpec) -> FakeDescription:
    completed_at = {
        CleanupRecordKind.EXPIRED: NOW - timedelta(seconds=RETENTION_SECONDS + 1),
        CleanupRecordKind.RECENT: NOW - timedelta(seconds=RETENTION_SECONDS - 1),
        CleanupRecordKind.PENDING: None,
        CleanupRecordKind.FOREIGN: NOW - timedelta(seconds=RETENTION_SECONDS + 1),
    }[spec.kind]
    owner = "another-owner" if spec.kind is CleanupRecordKind.FOREIGN else SCHEDULED_START_OWNER
    return description_for(
        scheduled_record(spec.id, completed_at=completed_at),
        owner=owner,
    )


def _cleanup_client(
    pages: tuple[tuple[FakeDescription, ...], ...],
    *,
    handles: dict[str, FakeHandle] | None = None,
) -> tuple[MagicMock, dict[str, FakeHandle], list[FakeScheduleAsyncIterator]]:
    descriptions = tuple(description for page in pages for description in page)
    resolved_handles = (
        handles if handles is not None else {item.id: FakeHandle(item) for item in descriptions}
    )
    iterators: list[FakeScheduleAsyncIterator] = []
    client = MagicMock()

    async def list_schedules(**kwargs: object) -> FakeScheduleAsyncIterator:
        page_size = kwargs["page_size"]
        next_page_token = kwargs["next_page_token"]
        assert isinstance(page_size, int)
        assert isinstance(next_page_token, bytes | None)
        iterator = FakeScheduleAsyncIterator(
            tuple(
                tuple(SimpleNamespace(id=description.id) for description in page) for page in pages
            ),
            next_page_token=next_page_token,
        )
        iterators.append(iterator)
        return iterator

    client.list_schedules = AsyncMock(side_effect=list_schedules)
    client.get_schedule_handle.side_effect = resolved_handles.__getitem__
    return client, resolved_handles, iterators


@pytest.mark.parametrize("case", CLEANUP_PAGE_CASES, ids=lambda case: case.id)
async def test_cleanup_processes_exactly_one_server_page(case: CleanupPageCase) -> None:
    descriptions_by_id = {
        spec.id: _description_for_spec(spec) for page in case.pages for spec in page
    }
    description_pages = tuple(
        tuple(descriptions_by_id[spec.id] for spec in page) for page in case.pages
    )
    client, handles, iterators = _cleanup_client(description_pages)
    activity = ScheduledStartCleanupActivity(
        client,
        ScheduledStartSettings(
            terminal_retention_seconds=RETENTION_SECONDS,
            cleanup_page_size=case.page_size,
        ),
        clock=lambda: NOW,
    )

    raw_result = await activity.cleanup({"cursor": _cleanup_cursor(case.starting_page)})
    result = ScheduledStartCleanupResult.model_validate(raw_result)

    expected_scanned_schedule_ids = tuple(
        descriptions_by_id[record_id].id for record_id in case.expected_scanned_ids
    )
    deleted_record_ids = tuple(
        record_id
        for record_id, description in descriptions_by_id.items()
        if handles[description.id].deleted
    )
    assert iterators[0].fetch_count == 1
    assert client.list_schedules.await_args.kwargs["page_size"] == case.page_size
    assert (
        tuple(call.args[0] for call in client.get_schedule_handle.call_args_list)
        == expected_scanned_schedule_ids
    )
    assert result.scanned == len(case.expected_scanned_ids)
    assert result.scanned <= case.page_size
    assert result.deleted == len(case.expected_deleted_ids)
    assert deleted_record_ids == case.expected_deleted_ids
    assert result.cursor == _cleanup_cursor(case.expected_next_page)


async def test_cleanup_cursor_walk_scans_each_stable_entry_once() -> None:
    descriptions_by_id = {spec.id: _description_for_spec(spec) for spec in PAGED_RECORDS}
    description_pages = tuple(
        tuple(descriptions_by_id[spec.id] for spec in page) for page in PAGED_RECORD_PAGES
    )
    client, handles, iterators = _cleanup_client(description_pages)
    settings = ScheduledStartSettings(
        terminal_retention_seconds=RETENTION_SECONDS,
        cleanup_page_size=2,
    )
    activity = ScheduledStartCleanupActivity(
        client,
        settings,
        clock=lambda: NOW,
    )
    cursor: str | None = None
    results: list[ScheduledStartCleanupResult] = []

    while True:
        result = ScheduledStartCleanupResult.model_validate(
            await activity.cleanup({"cursor": cursor})
        )
        results.append(result)
        cursor = result.cursor
        if cursor is None:
            break

    expected_schedule_ids = tuple(descriptions_by_id[spec.id].id for spec in PAGED_RECORDS)
    deleted_record_ids = tuple(
        spec.id for spec in PAGED_RECORDS if handles[descriptions_by_id[spec.id].id].deleted
    )
    assert (
        tuple(call.args[0] for call in client.get_schedule_handle.call_args_list)
        == expected_schedule_ids
    )
    assert all(iterator.fetch_count == 1 for iterator in iterators)
    assert all(result.scanned <= settings.cleanup_page_size for result in results)
    assert deleted_record_ids == ("expired-first", "expired-middle")


async def test_cleanup_treats_an_already_deleted_terminal_schedule_as_success() -> None:
    expired = description_for(
        scheduled_record(
            "already-deleted",
            completed_at=NOW - timedelta(seconds=RETENTION_SECONDS + 1),
        )
    )
    handle = FakeHandle(
        expired,
        delete_error=RPCError("missing", RPCStatusCode.NOT_FOUND, b""),
    )
    client, _, _ = _cleanup_client(((expired,),), handles={expired.id: handle})
    activity = ScheduledStartCleanupActivity(
        client,
        ScheduledStartSettings(terminal_retention_seconds=RETENTION_SECONDS),
        clock=lambda: NOW,
    )

    result = ScheduledStartCleanupResult.model_validate(await activity.cleanup({}))

    assert result.deleted == 1
    assert result.cursor is None
    assert handle.deleted is False


async def test_cleanup_treats_a_stale_missing_description_as_success() -> None:
    expired = _description_for_spec(
        CleanupRecordSpec(id="missing-before-describe", kind=CleanupRecordKind.EXPIRED)
    )
    handle = FakeHandle(
        expired,
        describe_error=RPCError("missing", RPCStatusCode.NOT_FOUND, b""),
    )
    client, _, _ = _cleanup_client(((expired,),), handles={expired.id: handle})
    activity = ScheduledStartCleanupActivity(
        client,
        ScheduledStartSettings(terminal_retention_seconds=RETENTION_SECONDS),
        clock=lambda: NOW,
    )

    result = ScheduledStartCleanupResult.model_validate(await activity.cleanup({}))

    assert result.scanned == 1
    assert result.deleted == 0
    assert handle.delete_attempts == 0


class AppliedDeleteThenUnavailableHandle(FakeHandle):
    def __init__(self, description: FakeDescription) -> None:
        super().__init__(description)
        self.delete_effects = 0

    async def delete(self, **_: object) -> None:
        self.delete_attempts += 1
        if self.delete_attempts == 1:
            self.deleted = True
            self.delete_effects += 1
            raise RPCError("response lost", RPCStatusCode.UNAVAILABLE, b"")
        raise RPCError("missing", RPCStatusCode.NOT_FOUND, b"")


async def test_cleanup_retry_after_an_applied_delete_is_idempotent() -> None:
    expired = _description_for_spec(
        CleanupRecordSpec(id="retry-after-delete", kind=CleanupRecordKind.EXPIRED)
    )
    handle = AppliedDeleteThenUnavailableHandle(expired)
    client, _, _ = _cleanup_client(((expired,),), handles={expired.id: handle})
    activity = ScheduledStartCleanupActivity(
        client,
        ScheduledStartSettings(terminal_retention_seconds=RETENTION_SECONDS),
        clock=lambda: NOW,
    )

    with pytest.raises(ApplicationError) as first_attempt:
        await activity.cleanup({})
    second_result = ScheduledStartCleanupResult.model_validate(await activity.cleanup({}))

    assert first_attempt.value.type == SCHEDULED_START_CLEANUP_FAILED
    assert first_attempt.value.non_retryable is False
    assert second_result.deleted == 1
    assert handle.delete_attempts == 2
    assert handle.delete_effects == 1


async def test_cleanup_rejects_a_missing_sdk_page() -> None:
    iterator = MagicMock()
    iterator.current_page = None
    iterator.next_page_token = None
    iterator.fetch_next_page = AsyncMock()
    iterator.__aiter__.return_value = iter(())
    client = MagicMock()
    client.list_schedules = AsyncMock(return_value=iterator)
    activity = ScheduledStartCleanupActivity(client, ScheduledStartSettings(), clock=lambda: NOW)

    with pytest.raises(ApplicationError) as raised:
        await activity.cleanup({})

    assert raised.value.type == SCHEDULED_START_CLEANUP_FAILED
    assert raised.value.non_retryable is False
    iterator.fetch_next_page.assert_awaited_once_with()
    client.get_schedule_handle.assert_not_called()


async def test_cleanup_rejects_an_oversized_sdk_page() -> None:
    settings = ScheduledStartSettings(cleanup_page_size=2)
    specs = tuple(
        CleanupRecordSpec(id=f"oversized-{index}", kind=CleanupRecordKind.PENDING)
        for index in range(settings.cleanup_page_size + 1)
    )
    descriptions = tuple(_description_for_spec(spec) for spec in specs)
    client, _, _ = _cleanup_client((descriptions,))
    activity = ScheduledStartCleanupActivity(
        client,
        settings,
        clock=lambda: NOW,
    )

    with pytest.raises(ApplicationError) as raised:
        await activity.cleanup({})

    assert raised.value.type == SCHEDULED_START_CLEANUP_FAILED
    assert raised.value.non_retryable is False
    client.get_schedule_handle.assert_not_called()


async def test_cleanup_rejects_an_invalid_cursor() -> None:
    client = MagicMock()
    activity = ScheduledStartCleanupActivity(client, ScheduledStartSettings(), clock=lambda: NOW)

    with pytest.raises(ApplicationError) as raised:
        await activity.cleanup({"cursor": "not-base64!"})

    assert raised.value.type == SCHEDULED_START_CLEANUP_INVALID_INPUT
    assert raised.value.non_retryable is True
    client.list_schedules.assert_not_called()


async def test_cleanup_classifies_temporal_unavailability() -> None:
    client = MagicMock()
    client.list_schedules = AsyncMock(side_effect=RuntimeError("transport failed"))
    activity = ScheduledStartCleanupActivity(client, ScheduledStartSettings(), clock=lambda: NOW)

    with pytest.raises(ApplicationError) as raised:
        await activity.cleanup({})

    assert raised.value.type == SCHEDULED_START_CLEANUP_FAILED
    assert raised.value.non_retryable is False


async def test_cleanup_classifies_description_unavailability() -> None:
    expired = _description_for_spec(
        CleanupRecordSpec(id="description-unavailable", kind=CleanupRecordKind.EXPIRED)
    )
    handle = FakeHandle(
        expired,
        describe_error=RPCError("unavailable", RPCStatusCode.UNAVAILABLE, b""),
    )
    client, _, _ = _cleanup_client(((expired,),), handles={expired.id: handle})
    activity = ScheduledStartCleanupActivity(client, ScheduledStartSettings(), clock=lambda: NOW)

    with pytest.raises(ApplicationError) as raised:
        await activity.cleanup({})

    assert raised.value.type == SCHEDULED_START_CLEANUP_FAILED
    assert raised.value.non_retryable is False


async def test_maintenance_uses_one_engine_owned_interval_schedule() -> None:
    client = MagicMock()
    client.create_schedule = AsyncMock()
    settings = ScheduledStartSettings(cleanup_interval_seconds=CLEANUP_INTERVAL_SECONDS)
    maintenance = ScheduledStartMaintenance(
        client,
        settings,
        task_queue="scheduled-start-dispatch",
    )

    await maintenance.ensure_schedule()

    schedule_id, schedule = client.create_schedule.await_args.args
    assert schedule_id.startswith(SCHEDULED_START_MAINTENANCE_ID_PREFIX)
    assert schedule.spec.intervals[0].every == timedelta(seconds=CLEANUP_INTERVAL_SECONDS)
    assert isinstance(schedule.action, ScheduleActionStartWorkflow)
    assert schedule.action.id == schedule_id
    assert client.create_schedule.await_args.kwargs["memo"] == {
        MEMO_SCHEDULED_START_OWNER: SCHEDULED_START_MAINTENANCE_OWNER
    }


async def test_maintenance_reconciles_only_its_owned_schedule() -> None:
    settings = ScheduledStartSettings(cleanup_interval_seconds=CLEANUP_INTERVAL_SECONDS)
    seed_client = MagicMock()
    seed_client.create_schedule = AsyncMock()
    seed = ScheduledStartMaintenance(
        seed_client,
        settings,
        task_queue="scheduled-start-dispatch",
    )
    await seed.ensure_schedule()
    schedule_id, schedule = seed_client.create_schedule.await_args.args
    description = FakeDescription(
        schedule_id,
        schedule,
        {MEMO_SCHEDULED_START_OWNER: SCHEDULED_START_MAINTENANCE_OWNER},
    )
    handle = FakeHandle(description)
    client = MagicMock()
    client.create_schedule = AsyncMock(side_effect=ScheduleAlreadyRunningError())
    client.get_schedule_handle.return_value = handle
    maintenance = ScheduledStartMaintenance(
        client,
        settings,
        task_queue="scheduled-start-dispatch",
    )

    await maintenance.ensure_schedule()

    assert handle.update_count == 1

    handle.description._memo[MEMO_SCHEDULED_START_OWNER] = "another-owner"

    with pytest.raises(RuntimeError, match="owned by another resource"):
        await maintenance.ensure_schedule()


async def test_maintenance_classifies_schedule_creation_failure() -> None:
    client = MagicMock()
    client.create_schedule = AsyncMock(side_effect=RuntimeError("transport failed"))
    maintenance = ScheduledStartMaintenance(
        client,
        ScheduledStartSettings(),
        task_queue="scheduled-start-dispatch",
    )

    with pytest.raises(RuntimeError, match="did not accept"):
        await maintenance.ensure_schedule()
