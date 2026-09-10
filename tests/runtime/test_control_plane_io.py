"""Storage latency must leave the HTTP event loop available."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import TypeVar
from unittest.mock import MagicMock

import pytest

from justflow.config.settings import ControlSettings
from justflow.runtime.blocking_io import (
    MAX_CONTROL_PLANE_BLOCKING_OPERATIONS,
    ControlPlaneBusyError,
    run_blocking,
)
from justflow.runtime.configuration_api import ConfigurationApi
from justflow.scope import LOCAL_RUNTIME_SCOPE, ScopeBindingKind, TrustedScopeBinding

DEADLOCK_GUARD_SECONDS = 2
SINGLE_EXECUTOR_WORKER = 1
T = TypeVar("T")


async def test_blocking_configuration_read_allows_an_independent_request() -> None:
    independent = threading.Event()
    entered = threading.Event()
    observed: list[bool] = []
    publication = MagicMock()

    def read_draft(scope):
        entered.set()
        observed.append(independent.wait(DEADLOCK_GUARD_SECONDS))
        return MagicMock(model_dump=lambda **kwargs: {"version": 1})

    publication.read_draft.side_effect = read_draft
    api = ConfigurationApi(
        settings=ControlSettings(), publication=publication, activation=MagicMock()
    )

    async def body() -> bytes:
        return b""

    async def another_request() -> None:
        await asyncio.to_thread(entered.wait, DEADLOCK_GUARD_SECONDS)
        independent.set()

    await asyncio.gather(
        api.dispatch(
            api.resolve_route("GET", ("api", "configuration", "draft")),
            query={},
            headers=(),
            read_body=body,
            scope_binding=TrustedScopeBinding.create(
                scope=LOCAL_RUNTIME_SCOPE, kind=ScopeBindingKind.API, binding_id="test"
            ),
        ),
        another_request(),
    )
    assert observed == [True]


@pytest.mark.parametrize(
    "executor_workers",
    [
        pytest.param(SINGLE_EXECUTOR_WORKER, id="queued-work"),
        pytest.param(MAX_CONTROL_PLANE_BLOCKING_OPERATIONS, id="all-work-running"),
    ],
)
async def test_cancellation_retains_admission_until_blocking_work_finishes(
    executor_workers: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = threading.Event()
    entered = asyncio.Queue[None]()
    submitted = asyncio.Queue[None]()
    completions: list[Awaitable[object]] = []
    loop = asyncio.get_running_loop()
    run_in_executor = loop.run_in_executor

    def track_submission(
        executor: Executor | None, operation: Callable[..., T], *args: object
    ) -> asyncio.Future[T]:
        future = run_in_executor(executor, operation, *args)
        completions.append(future)
        submitted.put_nowait(None)
        return future

    monkeypatch.setattr(loop, "run_in_executor", track_submission)

    def operation() -> None:
        loop.call_soon_threadsafe(entered.put_nowait, None)
        release.wait()

    with ThreadPoolExecutor(max_workers=executor_workers) as executor:
        loop.set_default_executor(executor)
        requests = [
            asyncio.create_task(run_blocking(operation))
            for _ in range(MAX_CONTROL_PLANE_BLOCKING_OPERATIONS)
        ]
        try:
            for _ in requests:
                await asyncio.wait_for(submitted.get(), DEADLOCK_GUARD_SECONDS)
            for _ in range(executor_workers):
                await asyncio.wait_for(entered.get(), DEADLOCK_GUARD_SECONDS)
            for request in requests:
                request.cancel()
            outcomes = await asyncio.gather(*requests, return_exceptions=True)
            assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
            with pytest.raises(ControlPlaneBusyError):
                await run_blocking(lambda: None)
        finally:
            release.set()
            await asyncio.gather(*requests, return_exceptions=True)
            await asyncio.gather(*completions)
        assert await run_blocking(lambda: "available") == "available"
