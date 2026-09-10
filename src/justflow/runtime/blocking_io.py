"""Process-wide admission for blocking control-plane operations."""

from __future__ import annotations

import asyncio
import contextvars
import logging
import threading
from collections.abc import Callable
from functools import partial
from typing import ParamSpec, TypeVar

MAX_CONTROL_PLANE_BLOCKING_OPERATIONS = 8
_admission = threading.BoundedSemaphore(MAX_CONTROL_PLANE_BLOCKING_OPERATIONS)
logger = logging.getLogger(__name__)
P = ParamSpec("P")
T = TypeVar("T")


class ControlPlaneBusyError(Exception):
    """All blocking-operation slots are occupied; retry the request later."""


async def run_blocking(operation: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    if not _admission.acquire(blocking=False):
        raise ControlPlaneBusyError("Control-plane storage capacity is occupied")
    context = contextvars.copy_context()
    try:
        future = asyncio.get_running_loop().run_in_executor(
            None, partial(context.run, operation, *args, **kwargs)
        )
    except BaseException:
        _admission.release()
        raise
    abandoned = False

    def completed(result: asyncio.Future[T]) -> None:
        _admission.release()
        error = result.exception() if not result.cancelled() else None
        if abandoned and error is not None:
            logger.warning(
                "Blocking control-plane operation failed after request cancellation",
                extra={"error_type": type(error).__name__},
            )

    future.add_done_callback(completed)
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        abandoned = True
        raise
