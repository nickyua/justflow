"""Correlation context for log records.

Any log line emitted while a step executes carries request_id/flow_name/
step_name, so one `grep request_id=X` reconstructs a run across the trigger
consumer, workflow activities, service actions, relay, and archival.
"""

from __future__ import annotations

import contextvars
import hashlib
import logging
from collections.abc import Iterator
from contextlib import contextmanager

UNSET = "-"

_request_context: contextvars.ContextVar[dict[str, str] | None] = contextvars.ContextVar(
    "justflow_request_context", default=None
)

LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] %(name)s "
    "[req=%(request_id)s flow=%(flow_name)s step=%(step_name)s]: %(message)s"
)


@contextmanager
def logging_context(
    request_id: str = UNSET, flow_name: str = UNSET, step_name: str = UNSET
) -> Iterator[None]:
    token = _request_context.set(
        {"request_id": request_id, "flow_name": flow_name, "step_name": step_name}
    )
    try:
        yield
    finally:
        _request_context.reset(token)


def current_context() -> dict[str, str]:
    return _request_context.get() or {}


def identity_log_digest(value: str) -> str:
    """Return a stable, non-reversible identity suitable for log correlation."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class ContextFilter(logging.Filter):
    """Injects the correlation fields into every record passing through."""

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = current_context()
        record.request_id = ctx.get("request_id", UNSET)
        record.flow_name = ctx.get("flow_name", UNSET)
        record.step_name = ctx.get("step_name", UNSET)
        return True


def configure_logging(level: str) -> None:
    """Set up root logging with correlation fields; call once at startup."""
    logging.basicConfig(level=getattr(logging, level), format=LOG_FORMAT)
    for handler in logging.getLogger().handlers:
        handler.addFilter(ContextFilter())
