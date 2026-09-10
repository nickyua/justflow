"""Bounded consumption and ownership of synchronous binary response bodies."""

from __future__ import annotations

BINARY_READ_CHUNK_BYTES = 64 * 1024


class BoundedReadError(ValueError):
    """A response body is invalid or exceeds its declared bound."""


class BoundedReadLimitError(BoundedReadError):
    """Reading stopped at the configured byte limit."""


def read_bounded_body(body: object, *, limit: int) -> bytes:
    if limit < 1:
        raise ValueError("Binary read limit must be positive")
    if isinstance(body, bytes):
        if len(body) > limit:
            raise BoundedReadLimitError("Response body exceeds its byte limit")
        return body
    read = getattr(body, "read", None)
    close = getattr(body, "close", None)
    failure: BaseException | None = None
    try:
        if not callable(read):
            raise BoundedReadError("Response body is not a readable binary stream")
        payload = bytearray()
        while True:
            chunk = read(min(BINARY_READ_CHUNK_BYTES, limit + 1 - len(payload)))
            if not isinstance(chunk, bytes):
                raise BoundedReadError("Response body returned non-byte data")
            if len(payload) + len(chunk) > limit:
                raise BoundedReadLimitError("Response body exceeds its byte limit")
            if not chunk:
                return bytes(payload)
            payload.extend(chunk)
    except BaseException as exc:
        failure = exc
        raise
    finally:
        if callable(close):
            try:
                close()
            except Exception as exc:
                if failure is None:
                    raise
                failure.add_note(f"Response body cleanup also failed: {type(exc).__name__}")
