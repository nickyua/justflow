"""Standalone HTTP client and appointment-offset policy; no admin UI dependencies."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TypeVar
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ValidationError

from justflow.runtime import (
    ScheduledStartCancelRequest,
    ScheduledStartCreateRequest,
    ScheduledStartMutationResult,
    ScheduledStartRescheduleRequest,
)
from justflow.runtime.api_models import (
    ApiErrorResponse,
    CapabilitiesApiResponse,
    ScheduledStartCreateApiResponse,
    StartApiRequest,
    StartWorkflowApiResponse,
)

REMINDER_LEAD_TIME = timedelta(hours=2)
MAX_CLIENT_RESPONSE_BYTES = 512 * 1024
CLIENT_READ_CHUNK_BYTES = 64 * 1024
CLIENT_REQUEST_TIMEOUT_SECONDS = 30
ResponseModel = TypeVar("ResponseModel", bound=BaseModel)


class CustomerClientError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"Customer workflow request failed: {code}")


class AppointmentTooLateError(ValueError):
    """A new reminder is already due; the host must choose an explicit alternate action."""


def reminder_start_at(appointment_at: datetime, *, now: datetime) -> datetime:
    if appointment_at.utcoffset() is None or now.utcoffset() is None:
        raise ValueError("Appointment and current time must contain an explicit UTC offset")
    if appointment_at.microsecond:
        raise ValueError("Appointment timestamps require whole-second precision")
    due = appointment_at.astimezone(UTC) - REMINDER_LEAD_TIME
    if due <= now.astimezone(UTC):
        raise AppointmentTooLateError(
            "Reminder is already due; do not silently schedule or start it"
        )
    return due


class CustomerWorkflowClient:
    """Borrow a caller-owned AsyncClient configured with its URL, credential and finite timeout."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        if client.follow_redirects:
            raise ValueError("The workflow client requires redirects to be disabled")
        self._client = client

    async def capabilities(self) -> CapabilitiesApiResponse:
        return await self._request("GET", "/v1/operations/capabilities", CapabilitiesApiResponse)

    async def start(self, request: StartApiRequest) -> StartWorkflowApiResponse:
        return await self._request("POST", "/v1/workflows", StartWorkflowApiResponse, body=request)

    async def schedule(
        self, request: ScheduledStartCreateRequest, *, key: str
    ) -> ScheduledStartCreateApiResponse:
        return await self._request(
            "POST", "/v1/scheduled-starts", ScheduledStartCreateApiResponse, body=request, key=key
        )

    async def reschedule(
        self, identity: str, request: ScheduledStartRescheduleRequest, *, key: str
    ) -> ScheduledStartMutationResult:
        return await self._request(
            "POST",
            f"/v1/scheduled-starts/{quote(identity, safe='')}/reschedule",
            ScheduledStartMutationResult,
            body=request,
            key=key,
        )

    async def cancel(
        self, identity: str, request: ScheduledStartCancelRequest, *, key: str
    ) -> ScheduledStartMutationResult:
        return await self._request(
            "POST",
            f"/v1/scheduled-starts/{quote(identity, safe='')}/cancel",
            ScheduledStartMutationResult,
            body=request,
            key=key,
        )

    async def _request(
        self,
        method: str,
        path: str,
        model: type[ResponseModel],
        *,
        body: BaseModel | None = None,
        key: str | None = None,
    ) -> ResponseModel:
        headers = {"accept-encoding": "identity"}
        if key is not None:
            headers["x-idempotency-key"] = key
        try:
            async with (
                asyncio.timeout(CLIENT_REQUEST_TIMEOUT_SECONDS),
                self._client.stream(
                    method,
                    path,
                    json=body.model_dump(mode="json") if body is not None else None,
                    headers=headers,
                ) as response,
            ):
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise CustomerClientError("encoded_response_unsupported")
                payload = bytearray()
                async for chunk in response.aiter_bytes(CLIENT_READ_CHUNK_BYTES):
                    if len(payload) + len(chunk) > MAX_CLIENT_RESPONSE_BYTES:
                        raise CustomerClientError("response_too_large")
                    payload.extend(chunk)
                raw = bytes(payload)
                if response.is_error:
                    raise CustomerClientError(ApiErrorResponse.model_validate_json(raw).error.code)
                return model.model_validate_json(raw)
        except (httpx.HTTPError, TimeoutError) as exc:
            raise CustomerClientError("transport_unavailable") from exc
        except ValidationError as exc:
            raise CustomerClientError("invalid_response") from exc
