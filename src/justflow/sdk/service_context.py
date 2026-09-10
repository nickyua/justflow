"""Trusted scope identity propagated to application service boundaries."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from justflow.scope import SCOPE_DIGEST_LENGTH

SERVICE_CALL_CONTEXT_VERSION: Literal["1"] = "1"
HTTP_SCOPE_DIGEST_HEADER = "Justflow-Scope-Digest"
GRPC_SCOPE_DIGEST_METADATA_KEY = "justflow-scope-digest"
LAMBDA_SERVICE_CALL_CONTEXT_FIELD = "justflow"


class ServiceCallContext(BaseModel):
    """Opaque scope identity asserted over an authenticated service boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    version: Literal["1"] = SERVICE_CALL_CONTEXT_VERSION
    scope_digest: str | None = Field(
        default=None,
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )
