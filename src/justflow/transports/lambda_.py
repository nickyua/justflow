"""Lambda transport - AWS Lambda function invocation."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from pydantic import Field

from justflow.config.models import MAX_PATH_LENGTH
from justflow.optional_dependencies import load_optional_dependency
from justflow.sdk.service_context import LAMBDA_SERVICE_CALL_CONTEXT_FIELD
from justflow.transports.base import (
    Completed,
    ConnectionTimeoutError,
    DispatchTimeoutError,
    MalformedResponseError,
    ServiceError,
    StrictTransportConfig,
    TransportConnectionError,
    TransportRequest,
)

NON_RETRYABLE_LAMBDA_ERROR_CODES = frozenset(
    {
        "AccessDeniedException",
        "InvalidParameterValueException",
        "InvalidRequestContentException",
        "KMSAccessDeniedException",
        "RequestTooLargeException",
        "ResourceNotFoundException",
        "UnsupportedMediaTypeException",
    }
)
SDK_TOTAL_ATTEMPTS = 1


class LambdaTransportConfig(StrictTransportConfig):
    function_name: str = Field(min_length=1, max_length=MAX_PATH_LENGTH)


class LambdaTransport:
    """Invokes AWS Lambda functions synchronously.

    boto3 ships in the ``aws`` extra; it is imported on first use so the core
    package works without it.
    """

    def __init__(
        self,
        config: LambdaTransportConfig,
        *,
        connect_timeout_sec: int,
        dispatch_timeout_sec: int,
        lambda_client: Any | None = None,
    ):
        if lambda_client is None:
            boto3 = load_optional_dependency("boto3", extra="aws", feature="the Lambda transport")
            botocore_config = load_optional_dependency(
                "botocore.config",
                extra="aws",
                feature="the Lambda transport",
            )
            lambda_client = boto3.client(
                "lambda",
                config=botocore_config.Config(
                    connect_timeout=connect_timeout_sec,
                    read_timeout=dispatch_timeout_sec,
                    retries={"total_max_attempts": SDK_TOTAL_ATTEMPTS, "mode": "standard"},
                ),
            )
        self._config = config
        self._lambda = lambda_client
        self._dispatch_timeout_sec = dispatch_timeout_sec

    async def send(self, request: TransportRequest) -> Completed:
        botocore_exceptions = load_optional_dependency(
            "botocore.exceptions", extra="aws", feature="the Lambda transport"
        )

        payload = {
            "action": request.action,
            "globals": request.globals,
            "input": request.input,
        }
        context = request.service_call_context
        if context.scope_digest is not None:
            payload[LAMBDA_SERVICE_CALL_CONTEXT_FIELD] = context.model_dump(
                mode="json",
                exclude_none=True,
            )

        try:
            async with asyncio.timeout(self._dispatch_timeout_sec):
                response = await asyncio.to_thread(
                    self._lambda.invoke,
                    FunctionName=self._config.function_name,
                    InvocationType="RequestResponse",
                    Payload=json.dumps(payload).encode(),
                )
        except TimeoutError as exc:
            raise DispatchTimeoutError("Lambda dispatch deadline exceeded") from exc
        except botocore_exceptions.ConnectTimeoutError as e:
            raise ConnectionTimeoutError("Lambda connection deadline exceeded") from e
        except botocore_exceptions.ReadTimeoutError as e:
            raise DispatchTimeoutError("Lambda dispatch deadline exceeded") from e
        except botocore_exceptions.ClientError as e:
            error_code = str(e.response.get("Error", {}).get("Code", "LAMBDA_CLIENT_ERROR"))
            if error_code in NON_RETRYABLE_LAMBDA_ERROR_CODES:
                raise ServiceError(str(e), code=error_code, retryable=False) from e
            raise TransportConnectionError(
                f"Lambda invoke failed for '{self._config.function_name}': {e}"
            ) from e
        except botocore_exceptions.BotoCoreError as e:
            raise TransportConnectionError(
                f"Lambda invoke failed for '{self._config.function_name}': {e}"
            ) from e

        try:
            response_payload = json.loads(response["Payload"].read())
        except (AttributeError, KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as e:
            raise MalformedResponseError(
                f"Non-JSON payload from Lambda '{self._config.function_name}': {e}"
            ) from e

        if response.get("FunctionError"):
            if not isinstance(response_payload, dict):
                raise MalformedResponseError(
                    f"Invalid error payload from Lambda '{self._config.function_name}'"
                )
            error_message = response_payload.get("errorMessage", "Unknown Lambda error")
            if not isinstance(error_message, str):
                raise MalformedResponseError(
                    f"Invalid error payload from Lambda '{self._config.function_name}'"
                )
            raise ServiceError(
                error_message,
                code="LAMBDA_ERROR",
            )

        return Completed(data=response_payload)

    async def close(self) -> None:
        return None
