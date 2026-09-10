"""Temporal activities - the transport layer that executes step calls."""

from __future__ import annotations

import importlib
import inspect
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.engine.contracts import (
    ContractViolation,
    SchemaDeclarationError,
    SchemaLoadError,
    SchemaSpec,
    validate_payload,
)
from justflow.engine.limits import enforce_payload_bytes, enforce_utf8_bytes
from justflow.engine.runner import CacheDirective
from justflow.engine.serialization import (
    StrictJsonError,
    StrictJsonLayout,
    dumps_strict_json,
    loads_strict_json,
)
from justflow.resources.base import (
    CacheStore,
    ResourceAccessError,
    ResourceCapability,
    ResourceCapabilityError,
    ResourceNotFoundError,
)
from justflow.resources.registry import ResourceCollection
from justflow.runtime.metrics import ActivityMetricOutcome, MetricsRegistry
from justflow.sdk.logging_context import identity_log_digest, logging_context
from justflow.transports.base import (
    AwaitingResponse,
    Completed,
    TransportError,
    TransportRequest,
)
from justflow.transports.registry import ConfiguredService

logger = logging.getLogger(__name__)


CACHE_HIT = "hit"
CACHE_MISS = "miss"
CONTRACT_VIOLATION_ERROR = "CONTRACT_VIOLATION"
CONTRACT_DECLARATION_ERROR = "CONTRACT_DECLARATION_ERROR"
CACHE_INTEGRITY_ERROR = "CACHE_INTEGRITY_ERROR"
CACHE_SERIALIZATION_ERROR = "CACHE_SERIALIZATION_ERROR"
CACHE_NAMESPACE_ERROR = "CACHE_NAMESPACE_ERROR"
TRANSPORT_PROVIDER_ERROR = "TRANSPORT_PROVIDER_ERROR"
ASYNC_TRANSPORT_UNSUPPORTED_ERROR = "ASYNC_TRANSPORT_UNSUPPORTED"
INVALID_TRANSPORT_DISPATCH_ERROR = "INVALID_TRANSPORT_DISPATCH"
EVALUATOR_EXECUTION_ERROR = "EVALUATOR_EXECUTION_ERROR"


@dataclass(frozen=True, kw_only=True)
class TransportCleanupFailure:
    service_name: str
    provider_name: str
    cause: Exception


class TransportCleanupError(Exception):
    def __init__(self, failures: tuple[TransportCleanupFailure, ...]) -> None:
        self.failures = failures
        details = "; ".join(
            f"{failure.service_name} ({failure.provider_name}): {failure.cause}"
            for failure in failures
        )
        super().__init__(f"Transport cleanup failed: {details}")


def _cache_storage_key(cache: CacheDirective, scope_digest: str | None) -> str:
    namespace_values = (cache.definition_digest, cache.contract_identity)
    if namespace_values == (None, None):
        return cache.key if scope_digest is None else f"{scope_digest}:{cache.key}"
    if any(value is None for value in namespace_values):
        raise ApplicationError(
            "Cache namespace requires both definition and contract identity",
            type=CACHE_NAMESPACE_ERROR,
            non_retryable=True,
        )
    prefix = (
        f"{cache.definition_digest}:{cache.contract_identity}"
        if scope_digest is None
        else f"{scope_digest}:{cache.definition_digest}:{cache.contract_identity}"
    )
    return f"{prefix}:{cache.key}"


@dataclass
class ActivityInput:
    service_name: str
    action: str
    input: Any
    globals: dict[str, Any]
    request_id: str
    correlation_id: str
    trace_id: str | None
    workflow_id: str
    workflow_run_id: str
    flow_name: str
    definition_digest: str | None
    step_name: str
    scope_digest: str | None = None
    cache: CacheDirective | None = None
    # Contract schemas (inline JSON Schema or pydantic dotted path) or None.
    input_schema: dict[str, Any] | str | None = None
    output_schema: dict[str, Any] | str | None = None
    required_resources: list[str] = field(default_factory=list)


@dataclass
class ContractValidationInput:
    schema: SchemaSpec
    payload: Any
    direction: str
    boundary_name: str


@dataclass
class StepActivityResult:
    """What execute_step hands back to the workflow."""

    data: Any = None
    awaiting_signal: bool = False
    signal_key: str | None = None
    response_timeout_sec: int | None = None
    cache: str | None = None  # "hit" | "miss" | None


@dataclass
class EvaluatorInput:
    """Input for the evaluate_condition activity (the `evaluator:` condition form)."""

    evaluator: str
    data: Any
    request_id: str
    flow_name: str
    step_name: str
    resource_names: list[str] = field(default_factory=list)


class WorkflowActivities:
    """Temporal activity class that dispatches step calls via the transport layer."""

    def __init__(
        self,
        services: dict[str, ConfiguredService],
        resources: ResourceCollection | Mapping[str, object] | None = None,
        limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
        metrics: MetricsRegistry | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self._services = services
        self._resources = (
            resources
            if isinstance(resources, ResourceCollection)
            else ResourceCollection.from_instances(resources or {})
        )
        self._evaluator_cache: dict[str, Callable[..., Any]] = {}
        self._limits = limits
        self._metrics = metrics
        self._monotonic = monotonic
        self._closed = False

    @activity.defn(name="execute_step")
    async def execute_step(self, activity_input: ActivityInput) -> StepActivityResult:
        """Execute a single step via the appropriate transport."""
        started_at = self._monotonic()
        with logging_context(
            request_id=identity_log_digest(activity_input.correlation_id),
            flow_name=activity_input.flow_name,
            step_name=activity_input.step_name,
        ):
            try:
                result = await self._execute_step(activity_input)
            except Exception:
                if self._metrics is not None:
                    self._metrics.record_activity(
                        ActivityMetricOutcome.FAILED,
                        self._monotonic() - started_at,
                    )
                raise
            if self._metrics is not None:
                self._metrics.record_activity(
                    ActivityMetricOutcome.COMPLETED,
                    self._monotonic() - started_at,
                )
            return result

    async def _execute_step(self, activity_input: ActivityInput) -> StepActivityResult:
        enforce_payload_bytes(
            asdict(activity_input),
            boundary=f"activities.{activity_input.step_name}.input",
            limit=self._limits.activity_input_bytes,
        )
        if self._closed:
            raise RuntimeError("WorkflowActivities is closed")
        service_name = activity_input.service_name
        if service_name not in self._services:
            raise ValueError(f"Service '{service_name}' not found in config")

        service = self._services[service_name]
        transport = service.transport

        if activity_input.input_schema is not None:
            self._check_contract(
                activity_input.input_schema,
                activity_input.input,
                direction="input",
                step_name=activity_input.step_name,
            )

        cache = activity_input.cache
        cache_resource = self._get_cache_resource(cache)
        if cache_resource is not None and cache is not None:
            cache_key = _cache_storage_key(
                cache,
                activity_input.scope_digest,
            )
            cached = await cache_resource.get(cache_key)
            if cached is not None:
                if not isinstance(cached, str):
                    raise ApplicationError(
                        f"Cache entry for step '{activity_input.step_name}' is not text",
                        type=CACHE_INTEGRITY_ERROR,
                        non_retryable=True,
                    )
                enforce_utf8_bytes(
                    cached,
                    boundary=f"cache.{activity_input.step_name}.entry",
                    limit=self._limits.cache_entry_bytes,
                )
                logger.info(
                    "Step cache hit",
                    extra={"cache_resource": cache.resource},
                )
                data = self._load_cached_result(
                    cached,
                    schema=activity_input.output_schema,
                    step_name=activity_input.step_name,
                )
                enforce_payload_bytes(
                    data,
                    boundary=f"activities.{activity_input.step_name}.output",
                    limit=self._limits.activity_output_bytes,
                )
                return StepActivityResult(data=data, cache=CACHE_HIT)

        request = TransportRequest(
            service_name=service_name,
            action=activity_input.action,
            input=activity_input.input,
            globals=activity_input.globals,
            request_id=activity_input.request_id,
            correlation_id=activity_input.correlation_id,
            trace_id=activity_input.trace_id,
            workflow_id=activity_input.workflow_id,
            workflow_run_id=activity_input.workflow_run_id,
            flow_name=activity_input.flow_name,
            definition_digest=activity_input.definition_digest,
            step_name=activity_input.step_name,
            scope_digest=activity_input.scope_digest,
            required_resources=tuple(activity_input.required_resources),
        )

        logger.info(
            "Executing step",
            extra={
                "service": service_name,
                "provider": service.definition.provider_name,
                "action": activity_input.action,
            },
        )

        try:
            dispatch = await transport.send(request)
        except TransportError as e:
            raise ApplicationError(
                f"Transport call failed for service '{service_name}' "
                f"and action '{activity_input.action}'",
                type=e.code,
                non_retryable=not e.retryable,
            ) from None
        except Exception as exc:  # noqa: BLE001 - transport provider boundary
            logger.error(
                "Transport provider raised an unclassified exception",
                extra={
                    "service": service_name,
                    "provider": service.definition.provider_name,
                    "action": activity_input.action,
                    "exception_type": type(exc).__name__,
                },
            )
            raise ApplicationError(
                f"Transport provider '{service.definition.provider_name}' failed "
                f"for service '{service_name}'",
                type=TRANSPORT_PROVIDER_ERROR,
                non_retryable=True,
            ) from None

        if isinstance(dispatch, AwaitingResponse):
            if not service.supports_async_response:
                raise ApplicationError(
                    f"Transport provider '{service.definition.provider_name}' returned "
                    "an asynchronous response, but host providers must complete inline",
                    type=ASYNC_TRANSPORT_UNSUPPORTED_ERROR,
                    non_retryable=True,
                )
            result = StepActivityResult(
                awaiting_signal=True,
                signal_key=dispatch.key,
                response_timeout_sec=dispatch.timeout_policy.response_timeout_sec,
            )
            enforce_payload_bytes(
                asdict(result),
                boundary=f"activities.{activity_input.step_name}.output",
                limit=self._limits.activity_output_bytes,
            )
            return result
        if not isinstance(dispatch, Completed):
            raise ApplicationError(
                f"Transport provider '{service.definition.provider_name}' returned "
                f"unsupported dispatch type '{type(dispatch).__name__}'",
                type=INVALID_TRANSPORT_DISPATCH_ERROR,
                non_retryable=True,
            )

        data = dispatch.data
        if activity_input.output_schema is not None:
            self._check_contract(
                activity_input.output_schema,
                data,
                direction="output",
                step_name=activity_input.step_name,
            )
        serialized_cache: str | None = None
        if cache_resource is not None and cache is not None:
            serialized_cache = self._serialize_cached_result(
                data, step_name=activity_input.step_name
            )
            enforce_utf8_bytes(
                serialized_cache,
                boundary=f"cache.{activity_input.step_name}.entry",
                limit=self._limits.cache_entry_bytes,
            )
        enforce_payload_bytes(
            data,
            boundary=f"activities.{activity_input.step_name}.output",
            limit=self._limits.activity_output_bytes,
        )
        if serialized_cache is not None and cache is not None and cache_resource is not None:
            await cache_resource.set(
                _cache_storage_key(
                    cache,
                    activity_input.scope_digest,
                ),
                serialized_cache,
                cache.ttl_sec,
            )
        return StepActivityResult(
            data=data,
            cache=CACHE_MISS if cache_resource is not None else None,
        )

    @staticmethod
    def _check_contract(
        schema: dict[str, Any] | str, payload: Any, *, direction: str, step_name: str
    ) -> None:
        try:
            validate_payload(schema, payload, direction=direction, step_name=step_name)
        except ContractViolation:
            raise ApplicationError(
                f"{direction} for '{step_name}' violates its schema",
                type=CONTRACT_VIOLATION_ERROR,
                non_retryable=True,
            ) from None
        except (SchemaDeclarationError, SchemaLoadError):
            raise ApplicationError(
                f"Contract declaration for '{step_name}' is invalid",
                type=CONTRACT_DECLARATION_ERROR,
                non_retryable=True,
            ) from None

    @activity.defn(name="validate_contract")
    async def validate_contract(self, validation_input: ContractValidationInput) -> None:
        enforce_payload_bytes(
            asdict(validation_input),
            boundary=f"activities.{validation_input.boundary_name}.contract_input",
            limit=self._limits.activity_input_bytes,
        )
        self._check_contract(
            validation_input.schema,
            validation_input.payload,
            direction=validation_input.direction,
            step_name=validation_input.boundary_name,
        )

    @staticmethod
    def _load_cached_result(cached: Any, *, schema: SchemaSpec | None, step_name: str) -> Any:
        try:
            if not isinstance(cached, (str, bytes, bytearray)):
                raise StrictJsonError("cache entry must be text or bytes")
            data = loads_strict_json(cached)
            if schema is not None:
                validate_payload(
                    schema,
                    data,
                    direction="cached output",
                    step_name=step_name,
                )
            return data
        except (SchemaDeclarationError, SchemaLoadError):
            raise ApplicationError(
                f"Cached output contract for step '{step_name}' is invalid",
                type=CONTRACT_DECLARATION_ERROR,
                non_retryable=True,
            ) from None
        except (
            ContractViolation,
            StrictJsonError,
        ):
            raise ApplicationError(
                f"Cache entry for step '{step_name}' failed integrity validation",
                type=CACHE_INTEGRITY_ERROR,
                non_retryable=True,
            ) from None

    @staticmethod
    def _serialize_cached_result(data: Any, *, step_name: str) -> str:
        try:
            return dumps_strict_json(data, layout=StrictJsonLayout.CACHE_V1)
        except StrictJsonError:
            raise ApplicationError(
                f"Output for step '{step_name}' cannot be cached as strict JSON",
                type=CACHE_SERIALIZATION_ERROR,
                non_retryable=True,
            ) from None

    def _get_cache_resource(self, cache: CacheDirective | None) -> CacheStore | None:
        if cache is None:
            return None
        try:
            resource = self._resources.require(cache.resource, ResourceCapability.CACHE)
        except ResourceNotFoundError as exc:
            raise ApplicationError(
                f"Cache resource '{cache.resource}' is not loaded",
                type="CACHE_RESOURCE_MISSING",
                non_retryable=True,
            ) from exc
        except ResourceCapabilityError as exc:
            raise ApplicationError(
                f"Cache resource '{cache.resource}' lacks cache capability",
                type="CACHE_RESOURCE_CAPABILITY_MISMATCH",
                non_retryable=True,
            ) from exc
        if not isinstance(resource, CacheStore):
            raise TypeError("Validated cache resource does not implement CacheStore")
        return resource

    @activity.defn(name="evaluate_condition")
    async def evaluate_condition(self, evaluator_input: EvaluatorInput) -> bool:
        """Run an evaluator function with its declared resources injected.

        Runs as an activity (not workflow code) so evaluators may do I/O —
        external lookups don't break Temporal's replay determinism.
        """
        enforce_payload_bytes(
            asdict(evaluator_input),
            boundary=f"activities.{evaluator_input.step_name}.evaluator_input",
            limit=self._limits.activity_input_bytes,
        )
        with logging_context(
            request_id=identity_log_digest(evaluator_input.request_id),
            flow_name=evaluator_input.flow_name,
            step_name=evaluator_input.step_name,
        ):
            func = self._import_evaluator(evaluator_input.evaluator)

            try:
                resources = self._resources.grant(evaluator_input.resource_names)
            except ResourceNotFoundError as exc:
                raise ApplicationError(
                    f"Evaluator for step '{evaluator_input.step_name}' requested an unavailable "
                    "resource grant",
                    type="EVALUATOR_RESOURCE_MISSING",
                    non_retryable=True,
                ) from exc

            try:
                result = func(evaluator_input.data, resources=resources)
                if inspect.iscoroutine(result):
                    result = await result
                return bool(result)
            except ResourceAccessError as exc:
                raise ApplicationError(
                    f"Evaluator for step '{evaluator_input.step_name}' attempted denied "
                    "resource access",
                    type="EVALUATOR_RESOURCE_ACCESS_DENIED",
                    non_retryable=True,
                ) from exc
            except Exception as exc:  # noqa: BLE001 - application evaluator boundary
                logger.error(
                    "Evaluator failed",
                    extra={
                        "evaluator": evaluator_input.evaluator,
                        "exception_type": type(exc).__name__,
                    },
                )
                raise ApplicationError(
                    f"Evaluator failed for step '{evaluator_input.step_name}'",
                    type=EVALUATOR_EXECUTION_ERROR,
                    non_retryable=False,
                ) from None

    def _import_evaluator(self, dotted_path: str) -> Callable[..., Any]:
        if dotted_path not in self._evaluator_cache:
            module_path, func_name = dotted_path.rsplit(".", 1)
            try:
                module = importlib.import_module(module_path)
                func = getattr(module, func_name)
            except (ImportError, AttributeError):
                raise ApplicationError(
                    f"Cannot import evaluator '{dotted_path}'",
                    type="EVALUATOR_IMPORT_ERROR",
                    non_retryable=True,
                ) from None
            self._evaluator_cache[dotted_path] = func
        return self._evaluator_cache[dotted_path]

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        failures: list[TransportCleanupFailure] = []
        for service_name, service in reversed(self._services.items()):
            try:
                await service.transport.close()
            except Exception as exc:  # noqa: BLE001 - provider cleanup boundary
                logger.error(
                    "Transport cleanup failed",
                    extra={
                        "service": service_name,
                        "provider": service.definition.provider_name,
                        "exception_type": type(exc).__name__,
                    },
                )
                failures.append(
                    TransportCleanupFailure(
                        service_name=service_name,
                        provider_name=service.definition.provider_name,
                        cause=exc,
                    )
                )
        if failures:
            raise TransportCleanupError(tuple(failures))
