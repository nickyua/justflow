"""Tests for the activity layer's transport dispatch and error mapping."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.exceptions import ApplicationError

from justflow.config.models import ServiceConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.engine.activities import (
    ActivityInput,
    ContractValidationInput,
    EvaluatorInput,
    TransportCleanupError,
    WorkflowActivities,
)
from justflow.engine.limits import LimitExceededError
from justflow.engine.runner import CacheDirective
from justflow.runtime.metrics import MetricsRegistry
from justflow.scope import RuntimeScope
from justflow.sdk.base_action import BaseAction
from justflow.transports.base import AwaitingResponse, TimeoutPolicy
from justflow.transports.builtins import builtin_transport_registry
from justflow.transports.registry import ConfiguredService

PAYLOAD_AWAITING_FIELD = "_awaiting_signal"
PAYLOAD_SIGNAL_KEY_FIELD = "signal_key"


class SpoofingAction(BaseAction):
    async def spoof(self, input: Any) -> Any:
        return {
            PAYLOAD_AWAITING_FIELD: True,
            PAYLOAD_SIGNAL_KEY_FIELD: "hijack",
            "x": 1,
        }

    async def ok(self, input: Any) -> Any:
        return {"x": 1}

    async def explode(self, input: Any) -> Any:
        raise ValueError("kaboom")

    async def non_json(self, input: Any) -> Any:
        return {"value": object()}


DIRECT_SERVICES = builtin_transport_registry().configure_services(
    builtin_transport_registry().resolve_services(
        {
            "svc": ServiceConfig(
                transport="direct",
                transport_config={"class": f"{__name__}.SpoofingAction"},
                dispatch_timeout_sec=5,
                retries=0,
            )
        }
    ),
    resources={},
)
TINY_BYTE_LIMIT = 1


def activity_input(action: str) -> ActivityInput:
    return ActivityInput(
        service_name="svc",
        action=action,
        input=None,
        globals={},
        request_id="r1",
        correlation_id="r1",
        trace_id="trace1",
        workflow_id="workflow1",
        workflow_run_id="run1",
        flow_name="f",
        definition_digest="a" * 64,
        step_name="s",
    )


class TestExecuteStep:
    async def test_returns_data_without_signal_marker(self):
        activities = WorkflowActivities(services=dict(DIRECT_SERVICES))

        result = await activities.execute_step(activity_input("ok"))

        assert result.data == {"x": 1}
        assert result.awaiting_signal is False
        assert result.signal_key is None

    async def test_activity_metrics_record_success_and_failure_latency(self) -> None:
        success_metrics = MetricsRegistry()
        success_clock = iter((10.0, 10.25))
        success = WorkflowActivities(
            services=dict(DIRECT_SERVICES),
            metrics=success_metrics,
            monotonic=lambda: next(success_clock),
        )
        failure_metrics = MetricsRegistry()
        failure_clock = iter((20.0, 20.5))
        failure = WorkflowActivities(
            services={},
            metrics=failure_metrics,
            monotonic=lambda: next(failure_clock),
        )

        await success.execute_step(activity_input("ok"))
        with pytest.raises(ValueError, match="not found in config"):
            await failure.execute_step(activity_input("ok"))

        assert 'outcome="completed"} 1' in success_metrics.render_prometheus().decode()
        assert "justflow_activity_latency_seconds_sum 0.25" in (
            success_metrics.render_prometheus().decode()
        )
        assert 'outcome="failed"} 1' in failure_metrics.render_prometheus().decode()
        assert "justflow_activity_latency_seconds_sum 0.5" in (
            failure_metrics.render_prometheus().decode()
        )

    async def test_marker_shaped_payload_is_plain_completed_data(self):
        activities = WorkflowActivities(services=dict(DIRECT_SERVICES))

        result = await activities.execute_step(activity_input("spoof"))

        assert result.awaiting_signal is False
        assert result.signal_key is None

    async def test_unknown_service_raises(self):
        activities = WorkflowActivities(services={})

        with pytest.raises(ValueError, match="not found in config"):
            await activities.execute_step(activity_input("ok"))

    async def test_transport_error_maps_to_application_error(self, caplog):
        activities = WorkflowActivities(services=dict(DIRECT_SERVICES))

        with pytest.raises(ApplicationError) as exc_info:
            await activities.execute_step(activity_input("explode"))

        assert exc_info.value.type == "ValueError"
        assert exc_info.value.non_retryable is False
        assert "kaboom" not in str(exc_info.value)
        assert "kaboom" not in caplog.text

    async def test_dispatch_error_is_non_retryable(self):
        activities = WorkflowActivities(services=dict(DIRECT_SERVICES))

        with pytest.raises(ApplicationError) as exc_info:
            await activities.execute_step(activity_input("no_such_action"))

        assert exc_info.value.type == "ACTION_DISPATCH_ERROR"
        assert exc_info.value.non_retryable is True

    async def test_oversized_input_is_rejected_before_dispatch(self):
        activities = WorkflowActivities(
            services=dict(DIRECT_SERVICES),
            limits=RuntimeLimits(activity_input_bytes=TINY_BYTE_LIMIT),
        )

        with pytest.raises(LimitExceededError, match="activities.s.input"):
            await activities.execute_step(activity_input("explode"))

    async def test_oversized_output_raises_typed_limit(self):
        activities = WorkflowActivities(
            services=dict(DIRECT_SERVICES),
            limits=RuntimeLimits(activity_output_bytes=TINY_BYTE_LIMIT),
        )

        with pytest.raises(LimitExceededError, match="activities.s.output"):
            await activities.execute_step(activity_input("ok"))

    async def test_unclassified_provider_exception_is_contained(self, caplog):
        transport = MagicMock()
        transport.send = AsyncMock(side_effect=KeyError("private provider detail"))
        transport.close = AsyncMock()
        configured = replace(DIRECT_SERVICES["svc"], transport=transport)
        activities = WorkflowActivities(services={"svc": configured})

        with pytest.raises(ApplicationError) as exc_info:
            await activities.execute_step(activity_input("ok"))

        assert exc_info.value.type == "TRANSPORT_PROVIDER_ERROR"
        assert exc_info.value.non_retryable is True
        assert "private provider detail" not in str(exc_info.value)
        assert "private provider detail" not in caplog.text

    async def test_custom_provider_async_dispatch_is_rejected(self):
        transport = MagicMock()
        transport.send = AsyncMock(
            return_value=AwaitingResponse(
                key="response-key",
                timeout_policy=TimeoutPolicy(response_timeout_sec=5),
            )
        )
        transport.close = AsyncMock()
        configured = replace(
            DIRECT_SERVICES["svc"],
            transport=transport,
            supports_async_response=False,
        )
        activities = WorkflowActivities(services={"svc": configured})

        with pytest.raises(ApplicationError) as exc_info:
            await activities.execute_step(activity_input("ok"))

        assert exc_info.value.type == "ASYNC_TRANSPORT_UNSUPPORTED"
        assert exc_info.value.non_retryable is True

    def test_cache_serializer_preserves_version_one_bytes(self):
        serialized = WorkflowActivities._serialize_cached_result(
            {"b": [2, 3], "a": 1},
            step_name="cache_v1",
        )

        assert serialized == '{"a": 1, "b": [2, 3]}'


class TestActivityLifecycle:
    async def test_close_is_reverse_order_continues_and_aggregates(self):
        events: list[str] = []

        def transport(label: str, *, fail: bool = False) -> MagicMock:
            instance = MagicMock()

            async def close() -> None:
                events.append(label)
                if fail:
                    raise RuntimeError(f"close failed: {label}")

            instance.close = AsyncMock(side_effect=close)
            return instance

        http = transport("http")
        grpc = transport("grpc", fail=True)
        queue = transport("queue")
        registry = builtin_transport_registry()
        definitions = registry.resolve_services(
            {
                "http_service": ServiceConfig(
                    transport="http",
                    transport_config={"base_url": "https://service"},
                    connect_timeout_sec=2,
                    dispatch_timeout_sec=5,
                    retries=0,
                ),
                "grpc_service": ServiceConfig(
                    transport="grpc",
                    transport_config={
                        "address": "service:50051",
                        "security": {"mode": "tls", "profile": "service"},
                    },
                    connect_timeout_sec=2,
                    dispatch_timeout_sec=5,
                    retries=0,
                ),
                "queue_service": ServiceConfig(
                    transport="queue",
                    transport_config={
                        "broker": "main",
                        "destination": "requests",
                        "idempotency": "durable",
                    },
                    dispatch_timeout_sec=2,
                    response_timeout_sec=5,
                    retries=0,
                ),
            }
        )
        activities = WorkflowActivities(
            services={
                "http_service": ConfiguredService(
                    definition=definitions["http_service"],
                    transport=http,
                    supports_async_response=False,
                ),
                "grpc_service": ConfiguredService(
                    definition=definitions["grpc_service"],
                    transport=grpc,
                    supports_async_response=False,
                ),
                "queue_service": ConfiguredService(
                    definition=definitions["queue_service"],
                    transport=queue,
                    supports_async_response=True,
                ),
            }
        )

        with pytest.raises(TransportCleanupError) as exc_info:
            await activities.close()

        assert events == ["queue", "grpc", "http"]
        assert [failure.service_name for failure in exc_info.value.failures] == ["grpc_service"]

        await activities.close()
        assert events == ["queue", "grpc", "http"]


INPUT_SCHEMA = {"type": "object", "required": ["record_id"]}
OUTPUT_SCHEMA = {"type": "object", "required": ["x"]}


class TestStepContracts:
    def _input(self, action: str, payload, input_schema=None, output_schema=None):
        inp = activity_input(action)
        inp.input = payload
        inp.input_schema = input_schema
        inp.output_schema = output_schema
        return inp

    async def test_valid_contracts_pass(self):
        activities = WorkflowActivities(services=dict(DIRECT_SERVICES))

        result = await activities.execute_step(
            self._input(
                "ok", {"record_id": "R1"}, input_schema=INPUT_SCHEMA, output_schema=OUTPUT_SCHEMA
            )
        )

        assert result.data == {"x": 1}

    async def test_input_violation_blocks_the_call(self):
        activities = WorkflowActivities(services=dict(DIRECT_SERVICES))

        with pytest.raises(ApplicationError) as exc_info:
            # 'explode' would raise ValueError if the transport were reached
            await activities.execute_step(self._input("explode", {}, input_schema=INPUT_SCHEMA))

        assert exc_info.value.type == "CONTRACT_VIOLATION"
        assert exc_info.value.non_retryable is True

    async def test_generic_contract_activity_uses_declaration_error_taxonomy(self):
        activities = WorkflowActivities(services={})

        with pytest.raises(ApplicationError) as exc_info:
            await activities.validate_contract(
                ContractValidationInput(
                    schema={"type": "not-a-json-schema-type"},
                    payload={},
                    direction="workflow input",
                    boundary_name="flow",
                )
            )

        assert exc_info.value.type == "CONTRACT_DECLARATION_ERROR"
        assert exc_info.value.non_retryable is True

    async def test_output_violation_fails_the_step(self):
        activities = WorkflowActivities(services=dict(DIRECT_SERVICES))

        with pytest.raises(ApplicationError) as exc_info:
            await activities.execute_step(
                self._input("ok", None, output_schema={"type": "object", "required": ["missing"]})
            )

        assert exc_info.value.type == "CONTRACT_VIOLATION"


class TestStepCaching:
    def _activities(self, cache_resource) -> WorkflowActivities:
        return WorkflowActivities(
            services=dict(DIRECT_SERVICES), resources={"cache": cache_resource}
        )

    def _cached_input(self, action: str) -> ActivityInput:
        inp = activity_input(action)
        inp.cache = CacheDirective(resource="cache", key="k1", ttl_sec=60)
        return inp

    async def test_miss_calls_service_and_stores(self):
        from justflow.resources.memory import MemoryCache

        cache = MemoryCache()
        activities = self._activities(cache)

        result = await activities.execute_step(self._cached_input("ok"))

        assert result.data == {"x": 1}
        assert result.cache == "miss"
        assert await cache.get("k1") == '{"x": 1}'

    async def test_hit_skips_the_service(self):
        from justflow.resources.memory import MemoryCache

        cache = MemoryCache()
        await cache.set("k1", '{"cached": true}')
        activities = self._activities(cache)

        # 'explode' would raise if the transport were reached
        result = await activities.execute_step(self._cached_input("explode"))

        assert result.data == {"cached": True}
        assert result.cache == "hit"

    async def test_versioned_cache_uses_definition_and_contract_namespace(self):
        from justflow.resources.memory import MemoryCache

        cache = MemoryCache()
        activities = self._activities(cache)
        request = self._cached_input("ok")
        request.cache = CacheDirective(
            resource="cache",
            key="k1",
            ttl_sec=60,
            definition_digest="a" * 64,
            contract_identity="sha256:contract",
        )

        await activities.execute_step(request)

        assert await cache.get("k1") is None
        assert await cache.get(f"{'a' * 64}:sha256:contract:k1") == '{"x": 1}'

    async def test_shared_cache_isolates_runtime_scopes(self):
        from justflow.resources.memory import MemoryCache

        cache = MemoryCache()
        activities = self._activities(cache)
        scope_a = RuntimeScope.create(
            tenant="tenant-a",
            application="orders",
            environment="production",
        )
        scope_b = RuntimeScope.create(
            tenant="tenant-b",
            application="orders",
            environment="production",
        )
        request_a = self._cached_input("ok")
        request_a.scope_digest = scope_a.digest
        request_a.cache = CacheDirective(
            resource="cache",
            key="k1",
            ttl_sec=60,
            definition_digest="a" * 64,
            contract_identity="sha256:contract",
        )
        request_b = replace(request_a, scope_digest=scope_b.digest)

        result_a = await activities.execute_step(request_a)
        result_b = await activities.execute_step(request_b)

        assert result_a.cache == "miss"
        assert result_b.cache == "miss"
        assert await cache.get(f"{scope_a.digest}:{'a' * 64}:sha256:contract:k1") == '{"x": 1}'
        assert await cache.get(f"{scope_b.digest}:{'a' * 64}:sha256:contract:k1") == '{"x": 1}'

    async def test_partial_cache_namespace_is_rejected(self):
        from justflow.resources.memory import MemoryCache

        request = self._cached_input("ok")
        request.cache = CacheDirective(
            resource="cache",
            key="k1",
            definition_digest="a" * 64,
        )

        with pytest.raises(ApplicationError) as exc_info:
            await self._activities(MemoryCache()).execute_step(request)

        assert exc_info.value.type == "CACHE_NAMESPACE_ERROR"
        assert exc_info.value.non_retryable is True

    async def test_errors_are_not_cached(self):
        from justflow.resources.memory import MemoryCache

        cache = MemoryCache()
        activities = self._activities(cache)

        with pytest.raises(ApplicationError):
            await activities.execute_step(self._cached_input("explode"))

        assert await cache.get("k1") is None

    async def test_missing_cache_resource_is_non_retryable(self):
        activities = WorkflowActivities(services=dict(DIRECT_SERVICES), resources={})

        with pytest.raises(ApplicationError) as exc_info:
            await activities.execute_step(self._cached_input("ok"))

        assert exc_info.value.type == "CACHE_RESOURCE_MISSING"
        assert exc_info.value.non_retryable is True

    async def test_cache_capability_mismatch_is_non_retryable(self):
        activities = WorkflowActivities(
            services=dict(DIRECT_SERVICES),
            resources={"cache": object()},
        )

        with pytest.raises(ApplicationError) as exc_info:
            await activities.execute_step(self._cached_input("ok"))

        assert exc_info.value.type == "CACHE_RESOURCE_CAPABILITY_MISMATCH"
        assert exc_info.value.non_retryable is True

    async def test_live_output_is_validated_before_cache_write(self):
        from justflow.resources.memory import MemoryCache

        cache = MemoryCache()
        activities = self._activities(cache)
        activity_request = self._cached_input("ok")
        activity_request.output_schema = {
            "type": "object",
            "required": ["missing"],
        }

        with pytest.raises(ApplicationError) as exc_info:
            await activities.execute_step(activity_request)

        assert exc_info.value.type == "CONTRACT_VIOLATION"
        assert await cache.get("k1") is None

    async def test_non_json_output_is_not_coerced_into_cache(self):
        from justflow.resources.memory import MemoryCache

        cache = MemoryCache()
        activities = self._activities(cache)

        with pytest.raises(ApplicationError) as exc_info:
            await activities.execute_step(self._cached_input("non_json"))

        assert exc_info.value.type == "CACHE_SERIALIZATION_ERROR"
        assert exc_info.value.non_retryable is True
        assert await cache.get("k1") is None

    async def test_invalid_cache_schema_keeps_declaration_error_taxonomy(self):
        from justflow.resources.memory import MemoryCache

        cache = MemoryCache()
        await cache.set("k1", '{"x": 1}')
        activities = self._activities(cache)
        activity_request = self._cached_input("explode")
        activity_request.output_schema = {"type": "not-a-json-schema-type"}

        with pytest.raises(ApplicationError) as exc_info:
            await activities.execute_step(activity_request)

        assert exc_info.value.type == "CONTRACT_DECLARATION_ERROR"
        assert exc_info.value.non_retryable is True

    async def test_oversized_live_output_is_not_cached(self):
        from justflow.resources.memory import MemoryCache

        cache = MemoryCache()
        activities = WorkflowActivities(
            services=dict(DIRECT_SERVICES),
            resources={"cache": cache},
            limits=RuntimeLimits(cache_entry_bytes=TINY_BYTE_LIMIT),
        )

        with pytest.raises(LimitExceededError, match="cache.s.entry"):
            await activities.execute_step(self._cached_input("ok"))

        assert await cache.get("k1") is None

    async def test_oversized_cached_value_is_rejected_before_use(self):
        from justflow.resources.memory import MemoryCache

        cache = MemoryCache()
        await cache.set("k1", '{"cached":true}')
        activities = WorkflowActivities(
            services=dict(DIRECT_SERVICES),
            resources={"cache": cache},
            limits=RuntimeLimits(cache_entry_bytes=TINY_BYTE_LIMIT),
        )

        with pytest.raises(LimitExceededError, match="cache.s.entry"):
            await activities.execute_step(self._cached_input("explode"))


@dataclass(frozen=True, kw_only=True)
class CacheReturns:
    value: Any


@dataclass(frozen=True, kw_only=True)
class CacheRaises:
    exc: type[BaseException]
    match: str


CacheOutcome = CacheReturns | CacheRaises


@dataclass(frozen=True, kw_only=True)
class CacheHitCase:
    id: str
    cached: str
    schema: dict[str, Any] | None
    outcome: CacheOutcome


CACHE_HIT_CASES = [
    CacheHitCase(
        id="valid-schema-output",
        cached='{"x": 1}',
        schema={"type": "object", "required": ["x"]},
        outcome=CacheReturns(value={"x": 1}),
    ),
    CacheHitCase(
        id="malformed-json",
        cached="{",
        schema=None,
        outcome=CacheRaises(exc=ApplicationError, match="integrity validation"),
    ),
    CacheHitCase(
        id="duplicate-key",
        cached='{"x": 1, "x": 2}',
        schema=None,
        outcome=CacheRaises(exc=ApplicationError, match="integrity validation"),
    ),
    CacheHitCase(
        id="non-finite-number",
        cached='{"x": NaN}',
        schema=None,
        outcome=CacheRaises(exc=ApplicationError, match="integrity validation"),
    ),
    CacheHitCase(
        id="schema-invalid-output",
        cached='{"other": 1}',
        schema={"type": "object", "required": ["x"]},
        outcome=CacheRaises(exc=ApplicationError, match="integrity validation"),
    ),
]


class TestCacheHitIntegrity:
    @pytest.mark.parametrize(
        "case",
        CACHE_HIT_CASES,
        ids=lambda case: case.id,
    )
    async def test_cached_result(self, case: CacheHitCase):
        from justflow.resources.memory import MemoryCache

        cache = MemoryCache()
        await cache.set("k1", case.cached)
        activities = WorkflowActivities(
            services=dict(DIRECT_SERVICES),
            resources={"cache": cache},
        )
        activity_request = activity_input("explode")
        activity_request.cache = CacheDirective(
            resource="cache",
            key="k1",
            ttl_sec=60,
        )
        activity_request.output_schema = case.schema

        if isinstance(case.outcome, CacheRaises):
            with pytest.raises(case.outcome.exc, match=case.outcome.match) as exc_info:
                await activities.execute_step(activity_request)
            assert exc_info.value.type == "CACHE_INTEGRITY_ERROR"
            assert exc_info.value.non_retryable is True
        else:
            result = await activities.execute_step(activity_request)
            assert result.data == case.outcome.value
            assert result.cache == "hit"


def item_supported(data, resources):
    return data["item"] in resources["runtime_config"]["supported"]


async def async_gate(data, resources):
    return bool(data.get("open"))


def failing_evaluator(data, resources):
    raise ValueError(data["secret"])


def denied_resource_evaluator(data, resources):
    return resources["other"]


def evaluator_input(evaluator: str, data, resource_names: list[str]) -> EvaluatorInput:
    return EvaluatorInput(
        evaluator=evaluator,
        data=data,
        request_id="r1",
        flow_name="f",
        step_name="s",
        resource_names=resource_names,
    )


class TestEvaluateConditionActivity:
    async def test_sync_evaluator_gets_declared_resources(self):
        activities = WorkflowActivities(
            services={}, resources={"runtime_config": {"supported": ["alpha"]}, "other": 1}
        )

        assert (
            await activities.evaluate_condition(
                evaluator_input(
                    f"{__name__}.item_supported",
                    {"item": "alpha"},
                    ["runtime_config"],
                )
            )
            is True
        )
        assert (
            await activities.evaluate_condition(
                evaluator_input(
                    f"{__name__}.item_supported",
                    {"item": "gamma"},
                    ["runtime_config"],
                )
            )
            is False
        )

    async def test_async_evaluator_is_awaited(self):
        activities = WorkflowActivities(services={})

        result = await activities.evaluate_condition(
            evaluator_input(f"{__name__}.async_gate", {"open": True}, [])
        )

        assert result is True

    async def test_missing_declared_resource_is_non_retryable(self):
        activities = WorkflowActivities(services={}, resources={})

        with pytest.raises(ApplicationError) as exc_info:
            await activities.evaluate_condition(
                evaluator_input(f"{__name__}.item_supported", {}, ["runtime_config"])
            )

        assert exc_info.value.type == "EVALUATOR_RESOURCE_MISSING"
        assert exc_info.value.non_retryable is True

    async def test_undeclared_resource_access_is_non_retryable(self):
        activities = WorkflowActivities(services={}, resources={"allowed": object()})

        with pytest.raises(ApplicationError) as exc_info:
            await activities.evaluate_condition(
                evaluator_input(
                    f"{__name__}.denied_resource_evaluator",
                    {},
                    ["allowed"],
                )
            )

        assert exc_info.value.type == "EVALUATOR_RESOURCE_ACCESS_DENIED"
        assert exc_info.value.non_retryable is True

    async def test_unimportable_evaluator_is_non_retryable(self):
        activities = WorkflowActivities(services={})

        with pytest.raises(ApplicationError) as exc_info:
            await activities.evaluate_condition(evaluator_input("no.such.module.fn", {}, []))

        assert exc_info.value.type == "EVALUATOR_IMPORT_ERROR"
        assert exc_info.value.non_retryable is True

    async def test_evaluator_exception_text_is_contained(self):
        sensitive_sentinel = "synthetic-evaluator-secret"
        activities = WorkflowActivities(services={})

        with pytest.raises(ApplicationError) as exc_info:
            await activities.evaluate_condition(
                evaluator_input(
                    f"{__name__}.failing_evaluator",
                    {"secret": sensitive_sentinel},
                    [],
                )
            )

        assert exc_info.value.type == "EVALUATOR_EXECUTION_ERROR"
        assert sensitive_sentinel not in str(exc_info.value)
