"""Tests for aggregate health and controlled-cardinality runtime metrics."""

from __future__ import annotations

import pytest

from justflow.runtime.health import (
    HealthComponent,
    HealthReason,
    HealthRegistry,
    HealthStatus,
)
from justflow.runtime.metrics import (
    ActivationMetricState,
    ActivityMetricOutcome,
    ApiMetricOutcome,
    MetricsRegistry,
    QueueMetricKind,
    QueueMetricOutcome,
    ScheduleMetricOperation,
    ScheduleMetricOutcome,
    StartMetricOutcome,
)


def test_readiness_requires_only_declared_components() -> None:
    metrics = MetricsRegistry()
    health = HealthRegistry(
        frozenset({HealthComponent.CATALOG, HealthComponent.TEMPORAL}),
        metrics=metrics,
    )

    health.mark_ready(HealthComponent.CATALOG)
    unavailable = health.report()
    health.mark_ready(HealthComponent.TEMPORAL)
    ready = health.report()
    health.mark_unavailable(HealthComponent.TEMPORAL, HealthReason.STOPPED)
    stopped = health.report()

    assert unavailable.ready is False
    assert ready.ready is True
    assert stopped.ready is False
    temporal = next(
        component
        for component in stopped.components
        if component.component is HealthComponent.TEMPORAL
    )
    assert temporal.status is HealthStatus.UNAVAILABLE
    assert temporal.reason is HealthReason.STOPPED


def test_metrics_render_only_closed_labels_and_aggregate_values() -> None:
    metrics = MetricsRegistry()
    metrics.record_start("webhook", StartMetricOutcome.STARTED)
    metrics.record_start("webhook", StartMetricOutcome.DUPLICATE)
    metrics.record_start("cloud_event", StartMetricOutcome.STARTED)
    metrics.record_api("start", ApiMetricOutcome.SUCCESS)
    metrics.record_api("operations_workflow_detail", ApiMetricOutcome.SUCCESS)
    metrics.record_queue(
        QueueMetricKind.TRIGGER,
        QueueMetricOutcome.ACK,
        redelivered=False,
    )
    metrics.record_execution_observation("COMPLETED")
    metrics.record_activity(ActivityMetricOutcome.COMPLETED, 0.25)
    metrics.set_component_health("temporal", healthy=True)
    metrics.record_schedule(ScheduleMetricOperation.CREATE, ScheduleMetricOutcome.SUCCESS)
    metrics.record_activation(ActivationMetricState.WAITING_FOR_READINESS)

    rendered = metrics.render_prometheus().decode("utf-8")

    assert 'source="webhook",outcome="started"} 1' in rendered
    assert 'source="webhook",outcome="duplicate"} 1' in rendered
    assert 'source="cloud_event",outcome="started"} 1' in rendered
    assert 'operation="start",outcome="success"} 1' in rendered
    assert 'operation="operations_workflow_detail",outcome="success"} 1' in rendered
    assert 'kind="trigger",outcome="ack",redelivered="false"} 1' in rendered
    assert 'status="completed"} 1' in rendered
    assert 'outcome="completed"} 1' in rendered
    assert 'component="temporal"} 1' in rendered
    assert 'operation="create",outcome="success"} 1' in rendered
    assert 'state="waiting_for_readiness"} 1' in rendered


@pytest.mark.parametrize(
    ("operation", "argument"),
    [
        pytest.param("start", "source-id", id="unbounded-start-source"),
        pytest.param("api", "workflow-id", id="unbounded-api-operation"),
        pytest.param("health", "provider-id", id="unbounded-health-component"),
    ],
)
def test_metrics_reject_uncontrolled_labels(operation: str, argument: str) -> None:
    metrics = MetricsRegistry()

    with pytest.raises(ValueError, match="controlled label"):
        if operation == "start":
            metrics.record_start(argument, StartMetricOutcome.ERROR)
        elif operation == "api":
            metrics.record_api(argument, ApiMetricOutcome.SERVER_ERROR)
        else:
            metrics.set_component_health(argument, healthy=False)


@pytest.mark.parametrize(
    "duration",
    [
        pytest.param(-0.1, id="negative"),
        pytest.param(float("inf"), id="infinite"),
        pytest.param(float("nan"), id="not-a-number"),
    ],
)
def test_activity_metrics_reject_invalid_durations(duration: float) -> None:
    with pytest.raises(ValueError, match="finite non-negative"):
        MetricsRegistry().record_activity(ActivityMetricOutcome.FAILED, duration)
