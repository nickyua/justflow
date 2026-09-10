"""Bounded process health and readiness state."""

from __future__ import annotations

import threading
from enum import Enum

from pydantic import BaseModel, ConfigDict

from justflow.runtime.metrics import MetricsRegistry


class HealthComponent(str, Enum):
    CATALOG = "catalog"
    TEMPORAL = "temporal"
    WORKER = "worker"
    WORKER_REGISTRATION = "worker_registration"
    TRIGGER_CONSUMER = "trigger_consumer"
    RESPONSE_CONSUMER = "response_consumer"
    PROVIDERS = "providers"


class HealthStatus(str, Enum):
    READY = "ready"
    UNAVAILABLE = "unavailable"


class HealthReason(str, Enum):
    AVAILABLE = "available"
    NOT_STARTED = "not_started"
    STARTUP_FAILED = "startup_failed"
    STOPPED = "stopped"


class StrictHealthModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ComponentHealth(StrictHealthModel):
    component: HealthComponent
    status: HealthStatus
    reason: HealthReason
    required: bool


class HealthReport(StrictHealthModel):
    ready: bool
    components: tuple[ComponentHealth, ...]


class HealthRegistry:
    def __init__(
        self,
        required: frozenset[HealthComponent],
        *,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        self._required = required
        self._metrics = metrics
        self._lock = threading.Lock()
        self._states = {
            component: (HealthStatus.UNAVAILABLE, HealthReason.NOT_STARTED)
            for component in HealthComponent
        }

    def mark_ready(self, component: HealthComponent) -> None:
        self._set(component, HealthStatus.READY, HealthReason.AVAILABLE)

    def mark_unavailable(
        self,
        component: HealthComponent,
        reason: HealthReason = HealthReason.STARTUP_FAILED,
    ) -> None:
        self._set(component, HealthStatus.UNAVAILABLE, reason)

    def report(self) -> HealthReport:
        with self._lock:
            components = tuple(
                ComponentHealth(
                    component=component,
                    status=self._states[component][0],
                    reason=self._states[component][1],
                    required=component in self._required,
                )
                for component in HealthComponent
            )
        return HealthReport(
            ready=all(
                component.status is HealthStatus.READY
                for component in components
                if component.required
            ),
            components=components,
        )

    def _set(
        self,
        component: HealthComponent,
        status: HealthStatus,
        reason: HealthReason,
    ) -> None:
        with self._lock:
            self._states[component] = (status, reason)
        if self._metrics is not None:
            self._metrics.set_component_health(
                component.value,
                healthy=status is HealthStatus.READY,
            )
