"""Process-local metrics with closed, controlled-cardinality labels."""

from __future__ import annotations

import math
import threading
from collections import defaultdict
from enum import Enum

DEFAULT_ACTIVITY_LATENCY_BUCKETS_SECONDS = (0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0)
MAX_METRICS_RESPONSE_BYTES = 262_144
ALLOWED_START_SOURCES = frozenset(
    {"broker", "cloud_event", "control_api", "host", "schedule", "webhook"}
)
ALLOWED_API_OPERATIONS = frozenset(
    {
        "activate",
        "cancel",
        "compare_revisions",
        "configuration_activate",
        "configuration_edit",
        "configuration_publish",
        "configuration_rollback",
        "configuration_validate",
        "configuration_view",
        "create_draft",
        "describe",
        "export_draft",
        "health",
        "import_draft",
        "list",
        "list_activations",
        "list_revisions",
        "metrics",
        "plan_activation",
        "publish",
        "read_activation",
        "read_draft",
        "read_publication",
        "read_readiness",
        "read_revision",
        "register_readiness",
        "rollback",
        "read_triggers_fragment",
        "update_triggers_fragment",
        "trigger_apply",
        "trigger_delete",
        "trigger_pause",
        "trigger_resume",
        "trigger_run",
        "scheduled_start_create",
        "scheduled_start_view",
        "scheduled_start_reschedule",
        "scheduled_start_cancel",
        "signal",
        "start",
        "terminate",
        "update_draft",
        "validate_draft",
        "read_workflow_fragment",
        "update_workflow_fragment",
        "delete_workflow_fragment",
        "preview_workflow_fragment",
        "capability_policy_administer",
        "operations_view",
        "admin_panel_view",
        "operations_overview",
        "operations_workflows",
        "operations_workflow_detail",
        "operations_definitions",
        "operations_runs",
        "operations_run_detail",
        "operations_triggers",
        "operations_configuration",
        "operations_activations",
        "operations_configuration_schema",
        "operations_authoring_reference",
        "operations_workflow_definition",
        "operations_scheduled_starts",
        "operations_scheduled_start_detail",
        "operations_capabilities",
    }
)
ALLOWED_HEALTH_COMPONENTS = frozenset(
    {
        "catalog",
        "temporal",
        "worker",
        "worker_registration",
        "trigger_consumer",
        "response_consumer",
        "providers",
    }
)


class StartMetricOutcome(str, Enum):
    STARTED = "started"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"
    ERROR = "error"


class ApiMetricOutcome(str, Enum):
    SUCCESS = "success"
    CLIENT_ERROR = "client_error"
    AUTH_ERROR = "auth_error"
    SERVER_ERROR = "server_error"


class QueueMetricKind(str, Enum):
    TRIGGER = "trigger"
    RESPONSE = "response"


class QueueMetricOutcome(str, Enum):
    ACK = "ack"
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"


class ActivityMetricOutcome(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"


class ScheduleMetricOperation(str, Enum):
    OBSERVE = "observe"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    LIST = "list"
    DESCRIBE = "describe"
    PAUSE = "pause"
    RESUME = "resume"
    TRIGGER = "trigger"
    BACKFILL = "backfill"


class ScheduleMetricOutcome(str, Enum):
    SUCCESS = "success"
    IDEMPOTENT = "idempotent"
    REJECTED = "rejected"
    ERROR = "error"
    CORRUPT = "corrupt"


class ScheduledStartMetricOutcome(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    RESCHEDULED = "rescheduled"
    CANCELED = "canceled"
    DUE = "due"
    STARTED = "started"
    FAILED = "failed"
    SKIPPED_STALE_FIRE = "skipped_stale_fire"
    ARBITER_CONFLICT = "arbiter_conflict"
    ARBITER_RECLAIM = "arbiter_reclaim"
    MUTATION_WAIT_TIMEOUT = "mutation_wait_timeout"


class ActivationMetricState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_FOR_READINESS = "waiting_for_readiness"
    APPLIED = "applied"
    FAILED = "failed"
    SUPERSEDED = "superseded"
    ROLLED_BACK = "rolled_back"


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._starts: dict[tuple[str, str], int] = defaultdict(int)
        self._api: dict[tuple[str, str], int] = defaultdict(int)
        self._queues: dict[tuple[str, str, str], int] = defaultdict(int)
        self._execution_observations: dict[str, int] = defaultdict(int)
        self._activity_counts: dict[str, int] = defaultdict(int)
        self._activity_latency_count = 0
        self._activity_latency_sum = 0.0
        self._activity_latency_buckets: dict[float, int] = {
            boundary: 0 for boundary in DEFAULT_ACTIVITY_LATENCY_BUCKETS_SECONDS
        }
        self._health: dict[str, int] = {}
        self._schedules: dict[tuple[str, str], int] = defaultdict(int)
        self._scheduled_starts: dict[str, int] = defaultdict(int)
        self._activation_states: dict[str, int] = defaultdict(int)

    def record_start(self, source: str, outcome: StartMetricOutcome) -> None:
        if source not in ALLOWED_START_SOURCES:
            raise ValueError("Start metric source is not a controlled label")
        with self._lock:
            self._starts[(source, outcome.value)] += 1

    def record_api(self, operation: str, outcome: ApiMetricOutcome) -> None:
        if operation not in ALLOWED_API_OPERATIONS:
            raise ValueError("API metric operation is not a controlled label")
        with self._lock:
            self._api[(operation, outcome.value)] += 1

    def record_queue(
        self,
        kind: QueueMetricKind,
        outcome: QueueMetricOutcome,
        *,
        redelivered: bool,
    ) -> None:
        with self._lock:
            self._queues[(kind.value, outcome.value, str(redelivered).lower())] += 1

    def record_execution_observation(self, status: str | None) -> None:
        normalized = status.lower() if status is not None else "unknown"
        allowed = {
            "running",
            "completed",
            "failed",
            "canceled",
            "cancelled",
            "terminated",
            "unknown",
        }
        if normalized not in allowed:
            normalized = "other"
        with self._lock:
            self._execution_observations[normalized] += 1

    def record_activity(self, outcome: ActivityMetricOutcome, duration_seconds: float) -> None:
        if not math.isfinite(duration_seconds) or duration_seconds < 0:
            raise ValueError("Activity metric duration must be a finite non-negative value")
        with self._lock:
            self._activity_counts[outcome.value] += 1
            self._activity_latency_count += 1
            self._activity_latency_sum += duration_seconds
            for boundary in self._activity_latency_buckets:
                if duration_seconds <= boundary:
                    self._activity_latency_buckets[boundary] += 1

    def set_component_health(self, component: str, *, healthy: bool) -> None:
        if component not in ALLOWED_HEALTH_COMPONENTS:
            raise ValueError("Health metric component is not a controlled label")
        with self._lock:
            self._health[component] = int(healthy)

    def record_schedule(
        self,
        operation: ScheduleMetricOperation,
        outcome: ScheduleMetricOutcome,
    ) -> None:
        with self._lock:
            self._schedules[(operation.value, outcome.value)] += 1

    def record_scheduled_start(self, outcome: ScheduledStartMetricOutcome) -> None:
        with self._lock:
            self._scheduled_starts[outcome.value] += 1

    def record_activation(self, state: ActivationMetricState) -> None:
        with self._lock:
            self._activation_states[state.value] += 1

    def render_prometheus(self) -> bytes:
        with self._lock:
            lines = self._render_locked()
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        if len(payload) > MAX_METRICS_RESPONSE_BYTES:
            raise RuntimeError("Metrics response exceeds its byte bound")
        return payload

    def _render_locked(self) -> list[str]:
        lines = [
            "# TYPE justflow_workflow_starts_total counter",
            *(
                f'justflow_workflow_starts_total{{source="{source}",outcome="{outcome}"}} {value}'
                for (source, outcome), value in sorted(self._starts.items())
            ),
            "# TYPE justflow_control_api_requests_total counter",
            *(
                f'justflow_control_api_requests_total{{operation="{operation}",outcome="{outcome}"}} {value}'
                for (operation, outcome), value in sorted(self._api.items())
            ),
            "# TYPE justflow_queue_deliveries_total counter",
            *(
                f'justflow_queue_deliveries_total{{kind="{kind}",outcome="{outcome}",redelivered="{redelivered}"}} {value}'
                for (kind, outcome, redelivered), value in sorted(self._queues.items())
            ),
            "# TYPE justflow_execution_observations_total counter",
            *(
                f'justflow_execution_observations_total{{status="{status}"}} {value}'
                for status, value in sorted(self._execution_observations.items())
            ),
            "# TYPE justflow_activities_total counter",
            *(
                f'justflow_activities_total{{outcome="{outcome}"}} {value}'
                for outcome, value in sorted(self._activity_counts.items())
            ),
            "# TYPE justflow_activity_latency_seconds histogram",
            *(
                f'justflow_activity_latency_seconds_bucket{{le="{boundary:g}"}} {value}'
                for boundary, value in self._activity_latency_buckets.items()
            ),
            f'justflow_activity_latency_seconds_bucket{{le="+Inf"}} {self._activity_latency_count}',
            f"justflow_activity_latency_seconds_sum {self._activity_latency_sum:g}",
            f"justflow_activity_latency_seconds_count {self._activity_latency_count}",
            "# TYPE justflow_component_ready gauge",
            *(
                f'justflow_component_ready{{component="{component}"}} {value}'
                for component, value in sorted(self._health.items())
            ),
            "# TYPE justflow_schedule_operations_total counter",
            *(
                f'justflow_schedule_operations_total{{operation="{operation}",outcome="{outcome}"}} {value}'
                for (operation, outcome), value in sorted(self._schedules.items())
            ),
            "# TYPE justflow_configuration_activation_observations_total counter",
            *(
                f'justflow_configuration_activation_observations_total{{state="{state}"}} {value}'
                for state, value in sorted(self._activation_states.items())
            ),
            "# TYPE justflow_scheduled_starts_total counter",
            *(
                f'justflow_scheduled_starts_total{{outcome="{outcome}"}} {value}'
                for outcome, value in sorted(self._scheduled_starts.items())
            ),
        ]
        return lines
