"""Pydantic models for workflow engine configuration."""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from justflow.config.grammar import (
    AliasName,
    OperationName,
    ParameterName,
    ProviderName,
    ReferencePath,
    ResourceName,
    ServiceName,
    SignalName,
    StepName,
    WorkflowName,
    validate_placeholders,
)

MAX_ACTION_LENGTH = 512
MAX_DESCRIPTION_LENGTH = 4096
MAX_PATH_LENGTH = 4096
MAX_TIMEOUT_SECONDS = 31_536_000
MAX_CONNECT_TIMEOUT_SECONDS = 300
MAX_RETRIES = 100
MAX_CONCURRENCY = 1_000
MAX_ITERATIONS = 10_000
MAX_REDACTION_PATHS = 1_000
MAX_RETENTION_POLICY_LENGTH = 128
PYTHON_CLASS_PATH_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+$"
AUDIT_REDACTABLE_ROOTS = frozenset({"params", "steps", "result", "error", "failures"})
AUDIT_STEP_PAYLOAD_FIELDS = frozenset({"globals", "input", "output", "message"})
RedactionPath = Annotated[str, Field(min_length=1, max_length=MAX_PATH_LENGTH)]
PythonClassPath = Annotated[
    StrictStr,
    Field(
        min_length=3,
        max_length=MAX_ACTION_LENGTH,
        pattern=PYTHON_CLASS_PATH_PATTERN,
    ),
]


class StrictDeclarationModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class IterationFailStrategy(str, Enum):
    STOP = "stop"
    SKIP = "skip"
    COLLECT = "collect"


# --- Resources ---


class ResourceConfig(StrictDeclarationModel):
    provider: ProviderName | None = None
    class_path: PythonClassPath | None = Field(default=None, alias="class")
    config: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_implementation(self) -> ResourceConfig:
        if (self.provider is None) == (self.class_path is None):
            raise ValueError("resource declaration requires exactly one of 'provider' or 'class'")
        return self


class ResourcesConfig(StrictDeclarationModel):
    resources: dict[ResourceName, ResourceConfig]


# --- Services ---


class ServiceConfig(StrictDeclarationModel):
    transport: ProviderName
    transport_config: dict[str, Any] = Field(default_factory=dict)
    connect_timeout_sec: StrictInt | None = Field(None, ge=1, le=MAX_CONNECT_TIMEOUT_SECONDS)
    dispatch_timeout_sec: StrictInt = Field(ge=1, le=MAX_TIMEOUT_SECONDS)
    response_timeout_sec: StrictInt | None = Field(None, ge=1, le=MAX_TIMEOUT_SECONDS)
    retries: StrictInt = Field(ge=0, le=MAX_RETRIES)
    params: dict[str, Any] = Field(default_factory=dict)


class ServicesConfig(StrictDeclarationModel):
    services: dict[ServiceName, ServiceConfig]

    @model_validator(mode="after")
    def validate_service_placeholders(self) -> ServicesConfig:
        validate_placeholders(self.model_dump(mode="python"), path="services")
        return self


# --- Workflow ---


class EvaluatorCondition(StrictDeclarationModel):
    evaluator: str = Field(min_length=1, max_length=MAX_PATH_LENGTH)
    resources: list[ResourceName] = Field(default_factory=list)


class OnResultBranch(StrictDeclarationModel):
    when: (
        Annotated[
            str,
            Field(min_length=1, max_length=MAX_DESCRIPTION_LENGTH),
        ]
        | EvaluatorCondition
        | None
    ) = None
    default: StepName | None = None
    then: StepName | None = None

    @model_validator(mode="after")
    def validate_branch(self) -> OnResultBranch:
        if self.default is None and self.when is None:
            raise ValueError("on_result branch must have either 'when' or 'default'")
        if self.default is not None and self.when is not None:
            raise ValueError("on_result branch cannot have both 'when' and 'default'")
        if self.when is not None and self.then is None:
            raise ValueError("on_result branch with 'when' must have 'then'")
        return self


class WaitForConfig(StrictDeclarationModel):
    signal: SignalName
    timeout_sec: StrictInt | None = Field(None, ge=1, le=MAX_TIMEOUT_SECONDS)
    timeout_until: ReferencePath | None = Field(
        default=None,
        description=(
            "Reference resolved at runtime to epoch seconds or an ISO-8601 timestamp with 'Z' or "
            "an explicit UTC offset"
        ),
    )
    on_timeout: StepName | None = None

    @model_validator(mode="after")
    def validate_bounded(self) -> WaitForConfig:
        if self.timeout_sec is None and self.timeout_until is None:
            raise ValueError("wait_for must be bounded: set timeout_sec and/or timeout_until")
        return self


class CacheConfig(StrictDeclarationModel):
    resource: ResourceName
    key: str = Field(min_length=1, max_length=MAX_PATH_LENGTH)
    ttl_sec: StrictInt | None = Field(None, ge=1, le=MAX_TIMEOUT_SECONDS)


class StepTargetKind(str, Enum):
    SERVICE = "service"
    WORKFLOW = "workflow"


class ServiceOperationTarget(StrictDeclarationModel):
    kind: Literal[StepTargetKind.SERVICE] = StepTargetKind.SERVICE
    service: ServiceName
    action: str = Field(min_length=1, max_length=MAX_ACTION_LENGTH)


class ChildWorkflowTarget(StrictDeclarationModel):
    kind: Literal[StepTargetKind.WORKFLOW] = StepTargetKind.WORKFLOW
    workflow: WorkflowName


StepTarget = Annotated[
    ServiceOperationTarget | ChildWorkflowTarget,
    Field(discriminator="kind"),
]


class StepDefinition(StrictDeclarationModel):
    """A reusable operation with exactly one typed execution target."""

    target: StepTarget
    params: dict[str, Any] = Field(default_factory=dict)
    required_resources: list[ResourceName] = Field(default_factory=list)
    cache: CacheConfig | None = None
    # Inline JSON Schema dict or dotted path to a pydantic BaseModel.
    input_schema: dict[str, Any] | str | None = None
    output_schema: dict[str, Any] | str | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_target(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        normalized = dict(value)
        legacy_keys = {"service", "action", "workflow"}.intersection(normalized)
        if "target" in normalized:
            if legacy_keys:
                raise ValueError("Step definition cannot mix target with legacy target fields")
            return normalized
        workflow = normalized.pop("workflow", None)
        service = normalized.pop("service", None)
        action = normalized.pop("action", None)
        if workflow is not None:
            if service is not None or action is not None:
                raise ValueError(
                    "Step definition must be either service+action or workflow, not both"
                )
            normalized["target"] = {
                "kind": StepTargetKind.WORKFLOW,
                "workflow": workflow,
            }
        elif service is not None or action is not None:
            if service is None or action is None:
                raise ValueError("Step definition requires service and action (or workflow)")
            normalized["target"] = {
                "kind": StepTargetKind.SERVICE,
                "service": service,
                "action": action,
            }
        else:
            raise ValueError("Step definition requires service and action (or workflow)")
        return normalized

    @model_validator(mode="after")
    def validate_target_options(self) -> StepDefinition:
        if isinstance(self.target, ChildWorkflowTarget):
            if self.cache is not None:
                raise ValueError(
                    "cache is not valid on sub-workflow steps (cache inside the child)"
                )
            if self.input_schema is not None or self.output_schema is not None:
                raise ValueError(
                    "schemas are not valid on sub-workflow steps (children own their contracts)"
                )
        return self


class FlowStep(StrictDeclarationModel):
    name: StepName
    op: OperationName | None = None
    input: ReferencePath | dict[str, ReferencePath] | list[ReferencePath] | None = None
    output: AliasName | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    then: StepName | None = None
    on_result: list[OnResultBranch] | None = None
    condition: str | None = Field(None, min_length=1, max_length=MAX_DESCRIPTION_LENGTH)
    for_each: ReferencePath | None = None
    as_var: ParameterName | None = Field(None, alias="as")
    parallel: StrictBool = False
    max_concurrency: StrictInt | None = Field(None, ge=1, le=MAX_CONCURRENCY)
    on_iteration_fail: IterationFailStrategy | None = None
    wait_for: WaitForConfig | None = None
    sleep_sec: StrictInt | None = Field(None, ge=1, le=MAX_TIMEOUT_SECONDS)
    until: str | None = Field(None, min_length=1, max_length=MAX_DESCRIPTION_LENGTH)
    max_iterations: StrictInt | None = Field(None, ge=1, le=MAX_ITERATIONS)
    interval_sec: StrictInt | None = Field(None, ge=1, le=MAX_TIMEOUT_SECONDS)
    on_exhausted: StepName | None = None
    on_failure: StepName | None = None
    terminal: StrictBool = False
    reason: str | None = Field(None, min_length=1, max_length=MAX_DESCRIPTION_LENGTH)

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @model_validator(mode="after")
    def validate_flow_step(self) -> FlowStep:
        if self.terminal:
            terminal_conflicts = (
                self.op,
                self.input,
                self.output,
                self.params or None,
                self.then,
                self.on_result,
                self.condition,
                self.for_each,
                self.as_var,
                self.max_concurrency,
                self.on_iteration_fail,
                self.wait_for,
                self.sleep_sec,
                self.until,
                self.max_iterations,
                self.interval_sec,
                self.on_exhausted,
                self.on_failure,
            )
            if any(value is not None for value in terminal_conflicts):
                raise ValueError(
                    f"Terminal step '{self.name}' should not have execution or transition fields"
                )
            if self.parallel:
                raise ValueError(
                    f"Terminal step '{self.name}' should not have execution or transition fields"
                )
            return self

        kinds = [self.op is not None, self.wait_for is not None, self.sleep_sec is not None]
        if sum(kinds) != 1:
            raise ValueError(
                f"Flow step '{self.name}' must have exactly one of 'op', 'wait_for', or 'sleep_sec'"
            )
        if (self.wait_for or self.sleep_sec) and (self.for_each or self.condition):
            raise ValueError(
                f"Step '{self.name}': wait_for/sleep_sec cannot be combined "
                f"with for_each or condition"
            )
        if self.on_failure and (self.wait_for or self.sleep_sec):
            raise ValueError(
                f"Step '{self.name}': on_failure is only valid on op steps (waits have on_timeout)"
            )
        if self.sleep_sec is not None and (self.on_result or self.output):
            raise ValueError(f"Step '{self.name}': sleep steps produce no output and cannot branch")
        if self.until is not None:
            if self.op is None:
                raise ValueError(f"Step '{self.name}': 'until' is only valid on op steps")
            if self.for_each:
                raise ValueError(f"Step '{self.name}': 'until' cannot be combined with for_each")
            if self.max_iterations is None:
                raise ValueError(f"Step '{self.name}': 'until' requires max_iterations")
        elif any(
            v is not None for v in (self.max_iterations, self.interval_sec, self.on_exhausted)
        ):
            raise ValueError(
                f"Step '{self.name}': max_iterations/interval_sec/on_exhausted "
                f"are only valid with 'until'"
            )
        if self.then and self.on_result and self.condition is None:
            raise ValueError(f"Step '{self.name}' cannot have both 'then' and 'on_result'")
        if self.condition is not None and self.then is None:
            raise ValueError(
                f"Step '{self.name}' with condition requires an explicit 'then' "
                f"target for the false path"
            )
        if self.parallel and not self.for_each:
            raise ValueError(f"Step '{self.name}' has parallel=true but no for_each")
        if self.parallel and self.max_concurrency is None:
            raise ValueError(f"Step '{self.name}' with parallel=true requires max_concurrency")
        if self.max_concurrency is not None and not self.parallel:
            raise ValueError(f"Step '{self.name}' has max_concurrency but parallel is not true")
        if self.then is None and not self.on_result:
            raise ValueError(
                f"Non-terminal step '{self.name}' requires a success successor "
                f"through 'then' or 'on_result'"
            )
        return self


class AuditCaptureMode(str, Enum):
    METADATA_ONLY = "metadata-only"
    REDACTED = "redacted"
    APPROVED_FULL = "approved-full"


class MetadataOnlyAuditCapture(StrictDeclarationModel):
    mode: Literal[AuditCaptureMode.METADATA_ONLY] = AuditCaptureMode.METADATA_ONLY


class RedactedAuditCapture(StrictDeclarationModel):
    mode: Literal[AuditCaptureMode.REDACTED] = AuditCaptureMode.REDACTED
    paths: list[RedactionPath] = Field(
        min_length=1,
        max_length=MAX_REDACTION_PATHS,
    )
    max_payload_bytes: StrictInt = Field(ge=1)

    @field_validator("paths")
    @classmethod
    def validate_redaction_paths(cls, paths: list[str]) -> list[str]:
        for path in paths:
            if not path.startswith("/"):
                raise ValueError("Redaction paths must be JSON Pointers beginning with '/'")
            index = 0
            while index < len(path):
                if path[index] != "~":
                    index += 1
                    continue
                if index + 1 >= len(path) or path[index + 1] not in {"0", "1"}:
                    raise ValueError("Redaction paths contain an invalid JSON Pointer escape")
                index += 2
            tokens = [token.replace("~1", "/").replace("~0", "~") for token in path[1:].split("/")]
            if not tokens or tokens[0] not in AUDIT_REDACTABLE_ROOTS:
                raise ValueError("Redaction paths must target an audit payload field")
            if tokens[0] == "steps" and (
                len(tokens) < 3 or tokens[2] not in AUDIT_STEP_PAYLOAD_FIELDS
            ):
                raise ValueError(
                    "Step redaction paths must target globals, input, output, or message"
                )
            if tokens[0] == "error" and (len(tokens) < 2 or tokens[1] != "message"):
                raise ValueError("Error redaction paths must target message")
            if tokens[0] == "failures" and (len(tokens) < 3 or tokens[2] != "message"):
                raise ValueError("Failure-list redaction paths must target item messages")
        if len(paths) != len(set(paths)):
            raise ValueError("Redaction paths must be unique")
        return paths


class ApprovedFullAuditCapture(StrictDeclarationModel):
    mode: Literal[AuditCaptureMode.APPROVED_FULL] = AuditCaptureMode.APPROVED_FULL
    max_payload_bytes: StrictInt = Field(ge=1)


AuditCaptureConfig = Annotated[
    MetadataOnlyAuditCapture | RedactedAuditCapture | ApprovedFullAuditCapture,
    Field(discriminator="mode"),
]


class OnCompleteConfig(StrictDeclarationModel):
    resource: ResourceName
    path: str = Field(min_length=1, max_length=MAX_PATH_LENGTH)
    retention_policy: str = Field(min_length=1, max_length=MAX_RETENTION_POLICY_LENGTH)
    capture: AuditCaptureConfig = Field(default_factory=lambda: MetadataOnlyAuditCapture())


class OnErrorConfig(StrictDeclarationModel):
    then: StepName


class WorkflowConfig(StrictDeclarationModel):
    workflow: WorkflowName
    description: str = Field(default="", max_length=MAX_DESCRIPTION_LENGTH)
    on_complete: OnCompleteConfig | None = None
    on_error: OnErrorConfig | None = None
    params: dict[ParameterName, str] = Field(default_factory=dict)
    input_schema: dict[str, Any] | str | None = None
    output_schema: dict[str, Any] | str | None = None
    result: ReferencePath | None = None
    steps: dict[OperationName, StepDefinition]
    flow: list[FlowStep] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_workflow(self) -> WorkflowConfig:
        validate_placeholders(self.model_dump(mode="python"), path="workflow")

        if self.output_schema is not None and self.result is None:
            raise ValueError("Workflow output_schema requires a configured result")

        for step in self.flow:
            if not step.on_result:
                continue
            default_positions = [i for i, b in enumerate(step.on_result) if b.default is not None]
            if not default_positions:
                raise ValueError(f"Step '{step.name}' on_result must end with a 'default' branch")
            if default_positions != [len(step.on_result) - 1]:
                raise ValueError(
                    f"Step '{step.name}' on_result must have exactly one 'default' "
                    f"branch and it must be last (branches after it would never run)"
                )
        return self
