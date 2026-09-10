"""Private validation of one authored workflow declaration."""

from __future__ import annotations

import importlib
import inspect
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from justflow.config.diagnostics import DiagnosticCategory, DiagnosticSeverity
from justflow.config.grammar import RESERVED_DATA_ROOTS, ReferencePath, placeholder_names
from justflow.config.models import (
    ApprovedFullAuditCapture,
    ChildWorkflowTarget,
    EvaluatorCondition,
    FlowStep,
    RedactedAuditCapture,
    ResourcesConfig,
    ServiceOperationTarget,
    ServicesConfig,
    StepDefinition,
    WorkflowConfig,
)
from justflow.config.runtime_limits import RuntimeLimits
from justflow.engine.analysis import analyze_workflow
from justflow.engine.contracts import (
    ContractIdentity,
    SchemaDeclarationError,
    SchemaLoadError,
    SchemaPathResult,
    SchemaSpec,
    SchemaValueType,
    inspect_schema_path,
    validate_contract_declaration,
)
from justflow.engine.evaluator import (
    ExpressionSyntaxError,
    condition_reference_roots,
    condition_references,
)
from justflow.resources.base import ResourceCapability, ResourceCapabilityError
from justflow.resources.registry import ResolvedResource, ResourceRegistry
from justflow.transports.registry import ResolvedService, TransportRegistry, TransportRegistryError

if TYPE_CHECKING:
    from justflow.config.validator import ValidationResult


INPUT_ROOT = "input"
COLLECTION_SCHEMA_TYPES = frozenset({SchemaValueType.ARRAY, SchemaValueType.OBJECT})
DEADLINE_SCHEMA_TYPES = frozenset(
    {SchemaValueType.INTEGER, SchemaValueType.NUMBER, SchemaValueType.STRING}
)


@dataclass(frozen=True, kw_only=True)
class _WorkflowValidationContext:
    workflow: WorkflowConfig
    location: str
    ordered_step_names: tuple[str, ...]
    step_names: frozenset[str]
    step_definition_names: frozenset[str]
    namespaces: frozenset[str]


@dataclass(frozen=True, kw_only=True)
class _WorkflowValidator:
    resources: ResourcesConfig
    services: ServicesConfig
    workflows: Mapping[str, WorkflowConfig]
    resolved_resources: Mapping[str, ResolvedResource]
    resolved_services: Mapping[str, ResolvedService]
    check_imports: bool
    transport_registry: TransportRegistry
    resource_registry: ResourceRegistry
    limits: RuntimeLimits
    contract_identities: dict[str, ContractIdentity]

    def validate(
        self,
        workflow_name: str,
        workflow: WorkflowConfig,
        result: ValidationResult,
    ) -> None:
        context = self._context(workflow_name, workflow)
        self._validate_namespaces(context, result)
        self._validate_contracts_and_result(context, result)
        self._validate_completion(context, result)
        self._validate_step_definitions(context, result)
        self._validate_flow(context, result)
        self._validate_unused_operations_and_analysis(context, result)

    @staticmethod
    def _context(
        workflow_name: str,
        workflow: WorkflowConfig,
    ) -> _WorkflowValidationContext:
        ordered_step_names = tuple(step.name for step in workflow.flow)
        step_names = frozenset(ordered_step_names)
        output_aliases = frozenset(step.output for step in workflow.flow if step.output)
        namespaces = step_names | output_aliases | frozenset(workflow.params)
        if workflow.on_error is not None or any(step.on_failure for step in workflow.flow):
            namespaces |= {"error"}
        return _WorkflowValidationContext(
            workflow=workflow,
            location=f"workflows.{workflow_name}",
            ordered_step_names=ordered_step_names,
            step_names=step_names,
            step_definition_names=frozenset(workflow.steps),
            namespaces=namespaces,
        )

    def _validate_namespaces(
        self,
        context: _WorkflowValidationContext,
        result: ValidationResult,
    ) -> None:
        workflow = context.workflow
        duplicate_step_names = sorted(
            name for name, count in Counter(context.ordered_step_names).items() if count > 1
        )
        if duplicate_step_names:
            result.add(context.location, f"Duplicate flow step names: {duplicate_step_names}")

        for parameter in sorted(RESERVED_DATA_ROOTS.intersection(workflow.params)):
            result.add(
                f"{context.location}.params.{parameter}",
                f"Workflow parameter '{parameter}' collides with a reserved engine root",
            )
        for step in workflow.flow:
            if step.name in RESERVED_DATA_ROOTS:
                result.add(
                    f"{context.location}.flow.{step.name}",
                    f"Flow step '{step.name}' collides with a reserved engine root",
                )
            if step.as_var in RESERVED_DATA_ROOTS:
                result.add(
                    f"{context.location}.flow.{step.name}.as",
                    f"Iteration variable '{step.as_var}' collides with a reserved engine root",
                )

        reserved_aliases = RESERVED_DATA_ROOTS | set(workflow.params) | set(context.step_names)
        alias_producers: dict[str, list[str]] = {}
        for step in workflow.flow:
            if step.output is None:
                continue
            alias_producers.setdefault(step.output, []).append(step.name)
            if step.output in reserved_aliases:
                result.add(
                    f"{context.location}.flow.{step.name}",
                    f"Output alias '{step.output}' collides with a reserved root, "
                    f"workflow parameter, or flow step",
                )
        for alias, producers in alias_producers.items():
            if len(producers) > 1:
                result.add(
                    context.location,
                    f"Output alias '{alias}' has more than one producer: {producers}",
                )

        if workflow.on_error is not None and workflow.on_error.then not in context.step_names:
            result.add(
                f"{context.location}.on_error",
                f"on_error target '{workflow.on_error.then}' not found in flow",
            )

    def _validate_contracts_and_result(
        self,
        context: _WorkflowValidationContext,
        result: ValidationResult,
    ) -> None:
        workflow = context.workflow
        self._validate_contract(
            f"{context.location}.input_schema",
            workflow.input_schema,
            result,
        )
        self._validate_contract(
            f"{context.location}.output_schema",
            workflow.output_schema,
            result,
        )
        self._validate_workflow_placeholders(context.location, workflow, result)

        if workflow.result is None:
            return
        root = workflow.result.root
        if root not in context.namespaces:
            result.add(
                f"{context.location}.result",
                f"result reference '{workflow.result}': '{root}' is not a flow step, "
                f"output alias, or workflow param",
                category=DiagnosticCategory.REFERENCE,
            )
            return
        self._add_schema_path_error(
            result,
            f"{context.location}.result",
            workflow.result,
            self._inspect_workflow_reference(workflow.result, workflow),
        )

    def _validate_completion(
        self,
        context: _WorkflowValidationContext,
        result: ValidationResult,
    ) -> None:
        completion = context.workflow.on_complete
        if completion is None:
            return

        archive_resource = self.resolved_resources.get(completion.resource)
        if archive_resource is None:
            if completion.resource not in self.resources.resources:
                result.add(
                    f"{context.location}.on_complete",
                    f"Resource '{completion.resource}' not found in resources.yaml",
                )
        else:
            try:
                self.resource_registry.require_capability(
                    archive_resource,
                    ResourceCapability.ARCHIVE,
                )
            except ResourceCapabilityError as exc:
                result.add(
                    f"{context.location}.on_complete.resource",
                    str(exc),
                    cause=exc,
                )

        if (
            isinstance(
                completion.capture,
                (RedactedAuditCapture, ApprovedFullAuditCapture),
            )
            and completion.capture.max_payload_bytes > self.limits.audit_record_bytes
        ):
            result.add(
                f"{context.location}.on_complete.capture.max_payload_bytes",
                f"Declared audit payload bound {completion.capture.max_payload_bytes} "
                f"exceeds the effective audit record limit {self.limits.audit_record_bytes}",
                category=DiagnosticCategory.LIMIT,
            )

    def _validate_step_definitions(
        self,
        context: _WorkflowValidationContext,
        result: ValidationResult,
    ) -> None:
        for step_name, step_definition in context.workflow.steps.items():
            self._validate_step_definition(context, step_name, step_definition, result)

    def _validate_step_definition(
        self,
        context: _WorkflowValidationContext,
        step_name: str,
        step_definition: StepDefinition,
        result: ValidationResult,
    ) -> None:
        location = f"{context.location}.steps.{step_name}"
        self._validate_contract(
            f"{location}.input_schema",
            step_definition.input_schema,
            result,
        )
        self._validate_contract(
            f"{location}.output_schema",
            step_definition.output_schema,
            result,
        )
        self._validate_step_target(location, step_definition, result)
        self._validate_required_resources(location, step_definition, result)
        self._validate_step_cache(location, step_definition, result)

    def _validate_step_target(
        self,
        location: str,
        step_definition: StepDefinition,
        result: ValidationResult,
    ) -> None:
        target = step_definition.target
        if isinstance(target, ChildWorkflowTarget):
            if target.workflow not in self.workflows:
                result.add(
                    location,
                    f"Sub-workflow '{target.workflow}' not found in loaded workflows",
                )
        elif target.service not in self.services.services:
            result.add(
                location,
                f"Service '{target.service}' not found in services.yaml",
            )
        else:
            service = self.resolved_services.get(target.service)
            if service is not None:
                try:
                    action_error = self.transport_registry.validate_action(
                        service,
                        target.action,
                    )
                except TransportRegistryError as exc:
                    result.add(location, str(exc))
                else:
                    if action_error is not None:
                        result.add(location, action_error)

    def _validate_required_resources(
        self,
        location: str,
        step_definition: StepDefinition,
        result: ValidationResult,
    ) -> None:
        for resource_name in step_definition.required_resources:
            if resource_name not in self.resources.resources:
                result.add(
                    location,
                    f"Required resource '{resource_name}' not found in resources.yaml",
                )

        duplicate_grants = sorted(
            name for name, count in Counter(step_definition.required_resources).items() if count > 1
        )
        if duplicate_grants:
            result.add(
                f"{location}.required_resources",
                f"Duplicate resource grants: {duplicate_grants}",
            )

    def _validate_step_cache(
        self,
        location: str,
        step_definition: StepDefinition,
        result: ValidationResult,
    ) -> None:
        if step_definition.cache is None:
            return
        cache_resource = self.resolved_resources.get(step_definition.cache.resource)
        if cache_resource is None:
            if step_definition.cache.resource not in self.resources.resources:
                result.add(
                    location,
                    f"Cache resource '{step_definition.cache.resource}' not found in resources.yaml",
                )
            return
        try:
            self.resource_registry.require_capability(
                cache_resource,
                ResourceCapability.CACHE,
            )
        except ResourceCapabilityError as exc:
            result.add(f"{location}.cache.resource", str(exc), cause=exc)

    def _validate_flow(
        self,
        context: _WorkflowValidationContext,
        result: ValidationResult,
    ) -> None:
        for step in context.workflow.flow:
            if not step.terminal:
                self._validate_flow_step(context, step, result)

    def _validate_flow_step(
        self,
        context: _WorkflowValidationContext,
        step: FlowStep,
        result: ValidationResult,
    ) -> None:
        location = f"{context.location}.flow.{step.name}"
        self._validate_flow_step_limits(location, step, result)
        self._validate_operation_reference(context, step, result)
        self._validate_failure_targets(context, step, result)
        self._validate_wait_for(context, step, result)
        self._validate_output_alias(context, step, result)
        self._validate_step_references(location, step, context.namespaces, result)
        self._validate_reference_fields(location, step, context.workflow, result)
        self._validate_schema_semantics(location, step, context.workflow, result)
        self._validate_successor(context, step, result)
        self._validate_result_branches(context, step, result)

    def _validate_flow_step_limits(
        self,
        location: str,
        step: FlowStep,
        result: ValidationResult,
    ) -> None:
        effective_iteration_limit = min(
            self.limits.loop_attempts,
            self.limits.total_invocations,
        )
        if step.max_iterations is not None and step.max_iterations > effective_iteration_limit:
            result.add(
                f"{location}.max_iterations",
                f"Declared iteration bound {step.max_iterations} exceeds the effective "
                f"limit {effective_iteration_limit}",
                category=DiagnosticCategory.LIMIT,
            )

        effective_concurrency_limit = min(
            self.limits.parallelism,
            self.limits.fanout_items,
        )
        if step.max_concurrency is not None and step.max_concurrency > effective_concurrency_limit:
            result.add(
                f"{location}.max_concurrency",
                f"Declared concurrency bound {step.max_concurrency} exceeds the effective "
                f"limit {effective_concurrency_limit}",
                category=DiagnosticCategory.LIMIT,
            )

    @staticmethod
    def _validate_operation_reference(
        context: _WorkflowValidationContext,
        step: FlowStep,
        result: ValidationResult,
    ) -> None:
        if step.op is None:
            return
        location = f"{context.location}.flow.{step.name}"
        if step.op not in context.step_definition_names:
            result.add(location, f"Op '{step.op}' not found in step definitions")
            return

        step_definition = context.workflow.steps[step.op]
        if (step.for_each is None and step.until is None) or step_definition.cache is None:
            return
        loop_kind = "for_each" if step.for_each is not None else "until"
        cache = step_definition.cache
        result.add(
            location,
            f"Step '{step.name}' uses operation '{step.op}' with {loop_kind}, but that operation "
            f"declares cache resource='{cache.resource}' key='{cache.key}'",
        )

    @staticmethod
    def _validate_failure_targets(
        context: _WorkflowValidationContext,
        step: FlowStep,
        result: ValidationResult,
    ) -> None:
        location = f"{context.location}.flow.{step.name}"
        if step.on_exhausted and step.on_exhausted not in context.step_names:
            result.add(
                location,
                f"on_exhausted target '{step.on_exhausted}' not found in flow",
            )
        if step.on_failure and step.on_failure not in context.step_names:
            result.add(
                location,
                f"on_failure target '{step.on_failure}' not found in flow",
            )

    def _validate_wait_for(
        self,
        context: _WorkflowValidationContext,
        step: FlowStep,
        result: ValidationResult,
    ) -> None:
        if step.wait_for is None:
            return
        location = f"{context.location}.flow.{step.name}"
        if step.wait_for.on_timeout and step.wait_for.on_timeout not in context.step_names:
            result.add(
                location,
                f"on_timeout target '{step.wait_for.on_timeout}' not found in flow",
            )
        if step.wait_for.timeout_until is None:
            return

        timeout_reference = step.wait_for.timeout_until
        if timeout_reference.root not in context.namespaces:
            result.add(
                location,
                f"timeout_until reference '{timeout_reference}': '{timeout_reference.root}' is "
                f"not a flow step, output alias, or workflow param",
                category=DiagnosticCategory.REFERENCE,
            )
            return

        status = self._inspect_workflow_reference(timeout_reference, context.workflow)
        timeout_location = f"{location}.wait_for.timeout_until"
        self._add_schema_path_error(
            result,
            timeout_location,
            timeout_reference,
            status,
        )
        self._add_schema_type_error(
            result,
            timeout_location,
            "timeout_until",
            status,
            DEADLINE_SCHEMA_TYPES,
            "an ISO-8601 string or epoch number",
        )

    @staticmethod
    def _validate_output_alias(
        context: _WorkflowValidationContext,
        step: FlowStep,
        result: ValidationResult,
    ) -> None:
        if step.output and step.output in context.step_names and step.output != step.name:
            result.add(
                f"{context.location}.flow.{step.name}",
                f"Output '{step.output}' collides with flow step '{step.output}'",
            )

    @staticmethod
    def _validate_successor(
        context: _WorkflowValidationContext,
        step: FlowStep,
        result: ValidationResult,
    ) -> None:
        if step.then and step.then not in context.step_names:
            result.add(
                f"{context.location}.flow.{step.name}",
                f"'then' target '{step.then}' not found in flow",
            )

    def _validate_result_branches(
        self,
        context: _WorkflowValidationContext,
        step: FlowStep,
        result: ValidationResult,
    ) -> None:
        if not step.on_result:
            return
        location = f"{context.location}.flow.{step.name}"
        duplicate_pairs = self._duplicate_result_condition_pairs(step)
        if duplicate_pairs:
            result.add(
                f"{location}.on_result",
                f"Duplicate result conditions at branch indexes: {duplicate_pairs}",
            )
        for branch in step.on_result:
            target = branch.then or branch.default
            if target and target not in context.step_names:
                result.add(location, f"on_result target '{target}' not found in flow")
            if isinstance(branch.when, EvaluatorCondition):
                self._validate_evaluator_condition(location, branch.when, result)

    def _validate_evaluator_condition(
        self,
        location: str,
        condition: EvaluatorCondition,
        result: ValidationResult,
    ) -> None:
        if self.check_imports:
            evaluator_error, evaluator_cause = self._evaluator_import_error(condition.evaluator)
        else:
            evaluator_error, evaluator_cause = None, None
        if evaluator_error is not None:
            result.add(
                location,
                evaluator_error,
                category=DiagnosticCategory.IMPORT,
                cause=evaluator_cause,
            )

        duplicate_resources = sorted(
            name for name, count in Counter(condition.resources).items() if count > 1
        )
        if duplicate_resources:
            result.add(
                f"{location}.on_result",
                f"Duplicate evaluator resource grants: {duplicate_resources}",
            )
        for resource_name in condition.resources:
            if resource_name not in self.resources.resources:
                result.add(
                    location,
                    f"Evaluator resource '{resource_name}' not in resources.yaml",
                )

    @staticmethod
    def _validate_unused_operations_and_analysis(
        context: _WorkflowValidationContext,
        result: ValidationResult,
    ) -> None:
        used_operations = {step.op for step in context.workflow.flow if step.op is not None}
        for operation in sorted(set(context.workflow.steps).difference(used_operations)):
            result.add(
                f"{context.location}.steps.{operation}",
                f"Operation definition '{operation}' is not used by the workflow flow",
                category=DiagnosticCategory.LINT,
                severity=DiagnosticSeverity.WARNING,
            )
        for issue in analyze_workflow(context.workflow):
            result.add(f"{context.location}.{issue.location}", issue.message)

    def _validate_contract(
        self,
        location: str,
        schema: dict[str, object] | str | None,
        result: ValidationResult,
    ) -> None:
        if schema is None or (isinstance(schema, str) and not self.check_imports):
            return
        try:
            self.contract_identities[location] = validate_contract_declaration(schema)
        except (SchemaDeclarationError, SchemaLoadError) as exc:
            result.add(
                location,
                str(exc),
                category=DiagnosticCategory.DECLARATION,
                cause=exc,
            )

    def _validate_workflow_placeholders(
        self,
        location: str,
        workflow: WorkflowConfig,
        result: ValidationResult,
    ) -> None:
        allowed = frozenset({*workflow.params, "request_id"})
        fields: list[tuple[str, object]] = [("params", workflow.params)]
        if workflow.on_complete is not None:
            fields.append(("on_complete.path", workflow.on_complete.path))
        for operation, definition in workflow.steps.items():
            fields.append((f"steps.{operation}.params", definition.params))
            if definition.cache is not None:
                fields.append((f"steps.{operation}.cache.key", definition.cache.key))
            if isinstance(definition.target, ServiceOperationTarget):
                service = self.services.services.get(definition.target.service)
                if service is not None:
                    fields.append((f"services.{definition.target.service}.params", service.params))
        for step in workflow.flow:
            fields.append((f"flow.{step.name}.params", step.params))

        for field_location, value in fields:
            missing = sorted(placeholder_names(value).difference(allowed))
            if missing:
                result.add(
                    f"{location}.{field_location}",
                    f"Placeholders require undeclared trigger globals: {missing}",
                    category=DiagnosticCategory.REFERENCE,
                )

    def _validate_step_references(
        self,
        location: str,
        step: FlowStep,
        namespaces: frozenset[str],
        result: ValidationResult,
    ) -> None:
        def check_reference(reference: ReferencePath, field: str) -> None:
            root = reference.root
            if root not in namespaces:
                result.add(
                    location,
                    f"{field} reference '{reference}': '{root}' is not a flow step, "
                    f"output alias, or workflow param",
                )

        if isinstance(step.input, ReferencePath):
            check_reference(step.input, "input")
        elif isinstance(step.input, dict):
            for reference in step.input.values():
                check_reference(reference, "input")
        elif isinstance(step.input, list):
            for reference in step.input:
                check_reference(reference, "input")

        if step.for_each:
            if step.for_each.root == INPUT_ROOT:
                if step.input is None:
                    result.add(
                        location,
                        f"for_each '{step.for_each}' references the step input, "
                        f"but the step has no 'input'",
                    )
            else:
                check_reference(step.for_each, "for_each")

        def check_condition_roots(condition: str, field: str) -> None:
            try:
                roots = condition_reference_roots(condition)
            except ExpressionSyntaxError as exc:
                result.add(location, f"{field}: {exc}")
                return
            for root in roots:
                if root != INPUT_ROOT and root not in namespaces:
                    result.add(
                        location,
                        f"{field} reference root '{root}' is not a flow step, "
                        f"output alias, or workflow param",
                    )

        if step.condition:
            check_condition_roots(step.condition, "condition")
        if step.until:
            check_condition_roots(step.until, "until")
        for branch in step.on_result or []:
            if isinstance(branch.when, str):
                check_condition_roots(branch.when, "on_result when")

    def _validate_reference_fields(
        self,
        location: str,
        step: FlowStep,
        workflow: WorkflowConfig,
        result: ValidationResult,
    ) -> None:
        refs: list[ReferencePath] = []
        if isinstance(step.input, ReferencePath):
            refs = [step.input]
        elif isinstance(step.input, dict):
            refs = list(step.input.values())
        elif isinstance(step.input, list):
            refs = list(step.input)

        for reference in refs:
            self._add_schema_path_error(
                result,
                f"{location}.input",
                reference,
                self._inspect_workflow_reference(reference, workflow),
            )

    def _validate_schema_semantics(
        self,
        location: str,
        step: FlowStep,
        workflow: WorkflowConfig,
        result: ValidationResult,
    ) -> None:
        step_definition = workflow.steps.get(step.op) if step.op is not None else None
        input_schema = step_definition.input_schema if step_definition is not None else None
        output_schema = step_definition.output_schema if step_definition is not None else None

        if step.for_each is not None:
            if step.for_each.root == INPUT_ROOT:
                status = self._inspect_declared_path(input_schema, step.for_each.path)
            else:
                status = self._inspect_workflow_reference(step.for_each, workflow)
            self._add_schema_path_error(
                result,
                f"{location}.for_each",
                step.for_each,
                status,
            )
            self._add_schema_type_error(
                result,
                f"{location}.for_each",
                "for_each",
                status,
                COLLECTION_SCHEMA_TYPES,
                "an array or object",
            )

        if step.condition is not None:
            self._validate_condition_schema_paths(
                step.condition,
                input_schema=input_schema,
                workflow=workflow,
                location=f"{location}.condition",
                result=result,
            )
        if step.until is not None:
            self._validate_condition_schema_paths(
                step.until,
                input_schema=output_schema,
                workflow=workflow,
                location=f"{location}.until",
                result=result,
            )
        for index, branch in enumerate(step.on_result or []):
            if isinstance(branch.when, str):
                self._validate_condition_schema_paths(
                    branch.when,
                    input_schema=output_schema,
                    workflow=workflow,
                    location=f"{location}.on_result.{index}.when",
                    result=result,
                )

    def _validate_condition_schema_paths(
        self,
        condition: str,
        *,
        input_schema: SchemaSpec | None,
        workflow: WorkflowConfig,
        location: str,
        result: ValidationResult,
    ) -> None:
        try:
            references = condition_references(condition)
        except ExpressionSyntaxError:
            return
        for reference in references:
            if reference.optional:
                continue
            if reference.root == INPUT_ROOT:
                status = self._inspect_declared_path(input_schema, reference.path)
            else:
                status = self._inspect_workflow_components(
                    reference.root,
                    reference.path,
                    workflow,
                )
            path_text = ".".join(
                (reference.root, *(str(component) for component in reference.path))
            )
            self._add_schema_path_error(result, location, path_text, status)

    @staticmethod
    def _inspect_declared_path(
        schema: SchemaSpec | None,
        path: tuple[str | int, ...],
    ) -> SchemaPathResult:
        if schema is None:
            return SchemaPathResult(exists=None)
        return inspect_schema_path(schema, path)

    def _inspect_workflow_reference(
        self,
        reference: ReferencePath,
        workflow: WorkflowConfig,
    ) -> SchemaPathResult:
        return self._inspect_workflow_components(
            reference.root,
            reference.path,
            workflow,
        )

    @staticmethod
    def _inspect_workflow_components(
        root: str,
        path: tuple[str | int, ...],
        workflow: WorkflowConfig,
    ) -> SchemaPathResult:
        flow_by_name = {step.name: step for step in workflow.flow}
        producer: FlowStep | None = None
        if root in flow_by_name:
            producer = flow_by_name[root]
            output_key = producer.output or producer.name
            output_schema: SchemaSpec | bool = True
            if producer.op is not None:
                definition = workflow.steps.get(producer.op)
                if definition is not None and definition.output_schema is not None:
                    output_schema = definition.output_schema
            wrapper_schema: SchemaSpec = {
                "type": "object",
                "properties": {output_key: output_schema},
                "additionalProperties": False,
            }
            if producer.for_each is not None and len(path) > 1:
                return SchemaPathResult(exists=None)
            return inspect_schema_path(wrapper_schema, path)

        producers = [
            step
            for step in workflow.flow
            if not step.terminal and (step.output or step.name) == root
        ]
        if len(producers) == 1:
            producer = producers[0]

        if producer is not None and producer.op is not None:
            if producer.for_each is not None and path:
                return SchemaPathResult(exists=None)
            definition = workflow.steps.get(producer.op)
            if definition is not None and definition.output_schema is not None:
                return inspect_schema_path(definition.output_schema, path)
        if root in workflow.params and workflow.input_schema is not None:
            return inspect_schema_path(workflow.input_schema, (root, *path))
        if root == "request_id":
            return inspect_schema_path({"type": "string"}, path)
        return SchemaPathResult(exists=None)

    @staticmethod
    def _add_schema_path_error(
        result: ValidationResult,
        location: str,
        reference: ReferencePath | str,
        status: SchemaPathResult,
    ) -> None:
        if status.exists is False:
            detail = status.detail or "path is not declared"
            result.add(
                location,
                f"Reference '{reference}' is invalid for its declared schema: {detail}",
                category=DiagnosticCategory.REFERENCE,
            )

    @staticmethod
    def _add_schema_type_error(
        result: ValidationResult,
        location: str,
        field: str,
        status: SchemaPathResult,
        allowed: frozenset[SchemaValueType],
        expectation: str,
    ) -> None:
        if status.value_types is None or not status.value_types.isdisjoint(allowed):
            return
        actual = ", ".join(sorted(value_type.value for value_type in status.value_types))
        result.add(
            location,
            f"{field} resolves to schema-known {actual}, expected {expectation}",
            category=DiagnosticCategory.DECLARATION,
        )

    @staticmethod
    def _duplicate_result_condition_pairs(step: FlowStep) -> list[tuple[int, int]]:
        seen: dict[tuple[object, ...], int] = {}
        duplicates: list[tuple[int, int]] = []
        for index, branch in enumerate(step.on_result or []):
            if isinstance(branch.when, str):
                identity: tuple[object, ...] = ("expression", branch.when)
            elif isinstance(branch.when, EvaluatorCondition):
                identity = (
                    "evaluator",
                    branch.when.evaluator,
                    tuple(branch.when.resources),
                )
            else:
                continue
            if identity in seen:
                duplicates.append((seen[identity], index))
            else:
                seen[identity] = index
        return duplicates

    @staticmethod
    def _evaluator_import_error(
        dotted_path: str,
    ) -> tuple[str | None, BaseException | None]:
        try:
            module_path, attribute_name = dotted_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            evaluator = getattr(module, attribute_name)
        except (ImportError, AttributeError, ValueError) as exc:
            return f"Evaluator '{dotted_path}' is not importable", exc
        if not callable(evaluator):
            return f"Evaluator '{dotted_path}' is not callable", None
        try:
            signature = inspect.signature(evaluator)
            signature.bind(object(), resources={})
        except (TypeError, ValueError) as exc:
            message = (
                f"Evaluator '{dotted_path}' must accept one data argument and "
                "the 'resources' keyword argument"
            )
            return message, exc
        return None, None
