"""CLI entry point: worker (default), validate, graph, and one-shot run."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Callable, Iterator
from contextlib import AsyncExitStack, contextmanager
from pathlib import Path
from typing import Any

from justflow.config.diagnostics import MAX_DIAGNOSTIC_JSON_BYTES, ValidationDiagnostic
from justflow.config.loader import ConfigLoader, ConfigLoadError
from justflow.config.models import ApprovedFullAuditCapture
from justflow.config.runtime_limits import RuntimeLimits
from justflow.config.settings import Settings, load_settings
from justflow.config.validator import ConfigValidator
from justflow.configuration import FileConfigurationSource
from justflow.engine.serialization import StrictJsonLayout, dumps_strict_json
from justflow.provenance import RuntimeProfile
from justflow.resources.base import ResourceFactoryContext
from justflow.runtime.admin_panel import AdminPanel
from justflow.scope import LOCAL_RUNTIME_SCOPE
from justflow.sdk.logging_context import configure_logging
from justflow.transports.security import TransportSecuritySettings

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR"]
DIAGNOSTIC_FORMATS = ("human", "json")
COMMANDS = (
    "api",
    "worker",
    "worker-server",
    "serve",
    "validate",
    "graph",
    "run",
    "definitions",
    "triggers",
    "schema",
)
LOCAL_TASK_QUEUE = "justflow-local"
LOCAL_DEPLOYMENT_NAME = "justflow-local"
LOCAL_BUILD_ID = "development"
LOCAL_PACKAGE_VERSION = "development"
LOCAL_TEMPORAL_NAMESPACE = "default"
ADMIN_DISTRIBUTION_MODULE = "justflow_admin"
ADMIN_INSTALL_MESSAGE = (
    "Administration panel assets are enabled, but the optional justflow-admin "
    "distribution is not installed. Install it with: pip install 'justflow[admin]'"
)
ADMIN_COMPATIBILITY_MESSAGE = (
    "The installed justflow-admin distribution is incompatible with this Justflow version. "
    "Install a compatible version with: pip install 'justflow[admin]'"
)
MAX_IDEMPOTENCY_KEY_LENGTH = 256
LOCAL_CLI_COMMANDS = frozenset({"validate", "graph", "run", "schema"})


def _add_runtime_arguments(parser: argparse.ArgumentParser, settings: Settings) -> None:
    parser.add_argument("--config-dir", default=settings.paths.config_dir)
    parser.add_argument("--temporal-address", default=settings.temporal.address)
    parser.add_argument("--task-queue", default=settings.temporal.task_queue)
    parser.add_argument("--deployment-name", default=settings.deployment.name)
    parser.add_argument("--build-id", default=settings.deployment.build_id)
    parser.add_argument("--artifact-digest", default=settings.deployment.artifact_digest)
    parser.add_argument("--package-version", default=settings.deployment.package_version)
    parser.add_argument("--source-revision", default=settings.deployment.source_revision)
    parser.add_argument(
        "--runtime-profile",
        default=settings.runtime.profile,
        type=RuntimeProfile,
        choices=list(RuntimeProfile),
    )
    parser.add_argument("--log-level", default=settings.logging.level, choices=LOG_LEVELS)


def _add_control_arguments(parser: argparse.ArgumentParser, settings: Settings) -> None:
    parser.add_argument("--host", default=settings.control.host)
    parser.add_argument("--port", default=settings.control.port, type=int)
    parser.add_argument(
        "--shutdown-grace-seconds",
        default=settings.control.shutdown_grace_seconds,
        type=int,
    )


def main() -> None:
    argv = sys.argv[1:]
    if not argv or argv[0] not in COMMANDS and argv[0] not in ("-h", "--help"):
        argv = ["worker", *argv]
    bootstrap = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    bootstrap.add_argument("--runtime-profile", type=RuntimeProfile, choices=list(RuntimeProfile))
    initial, _ = bootstrap.parse_known_args(argv)
    profile = initial.runtime_profile
    if profile is None and (
        argv[0] in LOCAL_CLI_COMMANDS
        or argv[:2] == ["api", "schema"]
        or "--help" in argv
        or "-h" in argv
    ):
        profile = RuntimeProfile.LOCAL
    settings = load_settings(runtime_profile=profile)

    parser = argparse.ArgumentParser(
        prog="justflow", description="Justflow - Temporal-based workflow orchestration"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    worker = subparsers.add_parser("worker", help="Run the Temporal worker (default)")
    _add_runtime_arguments(worker, settings)

    worker_server = subparsers.add_parser(
        "worker-server",
        help="Run the Temporal worker with liveness and readiness endpoints",
    )
    _add_runtime_arguments(worker_server, settings)
    _add_control_arguments(worker_server, settings)

    api = subparsers.add_parser(
        "api",
        help="Run ingress and the control/operations/configuration API without a worker",
    )
    _add_runtime_arguments(api, settings)
    _add_control_arguments(api, settings)
    api_commands = api.add_subparsers(dest="api_command")
    api_schema = api_commands.add_parser("schema", help="Manage the public OpenAPI contract")
    api_schema_commands = api_schema.add_subparsers(dest="api_schema_command", required=True)
    api_schema_export = api_schema_commands.add_parser(
        "export",
        help="Copy the bundled OpenAPI document into a repository",
    )
    api_schema_export.add_argument("--output", default=".justflow/api")

    serve = subparsers.add_parser(
        "serve",
        help="Run the worker, ingress, and optional control API",
    )
    _add_runtime_arguments(serve, settings)
    _add_control_arguments(serve, settings)

    validate = subparsers.add_parser("validate", help="Cross-validate all config layers")
    validate.add_argument("--config-dir", default=settings.paths.config_dir)
    validate.add_argument("--format", choices=DIAGNOSTIC_FORMATS, default="human")

    graph = subparsers.add_parser("graph", help="Render a workflow as an HTML diagram")
    graph.add_argument("workflow", help="Workflow name (from configs/workflows/)")
    graph.add_argument("--config-dir", default=settings.paths.config_dir)
    graph.add_argument("-o", "--output", default=None, help="Output HTML path")
    graph.add_argument("--mermaid-only", action="store_true")
    graph.add_argument(
        "--data-plane",
        action="store_true",
        help="Overlay resource cylinders with uses/cache/audit edges",
    )

    run = subparsers.add_parser("run", help="One-shot local run on a throwaway Temporal dev server")
    run.add_argument("workflow", help="Workflow name to execute")
    run.add_argument("--config-dir", default=settings.paths.config_dir)
    run.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Trigger global (repeatable); values parse as JSON when possible",
    )
    run.add_argument("--request-id", default=None)
    run.add_argument("--log-level", default=settings.logging.level, choices=LOG_LEVELS)

    definitions = subparsers.add_parser("definitions", help="Manage immutable workflow definitions")
    definition_commands = definitions.add_subparsers(dest="definition_command", required=True)
    publish = definition_commands.add_parser(
        "publish", help="Publish authored workflows into the immutable catalog"
    )
    publish.add_argument("--config-dir", default=settings.paths.config_dir)
    export = definition_commands.add_parser(
        "export", help="Export the configured catalog as a canonical bundle"
    )
    export.add_argument("--config-dir", default=settings.paths.config_dir)
    export.add_argument("--output", required=True)
    import_command = definition_commands.add_parser(
        "import", help="Validate and import a canonical catalog bundle"
    )
    import_command.add_argument("--config-dir", default=settings.paths.config_dir)
    import_command.add_argument("--input", required=True)
    import_command.add_argument("--dry-run", action="store_true")
    migrate = definition_commands.add_parser(
        "migrate", help="Migrate the local catalog to the configured S3 catalog"
    )
    migrate.add_argument("--config-dir", default=settings.paths.config_dir)
    migrate.add_argument("--dry-run", action="store_true")
    migrate_scope = definition_commands.add_parser(
        "migrate-scope", help="Inspect or migrate retained unscoped catalog state"
    )
    migrate_scope.add_argument("--config-dir", default=settings.paths.config_dir)
    migrate_scope.add_argument(
        "--decision",
        choices=("migrate", "keep_scoped"),
        default=None,
    )
    migrate_scope.add_argument("--dry-run", action="store_true")

    triggers = subparsers.add_parser("triggers", help="Manage declared triggers")
    triggers.add_argument("--config-dir", default=settings.paths.config_dir)
    triggers.add_argument("--temporal-address", default=settings.temporal.address)
    triggers.add_argument("--task-queue", default=settings.temporal.task_queue)
    triggers.add_argument("--deployment-name", default=settings.deployment.name)
    triggers.add_argument("--build-id", default=settings.deployment.build_id)
    triggers.add_argument("--artifact-digest", default=settings.deployment.artifact_digest)
    triggers.add_argument("--package-version", default=settings.deployment.package_version)
    triggers.add_argument("--source-revision", default=settings.deployment.source_revision)
    triggers.add_argument(
        "--runtime-profile",
        default=settings.runtime.profile,
        type=RuntimeProfile,
        choices=list(RuntimeProfile),
    )
    triggers.add_argument("--log-level", default=settings.logging.level, choices=LOG_LEVELS)
    trigger_commands = triggers.add_subparsers(dest="trigger_command", required=True)
    trigger_plan = trigger_commands.add_parser(
        "plan", help="Show the desired/actual schedule-trigger reconciliation plan"
    )
    trigger_plan.add_argument(
        "--unscoped",
        choices=("migrate", "retain"),
        default=None,
        help="Explicit decision for retained unscoped schedules",
    )
    trigger_apply = trigger_commands.add_parser(
        "apply", help="Apply a freshly computed and explicitly confirmed schedule-trigger plan"
    )
    trigger_apply.add_argument(
        "--unscoped",
        choices=("migrate", "retain"),
        default=None,
        help="Explicit decision for retained unscoped schedules",
    )
    trigger_apply_confirmation = trigger_apply.add_mutually_exclusive_group(required=True)
    trigger_apply_confirmation.add_argument("--confirm", metavar="PLAN_DIGEST")
    trigger_apply_confirmation.add_argument(
        "--non-interactive",
        action="store_true",
        help="Apply the freshly computed plan in deployment automation",
    )
    trigger_commands.add_parser("list", help="List managed schedule triggers")
    trigger_describe = trigger_commands.add_parser(
        "describe", help="Describe one managed schedule trigger"
    )
    trigger_describe.add_argument("trigger")
    for operation in ("pause", "resume", "trigger-now"):
        command = trigger_commands.add_parser(
            operation,
            help=f"{operation.replace('_', ' ').title()} a managed schedule trigger",
        )
        command.add_argument("trigger")
        if operation == "trigger-now":
            command.add_argument(
                "--idempotency-key",
                required=True,
                type=_bounded_idempotency_key,
            )
    trigger_backfill = trigger_commands.add_parser(
        "backfill", help="Backfill a bounded schedule-trigger interval"
    )
    trigger_backfill.add_argument("trigger")
    trigger_backfill.add_argument("--start", required=True)
    trigger_backfill.add_argument("--end", required=True)
    trigger_delete = trigger_commands.add_parser(
        "delete", help="Delete a managed schedule trigger with identity confirmation"
    )
    trigger_delete.add_argument("trigger")
    trigger_delete.add_argument("--confirm", required=True, metavar="DESIRED_DIGEST")

    schema = subparsers.add_parser("schema", help="Manage authoring JSON Schemas")
    schema_commands = schema.add_subparsers(dest="schema_command", required=True)
    schema_export = schema_commands.add_parser(
        "export",
        help="Copy the bundled authoring schemas into a repository",
    )
    schema_export.add_argument("--output", default=".justflow/schemas")

    args = parser.parse_args(argv)

    if args.command == "api":
        _cmd_api(args, settings)
    elif args.command == "worker":
        _cmd_worker(args, settings)
    elif args.command == "worker-server":
        _cmd_worker_server(args, settings)
    elif args.command == "serve":
        _cmd_serve(args, settings)
    elif args.command == "validate":
        _cmd_validate(args, settings)
    elif args.command == "graph":
        _cmd_graph(args)
    elif args.command == "run":
        _cmd_run(args, settings)
    elif args.command == "definitions":
        _cmd_definitions(args, settings)
    elif args.command == "triggers":
        _cmd_triggers(args, settings)
    elif args.command == "schema":
        _cmd_schema(args)


def _cmd_worker(args: argparse.Namespace, settings: Settings) -> None:
    from justflow.engine.worker import run_engine

    settings = _runtime_settings(args, settings, control_server=False)
    configure_logging(settings.logging.level)
    asyncio.run(run_engine(settings))


def _cmd_worker_server(args: argparse.Namespace, settings: Settings) -> None:
    from justflow.runtime.application import RuntimeApplication

    settings = _runtime_settings(args, settings, control_server=True)
    configure_logging(settings.logging.level)
    application = RuntimeApplication(settings).create_worker_app()
    _run_uvicorn(application, settings)


def _cmd_api(args: argparse.Namespace, settings: Settings) -> None:
    if args.api_command == "schema":
        _cmd_api_schema(args)
        return
    from justflow.runtime.application import RuntimeApplication

    settings = _runtime_settings(args, settings, control_server=True)
    configure_logging(settings.logging.level)
    admin_panel = _configured_admin_panel(settings)
    application = RuntimeApplication(settings, admin_panel=admin_panel).create_gateway_app()
    _run_uvicorn(application, settings)


def _cmd_api_schema(args: argparse.Namespace) -> None:
    from justflow.openapi import OpenApiExportConflictError, export_openapi_document

    if args.api_schema_command != "export":
        raise SystemExit(f"Unsupported API schema command '{args.api_schema_command}'")
    try:
        exported = export_openapi_document(args.output)
    except OpenApiExportConflictError as exc:
        raise SystemExit(str(exc)) from exc
    print(exported)


def _cmd_serve(args: argparse.Namespace, settings: Settings) -> None:
    from justflow.runtime.application import RuntimeApplication

    settings = _runtime_settings(args, settings, control_server=True)
    configure_logging(settings.logging.level)
    admin_panel = _configured_admin_panel(settings)
    application = RuntimeApplication(settings, admin_panel=admin_panel).create_combined_app()
    _run_uvicorn(application, settings)


def _runtime_settings(
    args: argparse.Namespace,
    settings: Settings,
    *,
    control_server: bool,
) -> Settings:
    settings.paths.config_dir = args.config_dir
    settings.temporal.address = args.temporal_address
    settings.temporal.task_queue = args.task_queue
    settings.deployment.name = args.deployment_name
    settings.deployment.build_id = args.build_id
    settings.deployment.artifact_digest = args.artifact_digest
    settings.deployment.package_version = args.package_version
    settings.deployment.source_revision = args.source_revision
    settings.runtime.profile = args.runtime_profile
    settings.logging.level = args.log_level
    if control_server:
        settings.control.host = args.host
        settings.control.port = args.port
        settings.control.shutdown_grace_seconds = args.shutdown_grace_seconds
    return Settings.model_validate(settings.model_dump())


def _run_uvicorn(application: Callable[..., Any], settings: Settings) -> None:
    try:
        import uvicorn
    except ImportError as exc:
        raise SystemExit(
            "The control server requires the optional 'control' dependency: "
            "install justflow[control]"
        ) from exc

    uvicorn.run(
        application,
        host=settings.control.host,
        port=settings.control.port,
        log_level=settings.logging.level.lower(),
        timeout_graceful_shutdown=settings.control.shutdown_grace_seconds,
    )


def _configured_admin_panel(settings: Settings) -> AdminPanel | None:
    if not settings.operations.admin_panel_enabled:
        return None
    try:
        from justflow_admin import create_admin_panel
    except ModuleNotFoundError as exc:
        if exc.name != ADMIN_DISTRIBUTION_MODULE:
            raise
        raise SystemExit(ADMIN_INSTALL_MESSAGE) from exc
    except ImportError as exc:
        raise SystemExit(ADMIN_COMPATIBILITY_MESSAGE) from exc
    return create_admin_panel()


def _cmd_validate(args: argparse.Namespace, settings: Settings) -> None:
    source = FileConfigurationSource(args.config_dir, scope=settings.runtime.scope)
    try:
        bundle = source.read(settings.runtime.scope).bundle
    except ConfigLoadError as exc:
        _emit_validation_report(
            output_format=args.format,
            diagnostics=[exc.as_diagnostic()],
            summary=None,
        )
        raise SystemExit(1) from exc
    validator = ConfigValidator(
        bundle.resources,
        bundle.services,
        bundle.workflows,
        limits=settings.limits.snapshot(),
        config_dir=args.config_dir,
        workflow_sources=source.workflow_sources,
        triggers=bundle.triggers,
    )
    result = validator.validate()
    summary = {
        "resources": len(bundle.resources.resources),
        "services": len(bundle.services.services),
        "triggers": len(bundle.triggers.triggers),
        "workflows": len(bundle.workflows),
    }
    _emit_validation_report(
        output_format=args.format,
        diagnostics=result.diagnostics,
        summary=summary,
    )
    if not result.is_valid:
        raise SystemExit(1)


def _emit_validation_report(
    *,
    output_format: str,
    diagnostics: list[ValidationDiagnostic],
    summary: dict[str, int] | None,
) -> None:
    valid = not any(diagnostic.severity.value == "error" for diagnostic in diagnostics)
    if output_format == "json":
        report = {
            "diagnostics": [diagnostic.as_dict() for diagnostic in diagnostics],
            "summary": summary,
            "valid": valid,
        }
        rendered = dumps_strict_json(report, layout=StrictJsonLayout.PRETTY)
        if len(rendered.encode("utf-8")) > MAX_DIAGNOSTIC_JSON_BYTES:
            raise SystemExit(f"Validation report exceeds {MAX_DIAGNOSTIC_JSON_BYTES} bytes")
        print(rendered)
        return

    if valid and summary is not None:
        print(
            f"Configuration valid: {summary['workflows']} workflows, "
            f"{summary['services']} services, {summary['resources']} resources, "
            f"{summary['triggers']} triggers"
        )
    else:
        print("Configuration validation failed:", file=sys.stderr)
    destination = sys.stdout if valid else sys.stderr
    for diagnostic in diagnostics:
        print(f"  - {diagnostic}", file=destination)


def _cmd_definitions(args: argparse.Namespace, settings: Settings) -> None:
    if args.definition_command in {"export", "import", "migrate", "migrate-scope"}:
        _cmd_catalog_transfer(args, settings)
        return
    if args.definition_command != "publish":
        raise SystemExit(f"Unsupported definitions command '{args.definition_command}'")
    from justflow.definitions.configuration import configured_catalog_store
    from justflow.definitions.manifest import build_definition_manifests
    from justflow.transports.builtins import builtin_transport_registry

    source = FileConfigurationSource(args.config_dir, scope=settings.runtime.scope)
    bundle = source.read(settings.runtime.scope).bundle
    resources = bundle.resources
    services = bundle.services
    workflows = bundle.workflows
    registry = builtin_transport_registry()
    validator = ConfigValidator(
        resources,
        services,
        workflows,
        transport_registry=registry,
        limits=settings.limits.snapshot(),
        config_dir=args.config_dir,
        workflow_sources=source.workflow_sources,
        triggers=bundle.triggers,
    )
    validator.validate().raise_if_invalid()
    resolved_resources = dict(validator.resolved_resources)
    resolved_services = dict(validator.resolved_services)
    manifests = build_definition_manifests(
        workflows,
        resolved_services,
        settings.limits.snapshot(),
        resources=resolved_resources,
    )
    configured_catalog_store(
        settings.catalog,
        settings.paths.catalog_dir or args.config_dir,
        scope=settings.runtime.scope,
    ).publish(manifests)
    for logical_name, manifest in sorted(manifests.items()):
        print(f"{logical_name} {manifest.definition_digest}")


def _cmd_schema(args: argparse.Namespace) -> None:
    from justflow.schemas import SchemaExportConflictError, export_authoring_schemas

    if args.schema_command != "export":
        raise SystemExit(f"Unsupported schema command '{args.schema_command}'")
    try:
        exported = export_authoring_schemas(args.output)
    except SchemaExportConflictError as exc:
        raise SystemExit(str(exc)) from exc
    for path in exported:
        print(path)


def _cmd_triggers(args: argparse.Namespace, settings: Settings) -> None:
    settings.paths.config_dir = args.config_dir
    settings.temporal.address = args.temporal_address
    settings.temporal.task_queue = args.task_queue
    settings.deployment.name = args.deployment_name
    settings.deployment.build_id = args.build_id
    settings.deployment.artifact_digest = args.artifact_digest
    settings.deployment.package_version = args.package_version
    settings.deployment.source_revision = args.source_revision
    settings.runtime.profile = args.runtime_profile
    settings.logging.level = args.log_level
    settings = Settings.model_validate(settings.model_dump())
    configure_logging(settings.logging.level)
    try:
        output = asyncio.run(_run_trigger_command(args, settings))
    except Exception as exc:
        from justflow.runtime.schedule_operations import ScheduleOperationError
        from justflow.runtime.schedule_reconciler import ScheduleReconciliationError

        if isinstance(exc, (ScheduleOperationError, ScheduleReconciliationError)):
            raise SystemExit(str(exc)) from exc
        raise
    if output is not None:
        print(dumps_strict_json(output, layout=StrictJsonLayout.PRETTY))


async def _run_trigger_command(args: argparse.Namespace, settings: Settings) -> object:
    from datetime import datetime

    from justflow.runtime.application import RuntimeApplication

    runtime = await RuntimeApplication(settings).create_schedule_runtime()
    command = args.trigger_command
    if command in {"plan", "apply"}:
        if args.unscoped is None:
            plan = await runtime.plan()
        else:
            from justflow.runtime.schedules import UnscopedScheduleDecision

            plan = await runtime.plan(unscoped_decision=UnscopedScheduleDecision(args.unscoped))
        if command == "plan":
            return _trigger_plan_output(plan)
        confirmation = plan.plan_digest if args.non_interactive else args.confirm
        if confirmation is None:
            raise TypeError("Schedule apply confirmation is missing")
        result = await runtime.apply(plan, confirmation=confirmation)
        return {
            "items": [
                {
                    "change": item.change.value,
                    "error_code": (item.error_code.value if item.error_code is not None else None),
                    "schedule_id": item.schedule_id,
                    "trigger_name": item.schedule_name,
                    "status": item.status.value,
                }
                for item in result.items
            ],
            "plan_digest": result.plan_digest,
            "successful": result.successful,
        }
    if command == "list":
        return [_trigger_schedule_output(item) for item in await runtime.operator.list()]
    if command == "describe":
        return _trigger_schedule_output(await runtime.operator.describe(args.trigger))
    if command == "pause":
        return _trigger_schedule_output(await runtime.operator.pause(args.trigger))
    if command == "resume":
        return _trigger_schedule_output(await runtime.operator.resume(args.trigger))
    if command == "trigger-now":
        from justflow.scope import safe_identity_digest

        request_identity = safe_identity_digest(
            "trigger-run-now",
            f"{settings.runtime.scope.digest}:{args.trigger}:{args.idempotency_key}",
        )
        status = await runtime.operator.trigger_now(
            args.trigger,
            request_identity_digest=request_identity,
        )
        return {"status": status.value, "trigger_name": args.trigger}
    if command == "backfill":
        try:
            start_at = datetime.fromisoformat(args.start)
            end_at = datetime.fromisoformat(args.end)
        except ValueError as exc:
            raise SystemExit("Backfill timestamps must be ISO 8601 values") from exc
        return _trigger_schedule_output(
            await runtime.operator.backfill(
                args.trigger,
                start_at=start_at,
                end_at=end_at,
            )
        )
    if command == "delete":
        await runtime.operator.delete(args.trigger, confirmation=args.confirm)
        return {"deleted": True, "trigger_name": args.trigger}
    raise SystemExit(f"Unsupported triggers command '{command}'")


def _trigger_plan_output(plan: object) -> dict[str, object]:
    from justflow.runtime.schedules import SchedulePlan

    if not isinstance(plan, SchedulePlan):
        raise TypeError("Schedule plan has an invalid type")
    return {
        "changes": [
            {
                "kind": change.kind.value,
                "reason": change.reason,
                "schedule_id": change.schedule_id,
                "trigger_name": change.schedule_name,
            }
            for change in plan.changes
        ],
        "has_conflicts": plan.has_conflicts,
        "mutation_count": plan.mutation_count,
        "plan_digest": plan.plan_digest,
        "unscoped_decision": plan.unscoped_decision.value,
    }


def _bounded_idempotency_key(value: str) -> str:
    if not value or len(value) > MAX_IDEMPOTENCY_KEY_LENGTH:
        raise argparse.ArgumentTypeError(
            f"Idempotency key must contain 1-{MAX_IDEMPOTENCY_KEY_LENGTH} characters"
        )
    return value


def _trigger_schedule_output(value: object) -> dict[str, object]:
    from justflow.runtime.schedule_operations import ManagedScheduleDescription

    if not isinstance(value, ManagedScheduleDescription):
        raise TypeError("Schedule-trigger description has an invalid type")
    result = value.model_dump(mode="json")
    result["trigger_name"] = result.pop("schedule_name")
    return result


def _cmd_catalog_transfer(args: argparse.Namespace, settings: Settings) -> None:
    from justflow.config.settings import S3CatalogSettings
    from justflow.definitions.catalog import CatalogStore
    from justflow.definitions.configuration import configured_catalog_store
    from justflow.definitions.migration import (
        MAX_CATALOG_BUNDLE_BYTES,
        CatalogBundle,
        UnscopedCatalogDecision,
        export_catalog,
        import_catalog,
        migrate_unscoped_catalog,
        plan_catalog_import,
        plan_unscoped_catalog_migration,
    )

    catalog_dir = settings.paths.catalog_dir or args.config_dir
    if args.definition_command == "migrate-scope":
        unscoped = CatalogStore(catalog_dir)
        scoped = configured_catalog_store(
            settings.catalog,
            catalog_dir,
            scope=settings.runtime.scope,
        )
        unscoped_plan = plan_unscoped_catalog_migration(unscoped, scoped)
        print(
            dumps_strict_json(
                unscoped_plan.model_dump(mode="json"),
                layout=StrictJsonLayout.PRETTY,
            )
        )
        if unscoped_plan.requires_decision and args.decision is None and not args.dry_run:
            raise SystemExit("Retained unscoped catalog state requires an explicit decision")
        if args.decision is not None and not args.dry_run:
            migrate_unscoped_catalog(
                unscoped,
                scoped,
                decision=UnscopedCatalogDecision(args.decision),
            )
        return

    if args.definition_command == "export":
        bundle = export_catalog(
            configured_catalog_store(
                settings.catalog,
                catalog_dir,
                scope=settings.runtime.scope,
            )
        )
        output = Path(args.output)
        try:
            with output.open("xb") as stream:
                stream.write(bundle.canonical_bytes())
        except FileExistsError:
            raise SystemExit(f"Catalog export target already exists: {output}") from None
        except OSError as exc:
            raise SystemExit(f"Cannot write catalog export '{output}': {exc}") from exc
        print(output)
        return

    if args.definition_command == "migrate":
        if not isinstance(settings.catalog, S3CatalogSettings):
            raise SystemExit("Catalog migration requires catalog.backend=s3")
        bundle = export_catalog(CatalogStore(catalog_dir))
        destination = configured_catalog_store(
            settings.catalog,
            catalog_dir,
            scope=settings.runtime.scope,
        )
    else:
        input_path = Path(args.input)
        try:
            if input_path.stat().st_size > MAX_CATALOG_BUNDLE_BYTES:
                raise SystemExit(f"Catalog bundle exceeds {MAX_CATALOG_BUNDLE_BYTES} bytes")
            bundle = CatalogBundle.from_bytes(input_path.read_bytes())
        except OSError as exc:
            raise SystemExit(f"Cannot read catalog bundle '{input_path}': {exc}") from exc
        destination = configured_catalog_store(
            settings.catalog,
            catalog_dir,
            scope=settings.runtime.scope,
        )

    plan = plan_catalog_import(destination, bundle)
    print(
        dumps_strict_json(
            plan.model_dump(mode="json"),
            layout=StrictJsonLayout.PRETTY,
        )
    )
    if plan.conflicts:
        raise SystemExit("Catalog transfer has unresolved conflicts; no changes were made")
    if not args.dry_run:
        import_catalog(destination, bundle)


def _cmd_graph(args: argparse.Namespace) -> None:
    from justflow.visualization.graph import build_graph
    from justflow.visualization.renderer import render_html, render_mermaid

    loader = ConfigLoader(args.config_dir)
    workflows = loader.inspect_workflows()
    if args.workflow not in workflows:
        print(
            f"Workflow '{args.workflow}' not found; available: {sorted(workflows)}",
            file=sys.stderr,
        )
        sys.exit(1)

    services_path = Path(args.config_dir) / "services.yaml"
    services_models = loader.load_services().services if services_path.exists() else None
    resources_models = None
    if args.data_plane:
        resources_path = Path(args.config_dir) / "resources.yaml"
        if resources_path.exists():
            resources_models = loader.load_resources().resources

    config = workflows[args.workflow]
    graph = build_graph(
        config,
        subworkflows=workflows,
        services=services_models,
        resources=resources_models,
    )
    mermaid_def = render_mermaid(graph)

    if args.mermaid_only:
        print(mermaid_def)
        return

    services_data = (
        {name: service.model_dump(mode="json") for name, service in services_models.items()}
        if services_models is not None
        else {}
    )

    html = render_html(
        mermaid_def,
        title=config.workflow,
        description=config.description,
        graph=graph,
        services=services_data,
        resources={},
    )
    output_path = Path(args.output or f"{args.workflow}.html")
    output_path.write_text(html)
    print(f"Written to {output_path}")


@contextmanager
def _stdout_to_stderr() -> Iterator[None]:
    """Route fd 1 to stderr for the duration (subprocesses included).

    The Temporal dev server prints its startup banner to stdout; without this
    it would corrupt the machine-readable JSON that `run` emits."""
    saved_stdout = os.dup(1)
    sys.stdout.flush()
    os.dup2(2, 1)
    try:
        yield
    finally:
        sys.stdout.flush()
        os.dup2(saved_stdout, 1)
        os.close(saved_stdout)


def _cmd_run(args: argparse.Namespace, settings: Settings) -> None:
    configure_logging(args.log_level)
    trigger_globals = dict(_parse_param(p) for p in args.param)
    request_id = args.request_id or f"local-{args.workflow}"
    with _stdout_to_stderr():
        record = asyncio.run(
            _run_local(
                args.config_dir,
                args.workflow,
                trigger_globals,
                request_id,
                settings.limits.snapshot(),
                settings.transport_security,
                ResourceFactoryContext(
                    postgres_dsns=settings.resource_connections.postgres_dsns,
                    redis_urls=settings.resource_connections.redis_urls,
                ),
            )
        )
    print(dumps_strict_json(record, layout=StrictJsonLayout.PRETTY))


def _parse_param(raw: str) -> tuple[str, Any]:
    if "=" not in raw:
        raise SystemExit(f"--param must be KEY=VALUE, got '{raw}'")
    key, value = raw.split("=", 1)
    try:
        return key, json.loads(value)
    except json.JSONDecodeError:
        return key, value


async def _run_local(
    config_dir: str,
    workflow_name: str,
    trigger_globals: dict[str, Any],
    request_id: str,
    limits: RuntimeLimits,
    transport_security: TransportSecuritySettings,
    resource_context: ResourceFactoryContext,
) -> dict[str, Any]:
    from temporalio.worker import Worker

    from justflow.definitions.catalog import DefinitionCatalog
    from justflow.definitions.environment import (
        build_execution_environment_snapshot,
        sanitized_runtime_configuration,
    )
    from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI, build_definition_manifests
    from justflow.definitions.routing import (
        WorkerDeployment,
        WorkerDeploymentRouter,
        retry_pinned_workflow_start,
        wait_for_worker_deployment,
        worker_deployment_config,
    )
    from justflow.definitions.runtime import prepare_definitions
    from justflow.engine.activities import WorkflowActivities
    from justflow.engine.archival import ArchivalActivity
    from justflow.engine.local_temporal import start_local_environment
    from justflow.engine.sandbox import workflow_sandbox_runner
    from justflow.provenance import (
        LOCAL_ARTIFACT_DIGEST,
        CatalogBackendIdentity,
        RuntimeProfile,
        WorkerArtifactIdentity,
        provenance_digest,
    )
    from justflow.resources.builtins import builtin_resource_registry
    from justflow.sdk.message_contract import make_workflow_id
    from justflow.sdk.resource_loader import ResourceLoader
    from justflow.transports.builtins import builtin_transport_registry

    source = FileConfigurationSource(config_dir, scope=LOCAL_RUNTIME_SCOPE)
    configuration_snapshot = source.read(LOCAL_RUNTIME_SCOPE)
    bundle = configuration_snapshot.bundle
    resources_config = bundle.resources
    services_config = bundle.services
    workflow_configs = bundle.workflows
    if any(
        workflow_config.on_complete is not None
        and isinstance(workflow_config.on_complete.capture, ApprovedFullAuditCapture)
        for workflow_config in workflow_configs.values()
    ):
        raise SystemExit(
            "Local CLI execution does not accept approved-full audit capture; "
            "run an embedded worker with payload protection"
        )
    transport_registry = builtin_transport_registry()
    resource_registry = builtin_resource_registry()
    validator = ConfigValidator(
        resources_config,
        services_config,
        workflow_configs,
        transport_registry=transport_registry,
        resource_registry=resource_registry,
        limits=limits,
        config_dir=config_dir,
        workflow_sources=source.workflow_sources,
        triggers=bundle.triggers,
    )
    validator.validate().raise_if_invalid()
    resolved_resources = dict(validator.resolved_resources)
    resolved_services = dict(validator.resolved_services)
    if workflow_name not in workflow_configs:
        raise SystemExit(
            f"Workflow '{workflow_name}' not found; available: {sorted(workflow_configs)}"
        )

    resource_loader = ResourceLoader(resource_registry, resource_context)
    await resource_loader.load(resolved_resources)
    lifecycle = AsyncExitStack()
    lifecycle.push_async_callback(resource_loader.close)
    failure: BaseException | None = None

    try:
        configured_services = transport_registry.configure_services(
            resolved_services,
            resources=resource_loader.resources,
            security=transport_security,
        )
        manifests = build_definition_manifests(
            workflow_configs,
            resolved_services,
            limits,
            resources=resolved_resources,
        )
        catalog = DefinitionCatalog.from_manifests(manifests)
        deployment = WorkerDeployment(
            artifact_identity=WorkerArtifactIdentity(
                deployment_name=LOCAL_DEPLOYMENT_NAME,
                build_id=LOCAL_BUILD_ID,
                artifact_digest=LOCAL_ARTIFACT_DIGEST,
                package_version=LOCAL_PACKAGE_VERSION,
            ),
            compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
        )
        local_catalog_identity = CatalogBackendIdentity(
            provider="in-memory",
            configuration_digest=provenance_digest({"lifetime": "process"}),
        )
        environment_snapshots = {
            name: build_execution_environment_snapshot(
                manifest=manifest,
                artifact_identity=deployment.artifact_identity,
                catalog_backend=local_catalog_identity,
                runtime_profile=RuntimeProfile.LOCAL,
                configuration=sanitized_runtime_configuration(
                    temporal_namespace=LOCAL_TEMPORAL_NAMESPACE,
                    temporal_task_queue=LOCAL_TASK_QUEUE,
                    payload_protection_mode="plaintext",
                    broker_providers={},
                    runtime_limits=manifest.deterministic_policy.runtime_limits,
                ),
                scope=LOCAL_RUNTIME_SCOPE,
                execution_configuration=configuration_snapshot.execution_identity,
            )
            for name, manifest in manifests.items()
        }
        prepared = prepare_definitions(
            workflow_configs,
            resolved_services,
            limits,
            catalog,
            WorkerDeploymentRouter.for_deployment(deployment),
            {name: snapshot.snapshot_digest for name, snapshot in environment_snapshots.items()},
            resources=resolved_resources,
            runtime_scope=LOCAL_RUNTIME_SCOPE,
            execution_configuration=configuration_snapshot.execution_identity,
        )
        target = prepared.start_targets[workflow_name]
        activities = WorkflowActivities(
            services=configured_services,
            resources=resource_loader.resources,
            limits=limits,
        )
        lifecycle.push_async_callback(activities.close)
        archival = ArchivalActivity(resources=resource_loader.resources, limits=limits)

        async with (
            await start_local_environment() as env,
            Worker(
                env.client,
                task_queue=LOCAL_TASK_QUEUE,
                workflows=list(prepared.workflow_classes.values()),
                activities=[
                    activities.execute_step,
                    activities.evaluate_condition,
                    activities.validate_contract,
                    archival.archive_workflow,
                ],
                workflow_runner=workflow_sandbox_runner(),
                deployment_config=worker_deployment_config(deployment),
            ),
        ):
            await wait_for_worker_deployment(env.client, deployment, LOCAL_TASK_QUEUE)
            handle = await retry_pinned_workflow_start(
                lambda: env.client.start_workflow(
                    target.workflow_type,
                    {
                        "request_id": request_id,
                        "globals": trigger_globals,
                        "definition_digest": target.manifest.definition_digest,
                        "worker_deployment": deployment.name,
                        "worker_build_id": deployment.build_id,
                        "worker_artifact": deployment.artifact_identity.model_dump(mode="json"),
                        "environment_snapshot_digest": target.environment_snapshot_digest,
                        "scope_digest": LOCAL_RUNTIME_SCOPE.digest,
                        "execution_configuration": (
                            target.execution_configuration.model_dump(
                                mode="json",
                                exclude_none=True,
                            )
                            if target.execution_configuration is not None
                            else None
                        ),
                    },
                    id=make_workflow_id(
                        workflow_name,
                        request_id,
                        scope=LOCAL_RUNTIME_SCOPE,
                    ),
                    task_queue=LOCAL_TASK_QUEUE,
                    memo=target.memo,
                    versioning_override=target.versioning_override,
                ),
                deployment,
                LOCAL_TASK_QUEUE,
            )
            result: dict[str, Any] = await handle.result()
        return result
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            await lifecycle.aclose()
        except Exception as cleanup_exc:
            if failure is None:
                raise
            failure.add_note(str(cleanup_exc))


if __name__ == "__main__":
    main()
