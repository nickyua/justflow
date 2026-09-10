"""Integration coverage for a host-registered synchronous transport."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from temporalio.worker import Worker

from justflow.config.loader import ConfigLoader
from justflow.config.validator import ConfigValidator
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.transports.base import (
    Completed,
    StrictTransportConfig,
    TransportFactoryContext,
    TransportRequest,
)
from justflow.transports.builtins import builtin_transport_registry
from justflow.transports.registry import TransportProvider

TASK_QUEUE = "test-custom-transport"
SERVICE_TIMEOUT_SECONDS = 10


class PrefixConfig(StrictTransportConfig):
    prefix: str = Field(min_length=1)


class PrefixTransport:
    def __init__(self, config: PrefixConfig):
        self._config = config

    async def send(self, request: TransportRequest) -> Completed:
        return Completed(
            data={
                "message": f"{self._config.prefix}:{request.globals['value']}",
            }
        )

    async def close(self) -> None:
        return None


def build_prefix_transport(
    config: PrefixConfig,
    context: TransportFactoryContext,
) -> PrefixTransport:
    del context
    return PrefixTransport(config)


def write_config(config_dir: Path) -> None:
    (config_dir / "resources.yaml").write_text("resources: {}\n")
    (config_dir / "services.yaml").write_text(
        f"""services:
  echo:
    transport: test.prefix
    transport_config:
      prefix: from-yaml
    dispatch_timeout_sec: {SERVICE_TIMEOUT_SECONDS}
    retries: 0
"""
    )
    workflows_dir = config_dir / "workflows"
    workflows_dir.mkdir()
    (workflows_dir / "custom.yaml").write_text(
        """workflow: custom_transport
result: invoke.reply
params:
  value: "${value}"
steps:
  echo:
    service: echo
    action: send
    params:
      value: "${value}"
flow:
  - name: invoke
    op: echo
    output: reply
    then: done
  - name: done
    terminal: true
"""
    )


async def test_custom_transport_runs_from_yaml_without_core_changes(tmp_path: Path) -> None:
    write_config(tmp_path)
    resources, services, workflows = ConfigLoader(tmp_path).load_all()
    registry = builtin_transport_registry()
    registry.register(
        TransportProvider(
            name="test.prefix",
            contract_version="1",
            config_model=PrefixConfig,
            factory=build_prefix_transport,
        )
    )
    validator = ConfigValidator(
        resources,
        services,
        workflows,
        transport_registry=registry,
    )
    validator.validate().raise_if_invalid()
    resolved_services = dict(validator.resolved_services)
    configured_services = registry.configure_services(resolved_services, resources={})
    workflow_class = compile_workflow(
        workflows["custom_transport"],
        resolved_services,
    )
    activities = WorkflowActivities(services=configured_services)

    try:
        async with (
            await start_local_environment() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[workflow_class],
                activities=[activities.execute_step],
                workflow_runner=workflow_sandbox_runner(),
            ),
        ):
            result = await env.client.execute_workflow(
                workflow_class.run,
                {"request_id": "custom-transport-1", "globals": {"value": "hello"}},
                id="custom-transport-1",
                task_queue=TASK_QUEUE,
            )
    finally:
        await activities.close()

    assert result["result"] == {"message": "from-yaml:hello"}
