"""Temporal sandbox configuration for dynamically compiled workflows."""

from __future__ import annotations

import dataclasses

from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxMatcher,
    SandboxRestrictions,
)

PASSTHROUGH_MODULES = (
    "justflow.engine.compiler",
    "justflow.runtime.schedule_dispatch",
    "justflow.runtime.scheduled_start_cleanup",
    "justflow.runtime.scheduled_start_dispatch",
    # Pydantic's compiled schema engine is deterministic and imported lazily during validation.
    "pydantic_core",
)

RUNTIME_IMPORT_RESTRICTIONS = SandboxMatcher(
    children={
        "__builtins__": SandboxMatcher(
            use={"__import__"},
            only_runtime=True,
        ),
        "importlib": SandboxMatcher(
            use={"import_module"},
            only_runtime=True,
        ),
    }
)

JUSTFLOW_SANDBOX_RESTRICTIONS = dataclasses.replace(
    SandboxRestrictions.default,
    invalid_module_members=(
        SandboxRestrictions.default.invalid_module_members | RUNTIME_IMPORT_RESTRICTIONS
    ),
).with_passthrough_modules(*PASSTHROUGH_MODULES)


def workflow_sandbox_runner() -> SandboxedWorkflowRunner:
    """Create an isolated runner that can resolve the dynamic workflow registry."""
    return SandboxedWorkflowRunner(restrictions=JUSTFLOW_SANDBOX_RESTRICTIONS)
