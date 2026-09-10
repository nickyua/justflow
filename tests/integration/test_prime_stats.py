"""Integration test — the prime_stats workflow end-to-end (seeded, deterministic)."""

from __future__ import annotations

import random

from prime_stats.actions.check_prime import is_prime
from prime_stats.actions.generate_numbers import RANDOM_MAX, RANDOM_MIN
from temporalio.worker import Worker

from justflow.config.loader import ConfigLoader
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from tests.conftest import PRIME_STATS_CONFIG_DIR, configure_builtin_services

TASK_QUEUE = "test-prime-stats"
N = 25
SEED = 42


async def test_prime_stats_end_to_end():
    loader = ConfigLoader(PRIME_STATS_CONFIG_DIR)
    services = configure_builtin_services(loader.load_services().services)
    workflow_config = loader.load_workflows()["prime_stats"]
    workflow_config = workflow_config.model_copy(update={"on_complete": None})

    wf_class = compile_workflow(workflow_config, services.resolved)
    activities = WorkflowActivities(services=dict(services.configured))

    async with (
        await start_local_environment() as env,
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[wf_class],
            activities=[activities.execute_step],
            workflow_runner=workflow_sandbox_runner(),
        ),
    ):
        result = await env.client.execute_workflow(
            wf_class.run,
            {"request_id": "primes-1", "globals": {"n": N, "seed": SEED}},
            id="primes-1",
            task_queue=TASK_QUEUE,
        )

    # Reproduce the expected outcome independently
    rng = random.Random(SEED)
    expected_numbers = [rng.randint(RANDOM_MIN, RANDOM_MAX) for _ in range(N)]
    expected_primes = sorted(x for x in expected_numbers if is_prime(x))

    summary = result["result"]
    assert summary["total"] == N
    assert summary["failed_checks"] == 0
    assert summary["prime_numbers"] == expected_primes
    assert summary["primes"] + summary["non_primes"] == N
    assert result["steps"]["fan_out"]["items_total"] == N
