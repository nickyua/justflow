"""Exercise real continuation and checkpoint restoration on the pinned server."""

from scripts.generate_beta_replay_baselines import record_baselines


async def test_baseline_workflows_restore_checkpoint_without_repeating_steps() -> None:
    recordings = await record_baselines()
    assert {fixture.behavior_family for fixture, _ in recordings} == {
        "contract-validation-baseline",
        "archival-baseline",
        "continuation-baseline",
        "continuation-restored-baseline",
    }
