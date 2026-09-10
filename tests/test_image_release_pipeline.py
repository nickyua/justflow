"""Version tags must only expose the candidate digests that passed verification."""

from pathlib import Path

import pytest
import yaml

RELEASE_WORKFLOW = Path(__file__).parents[1] / ".github" / "workflows" / "release.yml"


@pytest.mark.parametrize("image_variable", ["BASE_IMAGE", "REFERENCE_IMAGE"])
def test_candidate_builds_do_not_publish_version_tags(image_variable: str) -> None:
    workflow = yaml.safe_load(RELEASE_WORKFLOW.read_text())
    commands = [step.get("run", "") for step in workflow["jobs"]["publish-images"]["steps"]]
    builds = [
        command
        for command in commands
        if "docker buildx build" in command and image_variable in command
    ]
    assert builds
    assert all(f'--tag "${image_variable}:$EXPECTED_VERSION"' not in build for build in builds)
    assert any(f'--tag "${image_variable}:$CANDIDATE_TAG"' in build for build in builds)
    verifier_index = next(
        index
        for index, command in enumerate(commands)
        if "scripts/verify_container_images.py" in command
    )
    scan_index = next(
        index
        for index, command in enumerate(commands)
        if "--scanners vuln,secret,misconfig" in command
    )
    promotion_index = next(
        index
        for index, command in enumerate(commands)
        if f'"${image_variable}:$EXPECTED_VERSION"' in command
    )
    assert verifier_index < promotion_index
    assert scan_index < promotion_index
    assert all(
        "continue-on-error" not in step for step in workflow["jobs"]["publish-images"]["steps"]
    )
