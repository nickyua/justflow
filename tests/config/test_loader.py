"""Tests for config loader."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

import pytest

import justflow.config.loader as loader_module
from justflow.config.loader import ConfigLoader, ConfigLoadError

RESOURCES_YAML = """
resources:
  store:
    provider: memory_archive
    config:
      retention_policies:
        test: 60
  runtime_config:
    provider: static
    config:
      values:
        supported: [a, b]
"""

SERVICES_YAML = """
services:
  source_api:
    transport: http
    transport_config:
      base_url: "https://source-api:8080"
    connect_timeout_sec: 5
    dispatch_timeout_sec: 30
    retries: 2
  queue_worker:
    transport: queue
    transport_config:
      broker: main
      destination: worker-requests
      idempotency: durable
    dispatch_timeout_sec: 10
    response_timeout_sec: 120
    retries: 1
"""

WORKFLOW_YAML = """
workflow: sample_flow
steps:
  fetch:
    service: queue_worker
    action: fetch_data
flow:
  - name: fetch
    op: fetch
    output: data
    then: done
  - name: done
    terminal: true
"""


@pytest.fixture
def config_dir(tmp_path):
    (tmp_path / "resources.yaml").write_text(RESOURCES_YAML)
    (tmp_path / "services.yaml").write_text(SERVICES_YAML)
    workflows = tmp_path / "workflows"
    workflows.mkdir()
    (workflows / "sample_flow.yaml").write_text(WORKFLOW_YAML)
    return tmp_path


class TestConfigLoader:
    def test_load_resources(self, config_dir):
        resources = ConfigLoader(config_dir).load_resources()
        assert set(resources.resources) == {"store", "runtime_config"}
        assert resources.resources["store"].provider == "memory_archive"
        assert resources.resources["runtime_config"].config == {"values": {"supported": ["a", "b"]}}

    def test_load_services(self, config_dir):
        services = ConfigLoader(config_dir).load_services()
        assert set(services.services) == {"source_api", "queue_worker"}
        assert services.services["source_api"].transport == "http"
        assert (
            services.services["queue_worker"].transport_config["destination"] == "worker-requests"
        )

    def test_load_workflows_keyed_by_workflow_name(self, config_dir):
        workflows = ConfigLoader(config_dir).load_workflows()
        assert set(workflows) == {"sample_flow"}
        wf = workflows["sample_flow"]
        assert set(wf.steps) == {"fetch"}
        assert [s.name for s in wf.flow] == ["fetch", "done"]

    def test_load_all(self, config_dir):
        resources, services, workflows = ConfigLoader(config_dir).load_all()
        assert len(resources.resources) == 2
        assert len(services.services) == 2
        assert len(workflows) == 1

    def test_missing_workflows_dir_is_rejected_for_runnable_loading(self, config_dir):
        (config_dir / "workflows" / "sample_flow.yaml").unlink()
        (config_dir / "workflows").rmdir()
        with pytest.raises(ConfigLoadError, match="workflow directory is required"):
            ConfigLoader(config_dir).load_workflows()

    def test_inspection_allows_missing_workflows_dir(self, config_dir):
        (config_dir / "workflows" / "sample_flow.yaml").unlink()
        (config_dir / "workflows").rmdir()
        assert ConfigLoader(config_dir).inspect_workflows() == {}

    def test_empty_workflow_directory_is_rejected_for_runnable_loading(self, config_dir):
        (config_dir / "workflows" / "sample_flow.yaml").unlink()
        with pytest.raises(ConfigLoadError, match="at least one workflow file is required"):
            ConfigLoader(config_dir).load_all()

    def test_missing_config_dir(self):
        loader = ConfigLoader("/nonexistent")
        with pytest.raises(ConfigLoadError, match="configuration directory does not exist"):
            loader.load_resources()

    def test_duplicate_logical_workflow_names_are_rejected(self, config_dir):
        duplicate = config_dir / "workflows" / "duplicate.yaml"
        duplicate.write_text(WORKFLOW_YAML)

        with pytest.raises(ConfigLoadError, match="duplicate workflow name 'sample_flow'"):
            ConfigLoader(config_dir).load_workflows()

    def test_yml_workflow_file_is_rejected_with_rename_instruction(self, config_dir):
        unsupported = config_dir / "workflows" / "other.yml"
        unsupported.write_text(WORKFLOW_YAML.replace("sample_flow", "other"))

        with pytest.raises(ConfigLoadError, match=r"unsupported entries.*other\.yml.*'\.yaml'"):
            ConfigLoader(config_dir).load_workflows()

    def test_stray_config_entry_is_rejected(self, config_dir):
        (config_dir / "service.yaml").write_text("services: {}\n")

        with pytest.raises(ConfigLoadError, match=r"unsupported entries.*service\.yaml"):
            ConfigLoader(config_dir).load_all()

    def test_nested_duplicate_key_reports_source_and_key(self, config_dir):
        resources = config_dir / "resources.yaml"
        resources.write_text(
            """resources:
  store:
    provider: memory_archive
    config:
      region: first
      region: second
"""
        )

        with pytest.raises(ConfigLoadError) as exc_info:
            ConfigLoader(config_dir).load_resources()

        assert exc_info.value.source == resources
        assert "duplicate key 'region'" in exc_info.value.detail


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: dict[str, object]


@dataclass(frozen=True, kw_only=True)
class Raises:
    match: str


YamlOutcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class YamlValueCase:
    id: str
    source: str
    outcome: YamlOutcome


YAML_VALUE_CASES = [
    YamlValueCase(
        id="canonical-json",
        source="value: {enabled: true, count: 2, ratio: 1.5, missing: null}\n",
        outcome=Returns(
            value={"value": {"enabled": True, "count": 2, "ratio": 1.5, "missing": None}}
        ),
    ),
    YamlValueCase(
        id="timestamp",
        source="value: 2026-08-03\n",
        outcome=Raises(match="unsupported type date"),
    ),
    YamlValueCase(
        id="set",
        source="value: !!set {first: null}\n",
        outcome=Raises(match="unsupported type set"),
    ),
    YamlValueCase(
        id="non-finite-number",
        source="value: .inf\n",
        outcome=Raises(match="non-finite numbers are not supported"),
    ),
    YamlValueCase(
        id="non-string-key",
        source="value: {1: first}\n",
        outcome=Raises(match="mapping keys must be strings"),
    ),
]


@pytest.mark.parametrize("case", YAML_VALUE_CASES, ids=lambda case: case.id)
def test_yaml_values_are_strict_canonical_json(tmp_path, case: YamlValueCase) -> None:
    path = tmp_path / "value.yaml"
    path.write_text(case.source)

    if isinstance(case.outcome, Returns):
        assert ConfigLoader._load_yaml(path) == case.outcome.value
    else:
        with pytest.raises(ConfigLoadError, match=case.outcome.match):
            ConfigLoader._load_yaml(path)


@dataclass(frozen=True, kw_only=True)
class YamlBoundCase:
    id: str
    setting: str
    limit: int
    source: str
    match: str


YAML_BOUND_CASES = [
    YamlBoundCase(
        id="bytes",
        setting="MAX_CONFIG_FILE_BYTES",
        limit=8,
        source="value: too-long\n",
        match="exceeds 8 bytes",
    ),
    YamlBoundCase(
        id="aliases",
        setting="MAX_YAML_ALIASES",
        limit=1,
        source="base: &base [one]\nfirst: *base\nsecond: *base\n",
        match="alias count exceeds 1",
    ),
    YamlBoundCase(
        id="nesting",
        setting="MAX_YAML_DEPTH",
        limit=2,
        source="outer: {middle: {inner: value}}\n",
        match="nesting exceeds 2",
    ),
    YamlBoundCase(
        id="collection-items",
        setting="MAX_YAML_COLLECTION_ITEMS",
        limit=2,
        source="value: [one, two, three]\n",
        match="sequence contains more than 2 items",
    ),
    YamlBoundCase(
        id="expanded-nodes",
        setting="MAX_YAML_EXPANDED_NODES",
        limit=7,
        source="base: &base [one, two]\ncopies: [*base, *base]\n",
        match="expanded YAML node count exceeds 7",
    ),
]


@pytest.mark.parametrize("case", YAML_BOUND_CASES, ids=lambda case: case.id)
def test_yaml_resource_bounds(tmp_path, monkeypatch, case: YamlBoundCase) -> None:
    path = tmp_path / "bounded.yaml"
    path.write_text(case.source)
    monkeypatch.setattr(loader_module, case.setting, case.limit)

    with pytest.raises(ConfigLoadError, match=case.match):
        ConfigLoader._load_yaml(path)
