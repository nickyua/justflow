"""Tests for the engine CLI (worker/validate/graph) and visualization CLI."""

from __future__ import annotations

import json
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import justflow.__main__ as engine_cli
import justflow.visualization.__main__ as viz_cli
from justflow.config.settings import OperationsSettings, Settings
from justflow.runtime.schedule_operations import TriggerRunNowStatus
from justflow.runtime.schedule_reconciler import ScheduleApplyResult
from justflow.runtime.schedules import (
    UnscopedScheduleDecision,
    plan_schedule_reconciliation,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE
from tests.conftest import PRIME_STATS_CONFIG_DIR
from tests.settings import PRODUCTION_RUNTIME

WORKFLOW_YAML = PRIME_STATS_CONFIG_DIR / "workflows" / "prime_stats.yaml"

BROKEN_WORKFLOW = """
workflow: broken
steps:
  fetch:
    service: ghost_service
    action: do
flow:
  - name: s1
    op: fetch
    then: end
  - name: end
    terminal: true
"""


@pytest.fixture
def local_cli_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JUSTFLOW_RUNTIME__PROFILE", "local")


class TestWorkerCommand:
    @pytest.mark.parametrize("with_subcommand", [True, False], ids=["explicit", "default"])
    def test_flags_override_settings(self, monkeypatch, with_subcommand):
        monkeypatch.setenv("JUSTFLOW_RUNTIME__SCOPE", PRODUCTION_RUNTIME.scope.model_dump_json())
        argv = [
            "justflow",
            *(["worker"] if with_subcommand else []),
            "--config-dir",
            "custom-configs",
            "--task-queue",
            "custom-queue",
            "--artifact-digest",
            f"sha256:{'a' * 64}",
            "--package-version",
            "1.2.3",
            "--source-revision",
            "abc1234",
        ]
        monkeypatch.setattr("sys.argv", argv)

        with patch("justflow.engine.worker.run_engine", new=AsyncMock()) as run_mock:
            engine_cli.main()

        settings = run_mock.await_args.args[0]
        assert settings.paths.config_dir == "custom-configs"
        assert settings.temporal.task_queue == "custom-queue"
        assert settings.temporal.address == "localhost:7233"
        assert settings.deployment.artifact_digest == f"sha256:{'a' * 64}"
        assert settings.deployment.package_version == "1.2.3"
        assert settings.deployment.source_revision == "abc1234"


class TestServeCommand:
    def test_enabled_admin_distribution_is_injected(self, monkeypatch):
        settings = Settings(
            runtime=PRODUCTION_RUNTIME, operations=OperationsSettings(admin_panel_enabled=True)
        )
        admin_panel = MagicMock()
        application = MagicMock()
        runtime_application = MagicMock()
        runtime_application.return_value.create_combined_app.return_value = application
        monkeypatch.setattr("sys.argv", ["justflow", "serve"])

        with (
            patch.object(engine_cli, "load_settings", return_value=settings),
            patch("justflow_admin.create_admin_panel", return_value=admin_panel) as create_panel,
            patch(
                "justflow.runtime.application.RuntimeApplication",
                runtime_application,
            ),
            patch("uvicorn.run") as run_server,
        ):
            engine_cli.main()

        create_panel.assert_called_once_with()
        composed_settings = runtime_application.call_args.args[0]
        assert composed_settings.operations.admin_panel_enabled is True
        assert runtime_application.call_args.kwargs == {"admin_panel": admin_panel}
        run_server.assert_called_once_with(
            application,
            host=settings.control.host,
            port=settings.control.port,
            log_level=settings.logging.level.lower(),
            timeout_graceful_shutdown=settings.control.shutdown_grace_seconds,
        )

    def test_disabled_admin_does_not_require_distribution(self):
        settings = Settings(
            runtime=PRODUCTION_RUNTIME, operations=OperationsSettings(admin_panel_enabled=False)
        )

        with patch.dict("sys.modules", {"justflow_admin": None}):
            assert engine_cli._configured_admin_panel(settings) is None

    @pytest.mark.parametrize(
        ("module", "message"),
        [
            pytest.param(None, r"not installed", id="missing"),
            pytest.param(ModuleType("justflow_admin"), r"incompatible", id="incompatible"),
        ],
    )
    def test_enabled_admin_fails_closed(self, module, message):
        settings = Settings(
            runtime=PRODUCTION_RUNTIME, operations=OperationsSettings(admin_panel_enabled=True)
        )

        with (
            patch.dict("sys.modules", {"justflow_admin": module}),
            pytest.raises(SystemExit, match=message),
        ):
            engine_cli._configured_admin_panel(settings)


class TestValidateCommand:
    def test_valid_configs_report_and_exit_zero(self, monkeypatch, capsys):
        monkeypatch.setattr(
            "sys.argv", ["justflow", "validate", "--config-dir", str(PRIME_STATS_CONFIG_DIR)]
        )

        engine_cli.main()

        assert "Configuration valid" in capsys.readouterr().out

    def test_invalid_configs_list_errors_and_exit_one(self, monkeypatch, tmp_path, capsys):
        (tmp_path / "resources.yaml").write_text("resources: {}\n")
        (tmp_path / "services.yaml").write_text("services: {}\n")
        (tmp_path / "triggers.yaml").write_text("triggers: {}\n")
        (tmp_path / "workflows").mkdir()
        (tmp_path / "workflows" / "broken.yaml").write_text(BROKEN_WORKFLOW)
        monkeypatch.setattr("sys.argv", ["justflow", "validate", "--config-dir", str(tmp_path)])

        with pytest.raises(SystemExit) as exc_info:
            engine_cli.main()

        assert exc_info.value.code == 1
        assert "ghost_service" in capsys.readouterr().err

    def test_invalid_configs_emit_structured_json(self, monkeypatch, tmp_path, capsys):
        (tmp_path / "resources.yaml").write_text("resources: {}\n")
        (tmp_path / "services.yaml").write_text("services: {}\n")
        (tmp_path / "triggers.yaml").write_text("triggers: {}\n")
        (tmp_path / "workflows").mkdir()
        source = tmp_path / "workflows" / "broken.yaml"
        source.write_text(BROKEN_WORKFLOW)
        monkeypatch.setattr(
            "sys.argv",
            [
                "justflow",
                "validate",
                "--config-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
        )

        with pytest.raises(SystemExit) as exc_info:
            engine_cli.main()

        report = json.loads(capsys.readouterr().out)
        assert exc_info.value.code == 1
        assert report["valid"] is False
        assert report["summary"] == {
            "resources": 0,
            "services": 0,
            "triggers": 0,
            "workflows": 1,
        }
        assert any(
            diagnostic["source_file"] == str(source)
            and diagnostic["location"][:2] == ["workflows", "broken"]
            and diagnostic["severity"] == "error"
            for diagnostic in report["diagnostics"]
        )

    def test_loader_error_json_does_not_echo_yaml_values(self, monkeypatch, tmp_path, capsys):
        sensitive_value = "fixture-value-must-not-appear"
        (tmp_path / "resources.yaml").write_text(
            f"resources:\n  store:\n    class: package.Store\n    config:\n"
            f"      region: {sensitive_value}\n      region: replacement\n"
        )
        (tmp_path / "services.yaml").write_text("services: {}\n")
        (tmp_path / "triggers.yaml").write_text("triggers: {}\n")
        (tmp_path / "workflows").mkdir()
        (tmp_path / "workflows" / "example.yaml").write_text(
            "workflow: example\nsteps: {}\nflow:\n  - name: done\n    terminal: true\n"
        )
        monkeypatch.setattr(
            "sys.argv",
            [
                "justflow",
                "validate",
                "--config-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
        )

        with pytest.raises(SystemExit):
            engine_cli.main()

        output = capsys.readouterr().out
        report = json.loads(output)
        assert report["diagnostics"][0]["category"] == "declaration"
        assert report["diagnostics"][0]["location"][0] == "yaml"
        assert sensitive_value not in output

    def test_schedule_reference_must_resolve_to_an_authored_workflow(
        self,
        monkeypatch,
        tmp_path,
        capsys,
    ) -> None:
        config_dir = tmp_path / "configs"
        _write_minimal_configs(config_dir)
        (config_dir / "triggers.yaml").write_text(
            """
triggers:
  orphan_schedule:
    kind: schedule
    workflow: missing_workflow
    spec:
      kind: interval
      every_seconds: 60
"""
        )
        monkeypatch.setattr(
            "sys.argv",
            ["justflow", "validate", "--config-dir", str(config_dir)],
        )

        with pytest.raises(SystemExit) as raised:
            engine_cli.main()

        assert raised.value.code == 1
        output = capsys.readouterr().err
        assert "triggers.yaml:triggers.orphan_schedule.workflow" in output
        assert "unknown workflow 'missing_workflow'" in output


@pytest.mark.usefixtures("local_cli_profile")
class TestTriggersCommand:
    def test_plan_outputs_only_bounded_reconciliation_metadata(
        self,
        monkeypatch,
        capsys,
    ) -> None:
        plan = plan_schedule_reconciliation({}, {})
        runtime = SimpleNamespace(plan=AsyncMock(return_value=plan))
        application = MagicMock()
        application.create_schedule_runtime = AsyncMock(return_value=runtime)
        monkeypatch.setattr("sys.argv", ["justflow", "triggers", "plan"])

        with patch(
            "justflow.runtime.application.RuntimeApplication",
            return_value=application,
        ):
            engine_cli.main()

        output = json.loads(capsys.readouterr().out)
        assert output == {
            "changes": [],
            "has_conflicts": False,
            "mutation_count": 0,
            "plan_digest": plan.plan_digest,
            "unscoped_decision": "require_explicit",
        }

    def test_plan_passes_the_explicit_unscoped_migration_decision(
        self,
        monkeypatch,
        capsys,
    ) -> None:
        plan = plan_schedule_reconciliation(
            {},
            {},
            unscoped_decision=UnscopedScheduleDecision.MIGRATE,
        )
        runtime = SimpleNamespace(plan=AsyncMock(return_value=plan))
        application = MagicMock()
        application.create_schedule_runtime = AsyncMock(return_value=runtime)
        monkeypatch.setattr(
            "sys.argv",
            ["justflow", "triggers", "plan", "--unscoped", "migrate"],
        )

        with patch(
            "justflow.runtime.application.RuntimeApplication",
            return_value=application,
        ):
            engine_cli.main()

        runtime.plan.assert_awaited_once_with(unscoped_decision=UnscopedScheduleDecision.MIGRATE)
        assert json.loads(capsys.readouterr().out)["unscoped_decision"] == "migrate"

    def test_apply_recomputes_and_requires_the_supplied_plan_identity(
        self,
        monkeypatch,
        capsys,
    ) -> None:
        plan = plan_schedule_reconciliation({}, {})
        apply_result = ScheduleApplyResult(plan_digest=plan.plan_digest, items=())
        runtime = SimpleNamespace(
            plan=AsyncMock(return_value=plan),
            apply=AsyncMock(return_value=apply_result),
        )
        application = MagicMock()
        application.create_schedule_runtime = AsyncMock(return_value=runtime)
        monkeypatch.setattr(
            "sys.argv",
            ["justflow", "triggers", "apply", "--confirm", plan.plan_digest],
        )

        with patch(
            "justflow.runtime.application.RuntimeApplication",
            return_value=application,
        ):
            engine_cli.main()

        runtime.apply.assert_awaited_once_with(plan, confirmation=plan.plan_digest)
        assert json.loads(capsys.readouterr().out)["successful"] is True

    def test_non_interactive_apply_confirms_the_freshly_computed_plan(
        self,
        monkeypatch,
        capsys,
    ) -> None:
        plan = plan_schedule_reconciliation({}, {})
        apply_result = ScheduleApplyResult(plan_digest=plan.plan_digest, items=())
        runtime = SimpleNamespace(
            plan=AsyncMock(return_value=plan),
            apply=AsyncMock(return_value=apply_result),
        )
        application = MagicMock()
        application.create_schedule_runtime = AsyncMock(return_value=runtime)
        monkeypatch.setattr(
            "sys.argv",
            ["justflow", "triggers", "apply", "--non-interactive"],
        )

        with patch(
            "justflow.runtime.application.RuntimeApplication",
            return_value=application,
        ):
            engine_cli.main()

        runtime.apply.assert_awaited_once_with(plan, confirmation=plan.plan_digest)
        assert json.loads(capsys.readouterr().out)["successful"] is True

    def test_run_now_requires_and_hashes_an_idempotency_key(
        self,
        monkeypatch,
        capsys,
    ) -> None:
        operator = SimpleNamespace(trigger_now=AsyncMock(return_value=TriggerRunNowStatus.ACCEPTED))
        application = MagicMock()
        application.create_schedule_runtime = AsyncMock(
            return_value=SimpleNamespace(operator=operator)
        )
        monkeypatch.setattr(
            "sys.argv",
            [
                "justflow",
                "triggers",
                "trigger-now",
                "daily",
                "--idempotency-key",
                "request-1",
            ],
        )

        with patch(
            "justflow.runtime.application.RuntimeApplication",
            return_value=application,
        ):
            engine_cli.main()

        kwargs = operator.trigger_now.await_args.kwargs
        assert kwargs["request_identity_digest"] != "request-1"
        assert len(kwargs["request_identity_digest"]) == 64
        assert json.loads(capsys.readouterr().out) == {
            "status": "accepted",
            "trigger_name": "daily",
        }


@pytest.mark.usefixtures("local_cli_profile")
class TestDefinitionsCommand:
    def test_validate_accepts_authored_unpublished_workflow(
        self, monkeypatch, tmp_path, capsys
    ) -> None:
        (tmp_path / "resources.yaml").write_text("resources: {}\n")
        (tmp_path / "services.yaml").write_text("services: {}\n")
        (tmp_path / "triggers.yaml").write_text("triggers: {}\n")
        (tmp_path / "workflows").mkdir()
        (tmp_path / "workflows" / "example.yaml").write_text(
            "workflow: example\nsteps: {}\nflow:\n  - name: done\n    terminal: true\n"
        )
        monkeypatch.setattr(
            "sys.argv",
            ["justflow", "validate", "--config-dir", str(tmp_path)],
        )

        engine_cli.main()

        assert "Configuration valid" in capsys.readouterr().out

    def test_publish_creates_catalog_then_validate_accepts_it(
        self, monkeypatch, tmp_path, capsys
    ) -> None:
        (tmp_path / "resources.yaml").write_text("resources: {}\n")
        (tmp_path / "services.yaml").write_text("services: {}\n")
        (tmp_path / "triggers.yaml").write_text("triggers: {}\n")
        (tmp_path / "workflows").mkdir()
        (tmp_path / "workflows" / "example.yaml").write_text(
            """workflow: example
steps: {}
flow:
  - name: done
    terminal: true
"""
        )
        monkeypatch.setattr(
            "sys.argv",
            ["justflow", "definitions", "publish", "--config-dir", str(tmp_path)],
        )

        engine_cli.main()

        published = capsys.readouterr().out
        assert "example" in published
        assert (
            tmp_path / "definitions" / "scopes" / LOCAL_RUNTIME_SCOPE.digest / "aliases.json"
        ).is_file()

        monkeypatch.setattr(
            "sys.argv",
            ["justflow", "validate", "--config-dir", str(tmp_path)],
        )
        engine_cli.main()
        assert "Configuration valid" in capsys.readouterr().out

    def test_export_and_import_dry_run_then_apply(self, monkeypatch, tmp_path, capsys) -> None:
        source = tmp_path / "source"
        destination = tmp_path / "destination"
        bundle = tmp_path / "catalog.json"
        _write_minimal_configs(source)
        monkeypatch.setattr(
            "sys.argv",
            ["justflow", "definitions", "publish", "--config-dir", str(source)],
        )
        engine_cli.main()
        capsys.readouterr()

        monkeypatch.setattr(
            "sys.argv",
            [
                "justflow",
                "definitions",
                "export",
                "--config-dir",
                str(source),
                "--output",
                str(bundle),
            ],
        )
        engine_cli.main()
        capsys.readouterr()

        monkeypatch.setattr(
            "sys.argv",
            [
                "justflow",
                "definitions",
                "import",
                "--config-dir",
                str(destination),
                "--input",
                str(bundle),
                "--dry-run",
            ],
        )
        engine_cli.main()

        assert '"aliases_to_create": [' in capsys.readouterr().out
        scoped_aliases = (
            destination / "definitions" / "scopes" / LOCAL_RUNTIME_SCOPE.digest / "aliases.json"
        )
        assert not scoped_aliases.exists()

        monkeypatch.setattr(
            "sys.argv",
            [
                "justflow",
                "definitions",
                "import",
                "--config-dir",
                str(destination),
                "--input",
                str(bundle),
            ],
        )
        engine_cli.main()

        assert scoped_aliases.is_file()

    def test_migrate_requires_s3_destination(self, monkeypatch, tmp_path) -> None:
        monkeypatch.setattr(
            "sys.argv",
            [
                "justflow",
                "definitions",
                "migrate",
                "--config-dir",
                str(tmp_path),
                "--dry-run",
            ],
        )

        with pytest.raises(SystemExit, match="catalog.backend=s3"):
            engine_cli.main()


class TestSchemaCommand:
    def test_export_writes_bundled_schemas(self, monkeypatch, tmp_path, capsys) -> None:
        destination = tmp_path / "schemas"
        monkeypatch.setattr(
            "sys.argv",
            ["justflow", "schema", "export", "--output", str(destination)],
        )

        engine_cli.main()

        output = capsys.readouterr().out
        assert "workflow.schema.json" in output
        assert (destination / "configuration.schema.json").is_file()


class TestGraphCommand:
    def test_writes_html(self, monkeypatch, tmp_path):
        output = tmp_path / "graph.html"
        monkeypatch.setattr(
            "sys.argv",
            [
                "justflow",
                "graph",
                "prime_stats",
                "--config-dir",
                str(PRIME_STATS_CONFIG_DIR),
                "-o",
                str(output),
            ],
        )

        engine_cli.main()

        assert "prime_stats" in output.read_text()

    def test_mermaid_only_prints(self, monkeypatch, capsys):
        monkeypatch.setattr(
            "sys.argv",
            [
                "justflow",
                "graph",
                "prime_stats",
                "--config-dir",
                str(PRIME_STATS_CONFIG_DIR),
                "--mermaid-only",
            ],
        )

        engine_cli.main()

        out = capsys.readouterr().out
        assert "graph TD" in out or "flowchart" in out

    def test_unknown_workflow_exits_one(self, monkeypatch, capsys):
        monkeypatch.setattr(
            "sys.argv",
            ["justflow", "graph", "nope", "--config-dir", str(PRIME_STATS_CONFIG_DIR)],
        )

        with pytest.raises(SystemExit) as exc_info:
            engine_cli.main()

        assert exc_info.value.code == 1
        assert "not found" in capsys.readouterr().err


class TestParamParsing:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("source_id=s1", ("source_id", "s1")),
            ("n=5", ("n", 5)),
            ("flag=true", ("flag", True)),
            ('items=["a","b"]', ("items", ["a", "b"])),
            ("s=plain text", ("s", "plain text")),
        ],
    )
    def test_values_parse_as_json_when_possible(self, raw, expected):
        assert engine_cli._parse_param(raw) == expected

    def test_missing_equals_exits(self):
        with pytest.raises(SystemExit, match="KEY=VALUE"):
            engine_cli._parse_param("no-equals")


class TestVisualizationCli:
    def test_renders_html_with_config_tabs(self, monkeypatch, tmp_path):
        output = tmp_path / "out.html"
        argv = [
            "viz",
            str(WORKFLOW_YAML),
            "-o",
            str(output),
            "--config-dir",
            str(PRIME_STATS_CONFIG_DIR),
        ]
        monkeypatch.setattr("sys.argv", argv)

        viz_cli.main()

        html = output.read_text()
        assert "prime_stats" in html
        assert "mermaid" in html.lower()

    def test_mermaid_only_prints_definition(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["viz", str(WORKFLOW_YAML), "--mermaid-only"])

        viz_cli.main()

        out = capsys.readouterr().out
        assert "flowchart" in out or "graph" in out

    def test_missing_yaml_exits_nonzero(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["viz", "/nonexistent/flow.yaml"])

        with pytest.raises(SystemExit) as exc_info:
            viz_cli.main()

        assert exc_info.value.code == 1
        assert "not found" in capsys.readouterr().err


def _write_minimal_configs(config_dir: Path) -> None:
    config_dir.mkdir()
    (config_dir / "resources.yaml").write_text("resources: {}\n")
    (config_dir / "services.yaml").write_text("services: {}\n")
    (config_dir / "triggers.yaml").write_text("triggers: {}\n")
    (config_dir / "workflows").mkdir()
    (config_dir / "workflows" / "example.yaml").write_text(
        "workflow: example\nsteps: {}\nflow:\n  - name: done\n    terminal: true\n"
    )
