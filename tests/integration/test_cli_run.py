"""Integration test — `justflow run` executes a workflow end-to-end locally."""

from __future__ import annotations

import json

import justflow.__main__ as engine_cli
from tests.conftest import PRIME_STATS_CONFIG_DIR


def test_run_command_executes_workflow(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        [
            "justflow",
            "run",
            "prime_stats",
            "--config-dir",
            str(PRIME_STATS_CONFIG_DIR),
            "--param",
            "n=10",
            "--param",
            "seed=7",
            "--request-id",
            "cli-run-1",
        ],
    )

    engine_cli.main()

    record = json.loads(capsys.readouterr().out)
    assert record["status"] == "completed"
    assert record["request_id"] == "cli-run-1"
    summary = record["result"]
    assert summary["total"] == 10
    assert summary["primes"] + summary["non_primes"] == 10
