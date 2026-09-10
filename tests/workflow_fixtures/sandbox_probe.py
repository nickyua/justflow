"""Workflow used to verify nondeterministic operations stay sandboxed."""

from __future__ import annotations

import importlib
import os
import random
import socket
import time
from pathlib import Path

from temporalio import workflow


@workflow.defn
class SandboxProbe:
    @workflow.run
    async def run(self, operation: str) -> None:
        match operation:
            case "filesystem":
                Path.cwd()
            case "network":
                socket.socket()
            case "time":
                time.time()
            case "random":
                random.random()
            case "environment":
                os.getenv("JUSTFLOW_SANDBOX_PROBE")
            case "import":
                importlib.import_module("decimal")
            case _:
                raise ValueError(f"Unknown sandbox probe: {operation}")
