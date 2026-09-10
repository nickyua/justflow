"""Generate a list of random numbers (seeded for reproducible runs)."""

from __future__ import annotations

import random
from typing import Any

from justflow.sdk.base_action import BaseAction

RANDOM_MIN = 0
RANDOM_MAX = 1000


class GenerateNumbers(BaseAction):
    async def generate_numbers(self, input: Any) -> dict[str, Any]:
        n = int(self.globals["n"])
        rng = random.Random(self.globals["seed"])
        numbers = [rng.randint(RANDOM_MIN, RANDOM_MAX) for _ in range(n)]
        self.log.info("Generated %d numbers (seed=%s)", n, self.globals["seed"])
        return {"numbers": numbers}
