"""Summarize the fan-out results into prime/non-prime counts."""

from __future__ import annotations

from typing import Any

from justflow.sdk.base_action import BaseAction


class SummarizePrimes(BaseAction):
    async def summarize_primes(self, input: list[Any]) -> dict[str, Any]:
        failures = [e for e in input if isinstance(e, dict) and e.get("_error")]
        checked = [e for e in input if isinstance(e, dict) and not e.get("_error")]
        primes = sorted(e["number"] for e in checked if e["is_prime"])
        return {
            "total": len(input),
            "primes": len(primes),
            "non_primes": len(checked) - len(primes),
            "failed_checks": len(failures),
            "prime_numbers": primes,
        }
