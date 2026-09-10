"""Primality check for a single number (fan-out worker)."""

from __future__ import annotations

from typing import Any

from justflow.sdk.base_action import BaseAction


def is_prime(number: int) -> bool:
    if number < 2:
        return False
    if number % 2 == 0:
        return number == 2
    divisor = 3
    while divisor * divisor <= number:
        if number % divisor == 0:
            return False
        divisor += 2
    return True


class CheckPrime(BaseAction):
    async def check_prime(self, input: Any) -> dict[str, Any]:
        number = self.globals["number"]
        return {"number": number, "is_prime": is_prime(number)}
