"""Unit tests for the primes demo app."""

from __future__ import annotations

import pytest
from prime_stats.actions.check_prime import CheckPrime, is_prime
from prime_stats.actions.generate_numbers import RANDOM_MAX, RANDOM_MIN, GenerateNumbers
from prime_stats.actions.summarize_primes import SummarizePrimes


class TestIsPrime:
    @pytest.mark.parametrize(
        ("number", "expected"),
        [
            (0, False),
            (1, False),
            (2, True),
            (3, True),
            (4, False),
            (17, True),
            (25, False),
            (997, True),
            (1000, False),
        ],
    )
    def test_primality(self, number, expected):
        assert is_prime(number) is expected


class TestGenerateNumbers:
    async def test_seeded_generation_is_deterministic_and_bounded(self):
        first = await GenerateNumbers(globals={"n": 50, "seed": 42}).generate_numbers(None)
        second = await GenerateNumbers(globals={"n": 50, "seed": 42}).generate_numbers(None)

        assert first == second
        assert len(first["numbers"]) == 50
        assert all(RANDOM_MIN <= x <= RANDOM_MAX for x in first["numbers"])

    async def test_different_seeds_differ(self):
        a = await GenerateNumbers(globals={"n": 50, "seed": 1}).generate_numbers(None)
        b = await GenerateNumbers(globals={"n": 50, "seed": 2}).generate_numbers(None)
        assert a != b


class TestCheckPrime:
    async def test_reads_fanout_item_from_globals(self):
        result = await CheckPrime(globals={"number": 17}).check_prime(input=17)
        assert result == {"number": 17, "is_prime": True}


class TestSummarizePrimes:
    async def test_counts_primes_nonprimes_and_failures(self):
        checks = [
            {"number": 2, "is_prime": True},
            {"number": 9, "is_prime": False},
            {"number": 13, "is_prime": True},
            {"_error": True, "code": "ValueError", "message": "boom"},
        ]

        summary = await SummarizePrimes().summarize_primes(checks)

        assert summary == {
            "total": 4,
            "primes": 2,
            "non_primes": 1,
            "failed_checks": 1,
            "prime_numbers": [2, 13],
        }
