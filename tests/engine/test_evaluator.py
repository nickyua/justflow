"""Tests for the AST-based inline condition evaluator."""

from __future__ import annotations

import pytest

from justflow.engine.evaluator import (
    ConditionEvaluationError,
    ConditionReference,
    ExpressionSyntaxError,
    condition_reference_roots,
    condition_references,
    evaluate_condition,
    parse_condition,
)


def test_condition_references_include_nested_paths_and_optional_guards() -> None:
    references = condition_references(
        "input.items[0].id == source.record_id and exists(input.optional.value)"
    )

    assert references == (
        ConditionReference(
            root="input",
            path=("items", 0, "id"),
            optional=False,
        ),
        ConditionReference(
            root="input",
            path=("optional", "value"),
            optional=True,
        ),
        ConditionReference(
            root="source",
            path=("record_id",),
            optional=False,
        ),
    )


class TestInlineConditions:
    """The pre-G1 corpus — every previously valid condition must evaluate identically."""

    @pytest.mark.parametrize(
        "condition,data,expected",
        [
            ("input.status == 'NEW'", {"status": "NEW"}, True),
            ("input.status == 'NEW'", {"status": "OLD"}, False),
            ("input.count > 5", {"count": 10}, True),
            ("input.count > 5", {"count": 3}, False),
            ("input.count < 5", {"count": 3}, True),
            ("input.count >= 10", {"count": 10}, True),
            ("input.count <= 10", {"count": 10}, True),
            ("input.flag == true", {"flag": True}, True),
            ("input.flag == true", {"flag": False}, False),
            ("input.flag == false", {"flag": False}, True),
            ("input.value != 'bad'", {"value": "good"}, True),
            ("input.value != 'bad'", {"value": "bad"}, False),
            ("input.nested.field == 'x'", {"nested": {"field": "x"}}, True),
            ("input.confidence < 0.7", {"confidence": 0.5}, True),
            ("input.confidence < 0.7", {"confidence": 0.9}, False),
        ],
    )
    def test_legacy_corpus(self, condition, data, expected):
        assert evaluate_condition(condition, data, {}) == expected

    def test_step_output_reference_in_condition(self):
        step_outputs = {"prev_step": {"threshold": 0.5}}
        assert evaluate_condition("prev_step.threshold < 0.7", {}, step_outputs) is True


class TestExpressionLanguage:
    @pytest.mark.parametrize(
        "condition,data,expected",
        [
            ("input.a == 1 and input.b == 2", {"a": 1, "b": 2}, True),
            ("input.a == 1 and input.b == 2", {"a": 1, "b": 3}, False),
            ("input.a == 9 or input.b == 2", {"a": 1, "b": 2}, True),
            ("not input.flag", {"flag": False}, True),
            (
                "input.s == 'x' or (input.n > 5 and not input.f)",
                {"s": "y", "n": 6, "f": False},
                True,
            ),
            ("input.value in ('alpha', 'beta')", {"value": "alpha"}, True),
            ("input.value in ('alpha', 'beta')", {"value": "gamma"}, False),
            ("input.value not in ['alpha']", {"value": "gamma"}, True),
            ("len(input.items) > 2", {"items": [1, 2, 3]}, True),
            ("len(input.items) == 0", {"items": []}, True),
            ("1 < input.n < 10", {"n": 5}, True),
            ("1 < input.n < 10", {"n": 11}, False),
            ("input.items[0] == 'first'", {"items": ["first", "second"]}, True),
            ("input.map['k'] == 1", {"map": {"k": 1}}, True),
            ("input.x == None", {"x": None}, True),
            ("input.x == null", {"x": None}, True),
            ("input.flag == True", {"flag": True}, True),
            ("input.n", {"n": 5}, True),
            ("input.n", {"n": 0}, False),
        ],
        ids=[
            "and-true",
            "and-false",
            "or",
            "not",
            "nested-parens",
            "in-tuple",
            "in-miss",
            "not-in-list",
            "len-gt",
            "len-empty",
            "chained-true",
            "chained-false",
            "list-index",
            "dict-key",
            "python-none",
            "yaml-null",
            "python-true",
            "bare-truthy",
            "bare-falsy",
        ],
    )
    def test_expressions(self, condition, data, expected):
        assert evaluate_condition(condition, data, {}) == expected

    @pytest.mark.parametrize(
        "condition,data,expected",
        [
            ("exists(input.contact.email)", {"contact": {"email": "a@b"}}, True),
            ("exists(input.contact.email)", {"contact": {}}, False),
            ("exists(input.contact.email)", {}, False),
            ("exists(input.items[3])", {"items": [1]}, False),
            ("exists(ghost.field)", {}, False),
            ("exists(input.flag) and input.flag == true", {"flag": True}, True),
            ("exists(input.flag) and input.flag == true", {}, False),
        ],
        ids=[
            "present",
            "missing-leaf",
            "missing-branch",
            "missing-index",
            "missing-root",
            "guarded-true",
            "guarded-short-circuit",
        ],
    )
    def test_exists(self, condition, data, expected):
        assert evaluate_condition(condition, data, {}) == expected

    def test_params_are_a_reference_root(self):
        assert (
            evaluate_condition(
                "input.source == source_id",
                {"source": "s1"},
                {},
                {"source_id": "s1"},
            )
            is True
        )


class TestSyntaxRejection:
    @pytest.mark.parametrize(
        "expression",
        [
            "input.a + 1 > 2",
            "__import__('os').system('x')",
            "max(input.a, input.b)",
            "input.items[input.i]",
            "[x for x in input.items]",
            "lambda: 1",
            "'abc'.upper() == 'ABC'",
            "exists('literal')",
            "input.a ==",
            "input.items[1:2]",
        ],
        ids=[
            "arithmetic",
            "dunder-call",
            "non-whitelisted-fn",
            "dynamic-subscript",
            "comprehension",
            "lambda",
            "method-on-literal",
            "exists-literal",
            "incomplete",
            "slice",
        ],
    )
    def test_rejected_at_parse_time(self, expression):
        with pytest.raises(ExpressionSyntaxError):
            parse_condition(expression)


class TestReferenceRoots:
    @pytest.mark.parametrize(
        ("expression", "expected"),
        [
            ("input.a == 1", {"input"}),
            ("fetch.data.x > 0 and totals.sum < 10", {"fetch", "totals"}),
            ("input.flag == true or input.other == null", {"input"}),
            ("len(check.results) > 0", {"check"}),
            ("exists(input.x) and source_id == 's1'", {"input", "source_id"}),
        ],
    )
    def test_roots(self, expression, expected):
        assert condition_reference_roots(expression) == expected


class TestConditionEvaluationErrors:
    @pytest.mark.parametrize(
        ("condition", "data"),
        [
            ("ghost.field == true", {"field": True}),
            ("input.missing == true", {"present": 1}),
            ("input.nested.missing == 1", {"nested": {"other": 1}}),
            ("input.count > 'abc'", {"count": 3}),
            ("input.missing_flag", {"present": 1}),
            ("len(input.n) > 0", {"n": 5}),
            ("input.items[9] == 1", {"items": [1]}),
        ],
        ids=[
            "unknown-root",
            "missing-field",
            "missing-nested-field",
            "incomparable-types",
            "bare-condition-missing-field",
            "len-of-int",
            "index-out-of-range",
        ],
    )
    def test_bad_references_raise_instead_of_false(self, condition, data):
        with pytest.raises(ConditionEvaluationError, match="Cannot evaluate"):
            evaluate_condition(condition, data, {})

    def test_rejects_object_attributes_without_evaluating_them(self):
        class AdversarialValue:
            @property
            def secret(self):
                raise AssertionError("property access must not run")

        with pytest.raises(ConditionEvaluationError, match="unsupported type"):
            evaluate_condition("input.secret == 'value'", AdversarialValue(), {})
