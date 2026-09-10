"""Tests for reference resolution."""

from __future__ import annotations

import pytest

from justflow.engine.references import (
    ReferenceResolutionError,
    find_unresolved_placeholders,
    interpolate_params,
    resolve_input,
    resolve_reference,
)


class TestResolveReference:
    @pytest.mark.parametrize(
        "ref,step_outputs,params,expected",
        [
            ("fetch.data", {"fetch": {"data": "value"}}, {}, "value"),
            ("fetch.data.nested", {"fetch": {"data": {"nested": 42}}}, {}, 42),
            ("shared_output", {"shared_output": [1, 2, 3]}, {}, [1, 2, 3]),
            ("param_val", {}, {"param_val": "from_params"}, "from_params"),
            ("fetch.items.1", {"fetch": {"items": ["first", "second"]}}, {}, "second"),
        ],
    )
    def test_resolve_valid(self, ref, step_outputs, params, expected):
        assert resolve_reference(ref, step_outputs, params) == expected

    def test_resolve_missing_root(self):
        with pytest.raises(ReferenceResolutionError, match="not found"):
            resolve_reference("missing.field", {}, {})

    def test_resolve_missing_nested(self):
        with pytest.raises(ReferenceResolutionError, match="key is not present"):
            resolve_reference("step.missing", {"step": {"other": 1}}, {})

    def test_rejects_object_attributes_without_evaluating_them(self):
        class AdversarialValue:
            @property
            def secret(self):
                raise AssertionError("property access must not run")

        with pytest.raises(ReferenceResolutionError, match="unsupported type"):
            resolve_reference("step.secret", {"step": AdversarialValue()}, {})


class TestResolveInput:
    @pytest.mark.parametrize(
        ("input_spec", "expected"),
        [
            (None, None),
            ("a.x", 1),
            ({"first": "a.x", "second": "b.y"}, {"first": 1, "second": 2}),
            (["a.x", "b.y"], [1, 2]),
        ],
        ids=["none", "string-ref", "named-dict", "list"],
    )
    def test_resolves_every_input_shape(self, input_spec, expected):
        outputs = {"a": {"x": 1}, "b": {"y": 2}}
        assert resolve_input(input_spec, outputs, {}) == expected


class TestInterpolateParams:
    @pytest.mark.parametrize(
        "value,params,expected",
        [
            ("${name}", {"name": "Alice"}, "Alice"),
            ("hello ${name}", {"name": "world"}, "hello world"),
            ("${count}", {"count": 42}, 42),
            ({"key": "${val}"}, {"val": "x"}, {"key": "x"}),
            (["${a}", "${b}"], {"a": 1, "b": 2}, [1, 2]),
            ("no interpolation", {}, "no interpolation"),
            (123, {"x": "y"}, 123),
        ],
    )
    def test_interpolation(self, value, params, expected):
        assert interpolate_params(value, params) == expected

    def test_missing_param_raises(self):
        with pytest.raises(ReferenceResolutionError, match="not found"):
            interpolate_params("${missing}", {})

    @pytest.mark.parametrize("value", ["${missing", "${}", "${missing.name}"])
    def test_malformed_placeholder_raises(self, value):
        with pytest.raises(ReferenceResolutionError, match="Invalid placeholder syntax"):
            interpolate_params(value, {})

    def test_self_referencing_param_survives_as_placeholder(self):
        params = {"source_id": "${source_id}"}
        assert interpolate_params(params, params) == {"source_id": "${source_id}"}


class TestFindUnresolvedPlaceholders:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ({"source_id": "${source_id}"}, ["source_id"]),
            (
                {"nested": {"path": "audit/${source_id}/${request_id}.json"}},
                ["source_id", "request_id"],
            ),
            (["ok", "${a}"], ["a"]),
            ({"clean": "value", "n": 3}, []),
        ],
        ids=["self-reference", "nested-path", "in-list", "clean"],
    )
    def test_detects_leftover_placeholders(self, value, expected):
        assert find_unresolved_placeholders(value) == expected
