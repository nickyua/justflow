"""Safe AST-based inline condition expressions.

Inline conditions (in ``condition:``, ``on_result.when``, ``until:``) are
parsed with the stdlib ast module against an explicit node whitelist — no
eval(), no arbitrary code, no arithmetic. Evaluator functions (the
``evaluator:`` form) are NOT handled here: the runner routes them to the
``evaluate_condition`` activity so they can do I/O with their declared
resources without breaking workflow determinism.

Supported syntax:
  - and / or / not, parentheses
  - == != < <= > >= in "not in" (chaining allowed)
  - dotted references (input.flags.x, step.out, source_id) with constant
    subscripts (input.items[0], input.map['key'])
  - literals: 'str', numbers, true/false/null (and True/False/None),
    list/tuple literals of constants (for `in`)
  - functions: len(x), exists(ref) — exists() is the sanctioned way to test
    optional fields (missing references otherwise fail loud)
"""

from __future__ import annotations

import ast
import operator
from dataclasses import dataclass
from typing import Any

from justflow.engine.data import (
    DataNormalizationError,
    DataPathError,
    JSONValue,
    normalize_json_object,
    normalize_json_value,
    resolve_json_path,
)

WHITELISTED_FUNCTIONS = ("len", "exists")

# YAML-style and Python-style spellings of literal words. These win over any
# step output or param that happens to share the name.
NAME_LITERALS: dict[str, Any] = {
    "true": True,
    "false": False,
    "null": None,
    "True": True,
    "False": False,
    "None": None,
}

COMPARE_OPS: dict[type[ast.cmpop], Any] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}

_REFERENCE_NODES = (ast.Name, ast.Attribute, ast.Subscript)


class ExpressionSyntaxError(Exception):
    """The expression is not valid condition syntax (a config-time error)."""

    def __init__(self, expression: str, detail: str):
        self.expression = expression
        super().__init__(f"Invalid condition: {detail}")


class ConditionEvaluationError(Exception):
    """An inline condition references data that does not exist or cannot be compared.

    Raised instead of silently evaluating to false so that typo'd field names
    fail the workflow loudly rather than skipping steps forever.
    """

    def __init__(self, condition: str, detail: str):
        self.condition = condition
        super().__init__(f"Cannot evaluate condition: {detail}")


@dataclass(frozen=True, slots=True)
class ConditionReference:
    root: str
    path: tuple[str | int, ...]
    optional: bool = False


def parse_condition(expression: str) -> ast.Expression:
    """Parse and whitelist-check an inline condition; raises ExpressionSyntaxError."""
    try:
        tree = ast.parse(expression.strip(), mode="eval")
    except SyntaxError as e:
        raise ExpressionSyntaxError(expression, e.msg or "syntax error") from e
    _check_node(tree.body, expression)
    return tree


def condition_reference_roots(expression: str) -> set[str]:
    """Root names referenced by an inline condition (for config-time validation).

    Literals (true/false/null) and whitelisted function names are excluded;
    the special root 'input' is included when referenced.
    """
    tree = parse_condition(expression)
    return {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name)
        and node.id not in NAME_LITERALS
        and node.id not in WHITELISTED_FUNCTIONS
    }


def condition_references(expression: str) -> tuple[ConditionReference, ...]:
    tree = parse_condition(expression)
    references: set[ConditionReference] = set()

    def visit(node: ast.AST, *, optional: bool = False) -> None:
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            visit(node.args[0], optional=node.func.id == "exists")
            return
        if isinstance(node, _REFERENCE_NODES):
            reference = _condition_reference(node, optional=optional)
            if reference.root not in NAME_LITERALS and reference.root not in WHITELISTED_FUNCTIONS:
                references.add(reference)
            return
        for child in ast.iter_child_nodes(node):
            visit(child, optional=optional)

    visit(tree.body)
    return tuple(
        sorted(
            references,
            key=lambda reference: (
                reference.root,
                tuple(f"{type(token).__name__}:{token}" for token in reference.path),
            ),
        )
    )


def _condition_reference(node: ast.AST, *, optional: bool) -> ConditionReference:
    path: list[str | int] = []
    current = node
    while isinstance(current, (ast.Attribute, ast.Subscript)):
        if isinstance(current, ast.Attribute):
            path.append(current.attr)
            current = current.value
        else:
            if not isinstance(current.slice, ast.Constant) or not isinstance(
                current.slice.value, (str, int)
            ):
                raise ExpressionSyntaxError("", "invalid reference subscript")
            path.append(current.slice.value)
            current = current.value
    if not isinstance(current, ast.Name):
        raise ExpressionSyntaxError("", "invalid reference root")
    return ConditionReference(root=current.id, path=tuple(reversed(path)), optional=optional)


def evaluate_condition(
    condition: str,
    data: Any,
    step_outputs: dict[str, Any],
    params: dict[str, Any] | None = None,
) -> bool:
    """Evaluate an inline condition.

    Reference roots resolve against: 'input' (the data argument), step
    outputs/aliases, and workflow params. Missing roots/fields raise
    ConditionEvaluationError except inside exists().
    """
    try:
        tree = parse_condition(condition)
    except ExpressionSyntaxError as e:
        raise ConditionEvaluationError(condition, str(e)) from e
    try:
        normalized_data = normalize_json_value(data, path="input")
        normalized_outputs = normalize_json_object(step_outputs, path="steps")
        normalized_params = normalize_json_object(params or {}, path="params")
    except DataNormalizationError as exc:
        raise ConditionEvaluationError(condition, str(exc)) from exc
    ctx = _EvalContext(
        condition=condition,
        data=normalized_data,
        step_outputs=normalized_outputs,
        params=normalized_params,
    )
    return bool(_evaluate(tree.body, ctx))


# --- parsing whitelist ---


def _check_node(node: ast.expr, expression: str) -> None:
    if isinstance(node, ast.BoolOp):
        for value in node.values:
            _check_node(value, expression)
    elif isinstance(node, ast.UnaryOp):
        if not isinstance(node.op, ast.Not):
            raise ExpressionSyntaxError(expression, "only 'not' is allowed as a unary operator")
        _check_node(node.operand, expression)
    elif isinstance(node, ast.Compare):
        for op in node.ops:
            if not isinstance(op, (*COMPARE_OPS.keys(), ast.In, ast.NotIn)):
                raise ExpressionSyntaxError(
                    expression, f"comparison operator '{type(op).__name__}' is not allowed"
                )
        _check_node(node.left, expression)
        for comparator in node.comparators:
            _check_node(comparator, expression)
    elif isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name) or node.func.id not in WHITELISTED_FUNCTIONS:
            raise ExpressionSyntaxError(
                expression, f"only {WHITELISTED_FUNCTIONS} functions are allowed"
            )
        if node.keywords or len(node.args) != 1:
            raise ExpressionSyntaxError(
                expression, f"{node.func.id}() takes exactly one positional argument"
            )
        if node.func.id == "exists" and not isinstance(node.args[0], _REFERENCE_NODES):
            raise ExpressionSyntaxError(expression, "exists() takes a reference, not a literal")
        _check_node(node.args[0], expression)
    elif isinstance(node, ast.Attribute):
        if not isinstance(node.value, _REFERENCE_NODES):
            raise ExpressionSyntaxError(expression, "attribute access is only valid on references")
        _check_node(node.value, expression)
    elif isinstance(node, ast.Subscript):
        if not isinstance(node.value, _REFERENCE_NODES):
            raise ExpressionSyntaxError(expression, "subscripts are only valid on references")
        if not isinstance(node.slice, ast.Constant) or not isinstance(node.slice.value, (str, int)):
            raise ExpressionSyntaxError(expression, "subscripts must be string or int constants")
        _check_node(node.value, expression)
    elif isinstance(node, ast.Name):
        pass
    elif isinstance(node, ast.Constant):
        if not isinstance(node.value, (str, int, float, bool, type(None))):
            raise ExpressionSyntaxError(
                expression, f"literal of type {type(node.value).__name__} is not allowed"
            )
    elif isinstance(node, (ast.List, ast.Tuple)):
        for element in node.elts:
            if not isinstance(element, ast.Constant):
                raise ExpressionSyntaxError(expression, "collections may only contain literals")
    else:
        raise ExpressionSyntaxError(expression, f"unsupported syntax: {type(node).__name__}")


# --- evaluation ---


@dataclass
class _EvalContext:
    condition: str
    data: JSONValue
    step_outputs: dict[str, JSONValue]
    params: dict[str, JSONValue]


def _evaluate(node: ast.expr, ctx: _EvalContext) -> Any:
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return all(bool(_evaluate(v, ctx)) for v in node.values)
        return any(bool(_evaluate(v, ctx)) for v in node.values)
    if isinstance(node, ast.UnaryOp):
        return not _evaluate(node.operand, ctx)
    if isinstance(node, ast.Compare):
        return _evaluate_compare(node, ctx)
    if isinstance(node, ast.Call):
        return _evaluate_call(node, ctx)
    if isinstance(node, ast.Name):
        if node.id in NAME_LITERALS:
            return NAME_LITERALS[node.id]
        return _resolve_root(node.id, ctx)
    if isinstance(node, ast.Attribute):
        return _get_path_value(_evaluate(node.value, ctx), node.attr, ctx)
    if isinstance(node, ast.Subscript):
        if not isinstance(node.slice, ast.Constant):
            raise ConditionEvaluationError(ctx.condition, "invalid subscript")
        index = node.slice.value
        if not isinstance(index, (str, int)):
            raise ConditionEvaluationError(ctx.condition, "invalid subscript")
        return _get_path_value(_evaluate(node.value, ctx), index, ctx)
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, (ast.List, ast.Tuple)):
        values = [_evaluate(e, ctx) for e in node.elts]
        return tuple(values) if isinstance(node, ast.Tuple) else values
    raise ConditionEvaluationError(  # unreachable after parse_condition
        ctx.condition, f"unsupported node {type(node).__name__}"
    )


def _evaluate_compare(node: ast.Compare, ctx: _EvalContext) -> bool:
    left = _evaluate(node.left, ctx)
    for op, comparator in zip(node.ops, node.comparators):
        right = _evaluate(comparator, ctx)
        try:
            if isinstance(op, ast.In):
                ok = left in right
            elif isinstance(op, ast.NotIn):
                ok = left not in right
            else:
                ok = COMPARE_OPS[type(op)](left, right)
        except TypeError as e:
            raise ConditionEvaluationError(
                ctx.condition,
                f"cannot compare {type(left).__name__} with {type(right).__name__} ({e})",
            ) from e
        if not ok:
            return False
        left = right
    return True


def _evaluate_call(node: ast.Call, ctx: _EvalContext) -> Any:
    if not isinstance(node.func, ast.Name):
        raise ConditionEvaluationError(ctx.condition, "invalid function call")
    if node.func.id == "exists":
        try:
            _evaluate(node.args[0], ctx)
        except ConditionEvaluationError:
            return False
        return True

    value = _evaluate(node.args[0], ctx)
    try:
        return len(value)
    except TypeError as e:
        raise ConditionEvaluationError(
            ctx.condition, f"len() of {type(value).__name__} ({e})"
        ) from e


def _resolve_root(name: str, ctx: _EvalContext) -> Any:
    if name == "input":
        return ctx.data
    if name in ctx.step_outputs:
        return ctx.step_outputs[name]
    if name in ctx.params:
        return ctx.params[name]
    raise ConditionEvaluationError(
        ctx.condition,
        f"'{name}' is not 'input', a step output, or a workflow param "
        f"(steps: {sorted(ctx.step_outputs.keys())}, params: {sorted(ctx.params.keys())})",
    )


def _get_path_value(
    obj: Any,
    token: str | int,
    ctx: _EvalContext,
) -> JSONValue:
    try:
        return resolve_json_path(obj, [token], root="condition value")
    except (DataNormalizationError, DataPathError) as exc:
        raise ConditionEvaluationError(ctx.condition, str(exc)) from exc
