from __future__ import annotations
import ast
from typing import Any, Dict

ALLOWED_NODES = {
    ast.Expression, ast.BinOp, ast.UnaryOp, ast.Num, ast.Constant, ast.Name,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow, ast.Mod,
    ast.USub, ast.UAdd,
    ast.Call, ast.Load,
}
ALLOWED_FUNCS = {"min": min, "max": max, "abs": abs, "round": round}

class FormulaError(ValueError):
    pass

def _check_ast(node: ast.AST) -> None:
    for n in ast.walk(node):
        if type(n) not in ALLOWED_NODES:
            raise FormulaError(f"Disallowed expression node: {type(n).__name__}")
        if isinstance(n, ast.Call):
            if not isinstance(n.func, ast.Name) or n.func.id not in ALLOWED_FUNCS:
                raise FormulaError("Only min/max/abs/round are allowed")

def eval_expression(expr: str, variables: Dict[str, Any]) -> float:
    try:
        tree = ast.parse(expr, mode="eval")
        _check_ast(tree)
        code = compile(tree, "<formula>", "eval")
        safe_vars = {k: float(v) for k, v in variables.items() if v is not None and v != ""}
        return float(eval(code, {"__builtins__": {}}, {**ALLOWED_FUNCS, **safe_vars}))
    except FormulaError:
        raise
    except Exception as e:
        raise FormulaError(str(e))
