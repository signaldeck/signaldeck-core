from __future__ import annotations

from typing import Any


SUPPORTED_OPERATORS = {"=", "==", "!=", ">", ">=", "<", "<="}


def _as_number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compare_values(current_value: Any, operator: str, target_value: Any) -> bool:
    if operator not in SUPPORTED_OPERATORS:
        raise ValueError(
            f"Unsupported comparison operator '{operator}'. "
            f"Supported operators: {', '.join(sorted(SUPPORTED_OPERATORS))}"
        )

    if current_value is None:
        return False

    current_number = _as_number(current_value)
    target_number = _as_number(target_value)

    if operator in {">", ">=", "<", "<="}:
        if current_number is None or target_number is None:
            raise ValueError(
                f"Operator '{operator}' requires numeric values, got "
                f"{current_value!r} and {target_value!r}"
            )
        left = current_number
        right = target_number
    elif current_number is not None and target_number is not None:
        left = current_number
        right = target_number
    else:
        left = str(current_value)
        right = str(target_value)

    if operator in {"=", "=="}:
        return left == right
    if operator == "!=":
        return left != right
    if operator == ">":
        return left > right
    if operator == ">=":
        return left >= right
    if operator == "<":
        return left < right
    return left <= right
