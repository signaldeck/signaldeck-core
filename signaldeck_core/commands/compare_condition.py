from __future__ import annotations

from signaldeck_sdk import ConditionCommand

from .value_comparison import compare_values


class CompareConditionCommand(ConditionCommand):
    def __init__(self):
        super().__init__(
            "compare",
            "Compares two resolved values. Usage: compare <left> <operator> <right>. "
            "Operators: =, ==, !=, >, >=, <, <=",
        )

    async def evaluate(
        self,
        left,
        operator: str,
        right,
        cmdRes=None,
        stopEvent=None,
    ) -> bool:
        return compare_values(left, operator, right)
