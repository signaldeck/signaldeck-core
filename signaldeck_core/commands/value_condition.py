from __future__ import annotations

from signaldeck_sdk import ConditionCommand, ValueProvider

from .value_comparison import compare_values


class ValueConditionCommand(ConditionCommand):
    def __init__(self, value_provider: ValueProvider):
        self.value_provider = value_provider
        super().__init__(
            "value",
            "Compares a ValueProvider value. Usage: value <fieldName> <operator> <value>. "
            "Operators: =, ==, !=, >, >=, <, <=",
        )

    async def evaluate(
        self,
        field_name: str,
        operator: str,
        target_value,
        cmdRes=None,
        stopEvent=None,
    ) -> bool:
        current_value = self.value_provider.getValue(field_name)
        return compare_values(current_value, operator, target_value)
