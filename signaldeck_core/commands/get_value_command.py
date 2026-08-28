from __future__ import annotations

from signaldeck_sdk import ValueCommand, ValueProvider


class GetValueCommand(ValueCommand):
    def __init__(self, value_provider: ValueProvider):
        self.value_provider = value_provider
        super().__init__(
            "get_value",
            "Returns a ValueProvider value. Usage: get_value <fieldName>",
        )

    async def get_value(
        self,
        field_name: str,
        cmdRes=None,
        stopEvent=None,
    ):
        return self.value_provider.getValue(field_name)
