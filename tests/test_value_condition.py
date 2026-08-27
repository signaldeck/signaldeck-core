import asyncio
import unittest

from signaldeck_core.commands.value_comparison import compare_values
from signaldeck_core.commands.value_condition import ValueConditionCommand


class ValueProviderStub:
    def __init__(self, values):
        self.values = values

    def getValue(self, name):
        return self.values[name]


class ValueConditionCommandTest(unittest.TestCase):
    def test_compare_values_supports_numeric_and_string_operators(self):
        self.assertTrue(compare_values("80", ">=", 80))
        self.assertTrue(compare_values("80", "==", 80.0))
        self.assertTrue(compare_values("charging", "=", "charging"))
        self.assertTrue(compare_values("charging", "!=", "idle"))
        self.assertFalse(compare_values(None, "=", "anything"))

    def test_ordering_requires_numeric_values(self):
        with self.assertRaisesRegex(ValueError, "requires numeric values"):
            compare_values("charging", ">", "idle")

    def test_value_condition_reads_current_value_provider_value(self):
        async def run_test():
            values = ValueProviderStub({"battery_soc": 81})
            condition = ValueConditionCommand(values)

            self.assertTrue(
                await condition.evaluate("battery_soc", ">=", "80")
            )
            values.values["battery_soc"] = 79
            self.assertFalse(
                await condition.evaluate("battery_soc", ">=", "80")
            )

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
