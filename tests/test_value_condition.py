import asyncio
import unittest

from signaldeck_core.commands.compare_condition import CompareConditionCommand
from signaldeck_core.commands.compare_value_condition import CompareValueConditionCommand
from signaldeck_core.commands.get_value_command import GetValueCommand
from signaldeck_core.commands.value_comparison import compare_values


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

    def test_compare_condition_compares_resolved_values(self):
        async def run_test():
            condition = CompareConditionCommand()

            self.assertEqual(condition.name, "compare")
            self.assertTrue(await condition.evaluate("10", "<", "20"))
            self.assertTrue(await condition.evaluate("charging", "==", "charging"))
            self.assertFalse(await condition.evaluate("10", ">=", "20"))

        asyncio.run(run_test())

    def test_compare_value_condition_reads_current_value_provider_value(self):
        async def run_test():
            values = ValueProviderStub({"battery_soc": 81})
            condition = CompareValueConditionCommand(values)

            self.assertEqual(condition.name, "compare_value")
            self.assertTrue(
                await condition.evaluate("battery_soc", ">=", "80")
            )
            values.values["battery_soc"] = 79
            self.assertFalse(
                await condition.evaluate("battery_soc", ">=", "80")
            )

        asyncio.run(run_test())

    def test_get_value_command_returns_current_value_provider_value(self):
        async def run_test():
            values = ValueProviderStub({"battery_soc": 81})
            command = GetValueCommand(values)

            self.assertEqual(command.name, "get_value")
            self.assertEqual(
                await command.get_value("battery_soc"),
                81,
            )
            values.values["battery_soc"] = 79
            self.assertEqual(
                await command.get_value("battery_soc"),
                79,
            )

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
