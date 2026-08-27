import json
import tempfile
import unittest
from pathlib import Path

from signaldeck_sdk import AliasDefinition
from signaldeck_core.services.alias_repository import FileAliasRepository


class FileAliasRepositoryTest(unittest.TestCase):
    def test_save_and_reload_aliases(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "aliases.json"
            repository = FileAliasRepository(path)

            repository.save(AliasDefinition(name="first", value="echo one"))
            repository.save(AliasDefinition(name="second", value="echo two"))
            repository.save(AliasDefinition(name="first", value="echo updated"))

            aliases = repository.list()

            self.assertEqual(
                [(alias.name, alias.value) for alias in aliases],
                [("first", "echo updated"), ("second", "echo two")],
            )

            raw = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(raw[0]["name"], "first")
            self.assertEqual(raw[0]["value"], "echo updated")


if __name__ == "__main__":
    unittest.main()
