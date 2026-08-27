from __future__ import annotations

import json
from pathlib import Path

from signaldeck_sdk import AliasDefinition, AliasRepository


class FileAliasRepository(AliasRepository):
    def __init__(self, aliases_path: str | Path):
        self.aliases_path = Path(aliases_path)
        self.aliases_path.parent.mkdir(parents=True, exist_ok=True)

    def list(self) -> list[AliasDefinition]:
        if not self.aliases_path.exists():
            return []

        with self.aliases_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError("Alias repository must contain a JSON list")

        return [AliasDefinition.from_dict(item) for item in data]

    def save(self, alias: AliasDefinition) -> None:
        aliases = {item.name: item for item in self.list()}
        aliases[alias.name] = alias

        with self.aliases_path.open("w", encoding="utf-8") as f:
            json.dump(
                [aliases[name].to_dict() for name in sorted(aliases)],
                f,
                ensure_ascii=False,
                indent=2,
            )
            f.write("\n")
