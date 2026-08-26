from __future__ import annotations

import json
from pathlib import Path

from signaldeck_sdk import ScriptDefinition, ScriptRepository


class FileScriptRepository(ScriptRepository):
    def __init__(self, scripts_path: str | Path):
        self.scripts_path = Path(scripts_path)
        self.scripts_path.mkdir(parents=True, exist_ok=True)

    def _path_for(self, name: str) -> Path:
        if (
            not name
            or name in {".", ".."}
            or "/" in name
            or "\\" in name
            or Path(name).name != name
        ):
            raise ValueError(f"Invalid script name: {name!r}")
        return self.scripts_path / f"{name}.json"

    def _load(self, path: Path) -> ScriptDefinition:
        with path.open("r", encoding="utf-8") as f:
            script = ScriptDefinition.from_dict(json.load(f))
        if script.name != path.stem:
            raise ValueError(
                f"Script name {script.name!r} does not match file name {path.name!r}"
            )
        return script

    def list(self) -> list[ScriptDefinition]:
        return [self._load(path) for path in sorted(self.scripts_path.glob("*.json"))]

    def get(self, name: str) -> ScriptDefinition | None:
        path = self._path_for(name)
        if not path.exists():
            return None
        return self._load(path)

    def save(self, script: ScriptDefinition) -> None:
        path = self._path_for(script.name)
        with path.open("w", encoding="utf-8") as f:
            json.dump(script.to_dict(), f, ensure_ascii=False, indent=2)
            f.write("\n")
