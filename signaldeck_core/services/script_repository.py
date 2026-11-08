from __future__ import annotations

import json
import os
import tempfile
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
        # Readers in another Gunicorn worker must never observe partial JSON.
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=path.parent,
                prefix=f".{path.name}.", suffix=".tmp", delete=False,
            ) as handle:
                temp_path = Path(handle.name)
                json.dump(script.to_dict(), handle, ensure_ascii=False, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
