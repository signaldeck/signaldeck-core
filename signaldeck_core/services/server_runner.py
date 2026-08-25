from __future__ import annotations

from collections.abc import Callable
from typing import Any


ServerRunnerFunc = Callable[..., Any]


class ServerRunner:
    def __init__(self):
        self._runner: ServerRunnerFunc | None = None
        self._owner: str | None = None

    def register(
        self,
        owner: str,
        runner: ServerRunnerFunc,
    ) -> None:
        if self._runner is not None and self._owner != owner:
            raise RuntimeError(
                f"Server runner already registered by '{self._owner}'. "
                f"Plugin '{owner}' cannot register another runner."
            )

        self._owner = owner
        self._runner = runner

    @property
    def owner(self) -> str | None:
        return self._owner

    def run(self, app, **kwargs):
        if self._runner is None:
            return app.run(**kwargs)

        return self._runner(app, **kwargs)