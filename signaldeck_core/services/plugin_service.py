# signaldeck_core/services/plugin_service.py
from __future__ import annotations
import importlib
import logging

class PluginService:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger(__name__)

    def register_plugins(self, app, processors: dict, ctx) -> set[str]:
        already_handled: set[str] = set()

        for _, processor in processors.items():
            plugin_name = processor.className.split(".")[0]
            if plugin_name in already_handled:
                continue

            plugin_mod_name = f"{plugin_name}.plugin"
            try:
                plugin_mod = importlib.import_module(plugin_mod_name)
                plugin_mod.register(app,ctx)
            except ModuleNotFoundError as e:
                raise RuntimeError(
                    f"Plugin '{plugin_name}' must provide '{plugin_mod_name}' (plugin.py) "
                    f"and be installed in the environment."
                ) from e

            already_handled.add(plugin_name)

        # i18n: load plugin locales after registration
        ctx.translator.load_from_packages(list(already_handled))

        # expose translation in Jinja
        app.jinja_env.globals["t"] = ctx.t

        self.logger.info(f"Initialized plugins: {', '.join(sorted(already_handled))}")
        return already_handled