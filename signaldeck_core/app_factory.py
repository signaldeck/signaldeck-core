from __future__ import annotations
import logging, os
from flask import Flask

from typing import Optional

from jinja2 import ChoiceLoader, FileSystemLoader
from signaldeck_ui.blueprint import bp as ui_bp

from .models.manager import Manager 

from signaldeck_core.logging_setup import setup_logging
from signaldeck_core.routes import register_routes, install_shutdown_handlers
from signaldeck_core.services.server_runner import ServerRunner

class RfApp(Flask):
    def __init__(self, name: str, **kwargs):
        # static_folder=None verhindert App-/static collisions; UI liefert eigene static url
        super().__init__(name, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.jinja_env.globals.update(zip=zip)


def create_app(
    *,
    config_path: str,
    logging_config_path: str | None = "logging_config.json",
    collect_data: bool = False,
    extra_template_dirs: list[str] | None = None
) -> Flask:
    # 1) logging
    setup_logging(logging_config_path)

    # 2) app
    app = RfApp(__name__, static_folder=None)
    app.extensions["signaldeck.server_runner"] = ServerRunner()
    extra_template_dirs = extra_template_dirs or []
    if extra_template_dirs:
        app.jinja_loader = ChoiceLoader([
        FileSystemLoader(extra_template_dirs),
        app.jinja_loader,  # keep existing loaders (app + blueprints)
    ])
    # 3) UI blueprint
    app.register_blueprint(ui_bp)

    # 4) manager (dein bisheriges Orchestrierungsobjekt)
    houseManager = Manager(app, config_path, collect_data=collect_data)
    app.extensions["signaldeck.manager"] = houseManager

    # 5) routes + shutdown handlers
    register_routes(app)
    install_shutdown_handlers(app)

    return app

def create_app_from_env():
    config = os.environ.get("SIGNALDECK_CONFIG", "/config/demo.json")
    templates = os.environ.get("SIGNALDECK_TEMPLATES", "/templates")
    log_config = os.environ.get("SIGNALDECK_LOG_CONFIG", "logging_config.json")

    app = create_app(config_path=config, logging_config_path=log_config, template_path=templates)
    return app


def create_app_from_env() -> Flask:
    """
    Gunicorn-friendly factory.
    Reads configuration from environment variables only.
    """
    config_path = _env_required("SIGNALDECK_CONFIG")

    logging_config_path = os.getenv("SIGNALDECK_LOGGING_CONFIG", "logging_config.json")
    if logging_config_path == "":
        # erlauben, logging config komplett zu deaktivieren
        logging_config_path = None

    collect_data = _env_bool("SIGNALDECK_COLLECT_DATA", default=True)

    extra_template_dirs = _env_list("SIGNALDECK_TEMPLATE_DIRS", default=None)

    return create_app(
        config_path=config_path,
        logging_config_path=logging_config_path,
        collect_data=collect_data,
        extra_template_dirs=extra_template_dirs
    )


def _env_required(name: str) -> str:
    val = os.getenv(name)
    if val is None or val.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    v = val.strip().lower()
    if v in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise RuntimeError(f"Invalid boolean for {name}: {val!r} (expected true/false)")


def _env_list(name: str, default: Optional[list[str]] = None) -> Optional[list[str]]:
    """
    Reads a list from env. Accepts:
      - PATH-separator separated: "/a:/b:/c" (Linux/macOS) or "C:\\a;C:\\b" (Windows)
      - comma separated: "/a,/b,/c"
    We support both; we normalize and drop empty entries.
    """
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default

    # Prefer OS pathsep; also accept commas.
    parts: list[str] = []
    for chunk in raw.split(os.pathsep):
        parts.extend(chunk.split(","))

    cleaned = [p.strip() for p in parts if p.strip()]
    return cleaned or default