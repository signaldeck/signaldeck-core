from __future__ import annotations

import argparse, os
import importlib
import json
from pathlib import Path
from typing import Any


from signaldeck_core.app_factory import create_app
from signaldeck_core.services import server_runner
from .render_config import render_processor_config

def _load_raw_config(path: str) -> dict[str, Any]:
    """
    Minimal loader (json only). Replace with YAML support later if needed.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    if p.suffix.lower() != ".json":
        raise ValueError("Only JSON configs are supported right now (expected .json)")

    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _extract_processor_classpaths(cfg: dict[str, Any]) -> list[str]:
    """
    Adjust this to your actual config schema.
    Assumes:
      cfg["processors"] = [{"class": "...", ...}, ...]
    """
    out: list[str] = []
    processors = cfg.get("processors", [])
    if isinstance(processors, list):
        for p in processors:
            if isinstance(p, dict):
                cls = p.get("class") or p.get("className")
                if isinstance(cls, str) and cls:
                    out.append(cls)
    return out


def _plugin_root_from_classpath(classpath: str) -> str:
    # "signaldeck_plugin_hello_world.processors.hello_world.HelloWorldProcessor"
    return classpath.split(".", 1)[0]


def cmd_run(args: argparse.Namespace) -> int:
    app = create_app(
    config_path=args.config,
    logging_config_path=args.logging_config,
    collect_data=args.collect_data,
    extra_template_dirs=args.templates,
)
    debug = args.debug
    if not debug:
        debug = os.environ.get("SIGNALDECK_DEBUG_AUTO_RELOAD","False").lower() == "true"
    server_runner = app.extensions["signaldeck.server_runner"]

    server_runner.run(
    app,
    host=args.host,
    port=args.port,
    debug=debug,
    )
    return 0


def cmd_validate_config(args: argparse.Namespace) -> int:
    cfg = _load_raw_config(args.config)

    # 1) basic schema-ish checks
    classpaths = _extract_processor_classpaths(cfg)
    if not classpaths:
        raise SystemExit("Config contains no processors (expected cfg['processors'][].class).")

    # 2) import checks for processor classes
    errors: list[str] = []
    for cp in classpaths:
        try:
            mod_name, cls_name = cp.rsplit(".", 1)
            mod = importlib.import_module(mod_name)
            getattr(mod, cls_name)
        except Exception as e:
            errors.append(f"Processor import failed: {cp} -> {e!r}")

    # 3) plugin.py existence check (your enforced convention)
    plugin_roots = sorted({_plugin_root_from_classpath(cp) for cp in classpaths})
    for root in plugin_roots:
        try:
            importlib.import_module(f"{root}.plugin")
        except Exception as e:
            errors.append(f"Plugin entry missing/invalid: {root}.plugin -> {e!r}")

    if errors:
        print("Config validation failed:")
        for err in errors:
            print(f" - {err}")
        return 2

    print("Config validation OK.")
    print(f"Processors: {len(classpaths)}")
    print(f"Plugins: {len(plugin_roots)}")
    return 0


def cmd_list_plugins(args: argparse.Namespace) -> int:
    cfg = _load_raw_config(args.config)
    classpaths = _extract_processor_classpaths(cfg)

    plugin_roots = sorted({_plugin_root_from_classpath(cp) for cp in classpaths})
    if not plugin_roots:
        print("No plugins found (no processors in config).")
        return 0

    print("Plugins referenced by config:")
    for root in plugin_roots:
        mod_name = f"{root}.plugin"
        try:
            plugin_mod = importlib.import_module(mod_name)

            has_register = hasattr(plugin_mod, "register") and callable(getattr(plugin_mod, "register"))
            has_bp = hasattr(plugin_mod, "bp")

            # optional metadata if you add it later
            plugin_id = getattr(plugin_mod, "PLUGIN_ID", None)
            plugin_version = getattr(plugin_mod, "PLUGIN_VERSION", None)

            meta = []
            if plugin_id:
                meta.append(f"id={plugin_id}")
            if plugin_version:
                meta.append(f"version={plugin_version}")
            meta_str = f" ({', '.join(meta)})" if meta else ""

            print(f" - {root}{meta_str}  [register={has_register}, bp={has_bp}]")
        except Exception as e:
            print(f" - {root}  [ERROR importing {mod_name}: {e!r}]")

    return 0

def cmd_get_config(args) -> int:
    cfg = render_processor_config(args.processor)
    totalConfig = {
        "name": args.name or args.processor.split(".")[-1],
        "class": args.processor,
        "config": cfg
    }
    if args.add_config:
        # Load existing config file if it exists, otherwise start with empty list
        existing = []
        try:
            with open(args.add_config, "r", encoding="utf-8") as f:
                existing = json.load(f)
                if "processors" not in existing.keys():
                  existing["processors"]= []        
                existing["processors"].append(totalConfig)
        except FileNotFoundError:
            print(f"Config file {args.add_config} not found. A new one will be created.")
            existing = {"page_title":"Title","i18n":{"lang":"en","lang_fallback":"en"},"data_stores": [], "processors": [totalConfig], "groups":[],"cmd":{"alias":[],"scripts":[]}}
        with open(args.add_config, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        print(f"Appended processor config to {args.add_config}")
    print(json.dumps(totalConfig, indent=2, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="signaldeck")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- run ---
    run = sub.add_parser("run", help="Run the SignalDeck server")
    run.add_argument("--config", required=True, help="Path to instance config (json)")
    run.add_argument("--host", default="0.0.0.0")
    run.add_argument("--port", type=int, default=5000)
    run.add_argument("--debug", action="store_true")
    run.add_argument("--logging-config", default="logging_config.json")

    # collect_data default TRUE, disable via flag:
    run.add_argument(
        "--no-collect-data",
        action="store_false",
        dest="collect_data",
        help="Disable data collection (enabled by default)",
    )
    run.add_argument(
    "--templates",
    action="append",
    default=[],
    help="Additional template directory (can be used multiple times). "
         "Searched before packaged templates.",
)
    run.set_defaults(collect_data=True)

    run.set_defaults(func=cmd_run)

    # --- validate-config ---
    val = sub.add_parser("validate-config", help="Validate config and import processors/plugins")
    val.add_argument("--config", required=True, help="Path to instance config (json)")
    val.set_defaults(func=cmd_validate_config)

    # --- list-plugins ---
    lp = sub.add_parser("list-plugins", help="List plugins referenced by config")
    lp.add_argument("--config", required=True, help="Path to instance config (json)")
    lp.set_defaults(func=cmd_list_plugins)

    get_conf = sub.add_parser("get-config", help="Print default config for a processor class")
    get_conf.add_argument("--name", required=False, help="Name of processor instance")
    get_conf.add_argument("--processor", required=True, help="Fully qualified class path")
    get_conf.add_argument("--add-config", default=None, help="Append processor config to existing config file (path)")
    get_conf.set_defaults(func=cmd_get_config)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    rc = args.func(args)
    raise SystemExit(rc)
