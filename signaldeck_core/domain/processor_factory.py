# signaldeck_core/domain/processor_factory.py
from __future__ import annotations
import logging
from typing import Dict

from signaldeck_sdk import ApplicationContext, ValueProvider, Cmd, PersistData, DataStore, Processor

def class_from_name(class_path: str):
    class_name = class_path.split(".")[-1]
    path = class_path[:-(len(class_name) + 1)]
    mod = __import__(path, fromlist=[class_name])
    return getattr(mod, class_name)

def build_datastores(loop, data_store_cfg: list[dict]) -> Dict[str, DataStore]:
    res: Dict[str, DataStore] = {}
    for store in data_store_cfg:
        cls = class_from_name(store["class"])
        res[store["name"]] = cls(loop, **store.get("config", {}))
    return res

def build_processors(
    processors_cfg: list[dict],
    *,
    ctx: ApplicationContext,
    value_provider: ValueProvider,
    cmd: Cmd,
    data_stores: Dict[str, DataStore],
    logger: logging.Logger,
    collect_data: bool,
) -> Dict[str, Processor]:
    res: Dict[str, Processor] = {}
    for p in processors_cfg:
        if p.get("skip", False):
            continue

        cls = class_from_name(p["class"])
        inst = cls(p["name"], p["config"], ctx,value_provider, collect_data).withClassName(p["class"])
        inst.registerCommands(cmd)

        if isinstance(inst, PersistData):
            logger.info(f"Register data stores for processor {p['name']}")
            inst.registerDataStores(data_stores)
            inst.init_current_vals()

        res[p["name"]] = inst

    for name, ds in data_stores.items():
        logger.info(f"Fields from {name}: {ds.get_fields()}")

    return res