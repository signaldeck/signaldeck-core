# signaldeck_core/manager.py
import logging
import asyncio

from signaldeck_sdk import ValueProvider, Cmd
from ..services.asyncio_runtime import AsyncioRuntime
from ..services.config_loader import ConfigLoader
from ..services.plugin_service import PluginService
from ..services.scheduler_service import SchedulerService
from ..services.ui_asset_service import UiAssetService, UiAssets
from ..services.action_dispatcher import ActionDispatcher
from ..services.message_bus import InMemoryMessageBus

from ..domain.processor_factory import build_datastores, build_processors
from ..domain.group_factory import build_groups
from .context_impl import build_application_context

from ..commands.wait_for_value import WaitForValue  # or keep WaitForValue here; best: move to cmd module


class Manager:
    def __init__(self, app, config_path: str, collect_data: bool = True):
        self.logger = logging.getLogger(__name__)
        self.config_path = config_path
        self.collect_data = collect_data

        self.config_loader = ConfigLoader()
        self.plugin_service = PluginService(self.logger)
        self.scheduler = SchedulerService(self.logger)
        self.dispatcher = ActionDispatcher(logger=self.logger)

        self.runtime = AsyncioRuntime(self.logger)
        self.ui_assets = UiAssetService()

        self._load_and_init(app)

    def _load_and_init(self, app):
        cfg = self.config_loader.load(self.config_path)

        self.pageTitle = cfg.page_title

        # ValueProvider + Cmd
        self.valueProvider = ValueProvider()
        self.valueProvider.loop = self.runtime.loop

        self.cmd = Cmd(self.runtime.loop)
        self.cmd.registerCmd(WaitForValue(self.valueProvider))
        self.cmd.registerScripts(cfg.cmd_config.get("script", []))
        self.cmd.registerAliase(cfg.cmd_config.get("alias", []))

        # DataStores
        self.dataStore = build_datastores(self.runtime.loop, cfg.data_stores)

        # Message bus
        self.message_bus = InMemoryMessageBus(self.logger)

        # Context (includes translator)
        self.ctx = build_application_context(
            values=self.valueProvider,
            logger=self.logger,
            message_bus=self.message_bus,
            lang=cfg.i18n_lang,
            lang_fallback=cfg.i18n_fallback,
        )

        # Processors
        self.processor = build_processors(
            cfg.processors,
            ctx=self.ctx,
            value_provider=self.valueProvider,
            cmd=self.cmd,
            data_stores=self.dataStore,
            logger=self.logger,
            collect_data=self.collect_data,
        )

        # Groups
        self.groups = build_groups(cfg.groups)

        # Build indices (hashes, paths)
        self.hashes = {}
        self.groupFromHash = {}
        self.path = {}

        for group in self.groups:
            self.path.setdefault(group.path, []).append(group)
            for action in group.actions:
                hash_val = action.getHash()
                action.processor = self.processor[action.type]
                self.hashes[hash_val] = action
                self.groupFromHash[hash_val] = group



        # Register plugins (blueprints + i18n packages)
        self.plugin_service.register_plugins(app, self.processor, self.ctx)

        # Start asyncio tasks
        self._start_tasks()

    def _start_tasks(self):
        coros = []
        for p in self.processor.values():
            coros.extend(p.get_asyncio_tasks(self.collect_data))

        # ensure tasks have .__name__? (some are functions)
        for c in coros:
            try:
                self.logger.info(f"Scheduling task: {c.__name__}")
            except Exception:
                self.logger.info("Scheduling task (unnamed coroutine)")

        self.runtime.schedule_coroutines(coros)

    # ---- Delegations / API ----

    def shutdown(self):
        self.logger.info(f"Shutdown {len(self.processor)} processors")
        for name, p in self.processor.items():
            self.logger.info(f"Shutdown {name}")
            p.shutdown()
        self.runtime.shutdown_loop()

    def reinit(self, app):
        self.shutdown()
        self.runtime = AsyncioRuntime(self.logger)
        self._load_and_init(app)

    def sendHash(self, hashVal, params=None, file=None):
        return self.dispatcher.send_hash(self.processor, self.hashes, hashVal, params=params, file=file)

    def getCronsForActions(self, actions):
        return self.scheduler.get_crons_for_actions(actions)

    def setCronJob(self, action_hash: str, crondef: str | None):
        return self.scheduler.set_cron_job(action_hash, crondef)

    def getGroupsForPath(self, p):
        return self.path.get(p, [])

    def getJsAndCssFilesForGroups(self, groups) -> UiAssets:
        return self.ui_assets.get_js_css_for_groups(groups)

    def getTitleForPath(self, p):
        if p == "/":
            return self.pageTitle
        return self.pageTitle + " - " + p.strip("/").replace("/", " - ")

    def getAvailablePaths(self):
        res = []
        for p in self.path.keys():
            if p == "/":
                res.append((p, "Home"))
            else:
                res.append(("/" + p, p))
        return res