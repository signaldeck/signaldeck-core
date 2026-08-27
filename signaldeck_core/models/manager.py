# signaldeck_core/manager.py
import logging
import asyncio
from pathlib import Path

from signaldeck_sdk import ValueProvider, Cmd
from ..services.asyncio_runtime import AsyncioRuntime
from ..services.config_loader import ConfigLoader
from ..services.plugin_service import PluginService
from ..services.scheduler_service import SchedulerService
from ..services.ui_asset_service import UiAssetService, UiAssets
from ..services.action_dispatcher import ActionDispatcher
from ..services.message_bus import InMemoryMessageBus
from ..services.script_repository import FileScriptRepository
from ..services.alias_repository import FileAliasRepository

from ..domain.processor_factory import build_datastores, build_processors
from ..domain.group_factory import build_groups
from .context_impl import build_application_context

from ..commands.wait_for_value import WaitForValue
from ..commands.compare_condition import CompareConditionCommand
from ..commands.compare_value_condition import CompareValueConditionCommand


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

        self.valueProvider = ValueProvider()
        self.valueProvider.loop = self.runtime.loop

        config_dir = Path(self.config_path).resolve().parent

        scripts_path = Path(cfg.scripts_path)
        if not scripts_path.is_absolute():
            scripts_path = config_dir / scripts_path
        self.script_repository = FileScriptRepository(scripts_path)

        aliases_path = Path(cfg.aliases_path)
        if not aliases_path.is_absolute():
            aliases_path = config_dir / aliases_path
        self.alias_repository = FileAliasRepository(aliases_path)

        self.cmd = Cmd(
            self.runtime.loop,
            script_repository=self.script_repository,
            alias_repository=self.alias_repository,
        )
        self.cmd.registerCmd(WaitForValue(self.valueProvider))
        self.cmd.registerCmd(CompareConditionCommand())
        self.cmd.registerCmd(CompareValueConditionCommand(self.valueProvider))

        # Inline definitions remain supported as migration input. Persisted definitions
        # are loaded afterwards and therefore win on name collisions.
        self.cmd.registerScripts(cfg.cmd_config.get("script", []))
        self.cmd.loadScripts()
        self.cmd.registerAliase(cfg.cmd_config.get("alias", []))
        self.cmd.loadAliases()

        self.dataStore = build_datastores(self.runtime.loop, cfg.data_stores)

        self.message_bus = InMemoryMessageBus(self.logger)

        self.ctx = build_application_context(
            values=self.valueProvider,
            logger=self.logger,
            message_bus=self.message_bus,
            lang=cfg.i18n_lang,
            lang_fallback=cfg.i18n_fallback,
        )

        self.processor = build_processors(
            cfg.processors,
            ctx=self.ctx,
            value_provider=self.valueProvider,
            cmd=self.cmd,
            data_stores=self.dataStore,
            logger=self.logger,
            collect_data=self.collect_data,
        )

        self.groups = build_groups(cfg.groups)

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

        self.plugin_service.register_plugins(app, self.processor, self.ctx)

        self._start_tasks()

    def _start_tasks(self):
        coros = []
        for p in self.processor.values():
            coros.extend(p.get_asyncio_tasks(self.collect_data))

        for c in coros:
            try:
                self.logger.info(f"Scheduling task: {c.__name__}")
            except Exception:
                self.logger.info("Scheduling task (unnamed coroutine)")

        self.runtime.schedule_coroutines(coros)

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
        return self.dispatcher.send_hash(
            self.processor,
            self.hashes,
            hashVal,
            params=params,
            file=file,
        )

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
