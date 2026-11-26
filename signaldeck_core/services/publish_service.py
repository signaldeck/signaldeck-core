import asyncio
import logging
import math
from signaldeck_sdk.publisher import PublisherFactory


class PublisherRegistry:
    def __init__(self):
        self.factories: dict[str, PublisherFactory] = {}

    def register(self, name, factory):
        if name in self.factories:
            raise ValueError(f"Publisher type already registered: {name}")
        self.factories[name] = factory


class PublishService:
    def __init__(self, values, registry, logger=None):
        self.values = values
        self.registry = registry
        self.logger = logger or logging.getLogger(__name__)

    def tasks(self, collect_data):
        jobs = []
        for key, definition in self.values.publish_definitions.items():
            factory = self.registry.factories.get(definition["type"])
            if factory is None:
                raise ValueError(f"Unknown publisher type for {key}: {definition['type']}")
            interval = float(definition["interval_seconds"])
            if not math.isfinite(interval) or interval <= 0:
                raise ValueError(f"Invalid publish interval for {key}")
            mapping = definition["values"]
            if not mapping or any(value not in self.values.values for value in mapping.values()):
                raise ValueError(f"Invalid publish value mapping for {key}")
            jobs.append((key, factory, definition, interval))
        if not collect_data:
            return []
        return [self._run(*job) for job in jobs]

    async def _run(self, key, factory, definition, interval):
        publisher = None
        try:
            while True:
                try:
                    if publisher is None:
                        publisher = factory(definition.get("config", {}))
                    data = self.values.getPublishValues(definition["values"])
                    await publisher.publish(data)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger.exception("Publish failed for %s", key)
                await asyncio.sleep(interval)
        finally:
            if publisher is not None:
                try:
                    await publisher.shutdown()
                except Exception:
                    self.logger.exception("Publisher shutdown failed for %s", key)
